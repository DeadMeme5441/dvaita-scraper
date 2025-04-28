# src/tasks/discovery.py

import re
import asyncio
from typing import List, Tuple, Optional, Dict, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger
from slugify import slugify
from devatrans import DevaTrans
from pymongo import UpdateOne
from pydantic import ValidationError, HttpUrl
from datetime import datetime, timezone
from arq.connections import ArqRedis

from src.config import settings
from src.utils.http_client import http_client
from src.db.mongo_client import get_mongo_db

# Import BookCreate model reflecting Phase 1 status structure
from src.models.book import BookCreate

# --- Task names are no longer needed here as we don't enqueue Phase 2 ---
# from src.tasks.processing import FETCH_SUTRAS_TASK_NAME
# DISCOVER_SUTRAS_TASK_NAME = "discover_book_sutras"

# --- Constants ---
BOOKS_COLLECTION = "books"
PATH_RE = re.compile(r"/category-details/(?P<page_id>\d+)/(?P<work_id>\d+)")
SKIP_TITLES = {
    "temp",
    "अमरकोश",
    "संस्कृत् हेरिटेज्",
    "sandhi",
    "samaasa",
    "semantic search",
    "chatbox (q&a)",
}
NUM_BULLET = re.compile(r"^\d+\.\s*")

# Initialize DevaTrans globally
try:
    dt = DevaTrans()
except Exception as e:
    logger.error(f"Failed to initialize DevaTrans: {e}")
    dt = None


# --- Helper Functions (Specific to Discovery) ---
# (Helpers _parse_sections, _transliterate, _extract_sidebar_html,
#  _extract_page_urls_from_sidebar, _get_arq_redis_from_context remain unchanged)
def _parse_sections(html: str) -> List[Tuple[str, List[Tag]]]:
    sections_data = []
    try:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("div.card")
        logger.debug(f"[DiscoveryHelpers] Found {len(cards)} potential section cards.")
        for card in cards:
            header_tag = card.find("h5", class_="card-title")
            if not header_tag:
                continue
            section_sa = header_tag.get_text(strip=True).rstrip(":")
            if not section_sa:
                continue
            list_items: List[Tag] = []
            card_body = card.find("div", class_="card-body")
            if card_body:
                list_ul = card_body.find("ul", class_="card-text")
                if list_ul:
                    list_items = list_ul.find_all("li", recursive=False)
                else:
                    list_items = card_body.find_all("li", recursive=False)
            valid_list_items = [li for li in list_items if li.find("a", href=True)]
            if valid_list_items:
                sections_data.append((section_sa, valid_list_items))
    except Exception as e:
        logger.exception(f"[DiscoveryHelpers] Error parsing sections: {e}")
    if not sections_data:
        logger.warning("[DiscoveryHelpers] No sections found.")
    return sections_data


def _transliterate(text_sa: str) -> str:
    if not dt:
        return text_sa
    try:
        trans = dt.transliterate(
            input_type="sen", to_convention="iast", sentence=text_sa
        )
        return trans.strip() if trans else text_sa
    except Exception as e:
        logger.warning(f"DevaTrans failed for '{text_sa}': {e}.")
        return text_sa


def _extract_sidebar_html(html: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
        div = soup.find("div", id="sidebar-menu")
        if div:
            return div.decode_contents().strip()
        else:
            if soup.find("a", href=lambda x: x and "category-details" in x):
                logger.warning(
                    "[DiscoveryHelpers] Could not find <div id='sidebar-menu'>, but found category links. Returning full HTML as fallback."
                )
                return html.strip()
            logger.warning(
                "[DiscoveryHelpers] Could not find <div id='sidebar-menu'> and no category links found."
            )
            return None
    except Exception as e:
        logger.exception(f"[DiscoveryHelpers] Error extracting sidebar: {e}")
        return None


def _extract_page_urls_from_sidebar(
    sidebar_html: str, base_url: str, book_id: int
) -> List[str]:
    """Extracts and validates page URLs from sidebar HTML, returning a list of valid URL strings."""
    page_urls = set()
    if not sidebar_html:
        return []
    try:
        soup = BeautifulSoup(sidebar_html, "lxml")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if not href or "category-details" not in href:
                continue
            path_match = PATH_RE.search(href)
            if (
                path_match
                and path_match.group("work_id")
                and int(path_match.group("work_id")) == book_id
            ):
                full_url = urljoin(base_url, href)
                try:
                    HttpUrl(full_url)  # Validate format
                    page_urls.add(full_url)  # Add the string URL
                except ValidationError:
                    logger.warning(
                        f"[DiscoveryHelpers] Skipping invalid URL format found in sidebar: {full_url}"
                    )
    except Exception as e:
        logger.exception(
            f"[DiscoveryHelpers] Error extracting page URLs from sidebar: {e}"
        )
    return sorted(list(page_urls))


def _get_arq_redis_from_context(ctx) -> Optional[ArqRedis]:
    """Safely retrieves the ArqRedis instance from the worker context."""
    try:
        redis: Optional[ArqRedis] = ctx.get("redis")
        if not redis:
            logger.error("ARQ Redis pool not found in worker context.")
            return None
        return redis
    except Exception as e:
        logger.error(f"Error accessing ARQ Redis from context: {e}")
        return None


# --- ARQ Task Definitions ---


async def discover_initial_books(ctx, *args, **kwargs) -> str:
    """
    ARQ Task 1 (Phase 1): Fetches homepage, parses initial book links/metadata,
    and enqueues fetch_book_details task for each unique book found.
    """
    log_prefix = "[InitialDiscovery]"
    logger.info(f"{log_prefix} Starting task...")
    if not settings or not http_client:
        return f"{log_prefix} Failed: Settings/HTTP client missing."

    arq_redis = _get_arq_redis_from_context(ctx)
    if not arq_redis:
        logger.warning(
            f"{log_prefix} ARQ Redis pool not in context. Cannot enqueue next tasks."
        )

    base_url = str(settings.target_base_url)
    initial_books_found = []
    processed_links = set()
    try:
        logger.info(f"{log_prefix} Fetching homepage: {base_url}")
        homepage_html = await http_client.get(
            base_url, use_proxy=True, response_format="text"
        )
        logger.info(f"{log_prefix} Homepage fetched successfully.")
        sections = _parse_sections(homepage_html)
        if not sections:
            return f"{log_prefix} Completed: No book sections found."
        logger.info(f"{log_prefix} Found {len(sections)} potential sections.")
        for section_sa, lis in sections:
            section_iast = _transliterate(section_sa)
            section_slug = slugify(section_iast) or slugify(section_sa)
            if not section_slug:
                continue
            for order, li in enumerate(lis, start=1):
                a_tag = li.find("a", href=True)
                if not a_tag:
                    continue
                raw_title_sa = NUM_BULLET.sub("", a_tag.get_text(strip=True))
                if not raw_title_sa or raw_title_sa.lower() in SKIP_TITLES:
                    continue
                href = a_tag["href"]
                if not href:
                    logger.warning(f"{log_prefix} Empty href for '{raw_title_sa}'.")
                    continue
                full_url = urljoin(base_url, href)
                if full_url in processed_links:
                    continue
                path_match = PATH_RE.search(full_url)
                if (
                    not path_match
                    or not path_match.group("work_id")
                    or not path_match.group("page_id")
                ):
                    continue
                try:
                    page_id = path_match.group("page_id")
                    book_id = int(path_match.group("work_id"))
                    title_iast = _transliterate(raw_title_sa)
                    HttpUrl(full_url)  # Validate URL format
                    initial_books_found.append(
                        {
                            "book_id": book_id,
                            "page_id": page_id,
                            "title_sa": raw_title_sa.strip(),
                            "title_en": title_iast.strip(),
                            "section": section_slug,
                            "order": order,
                            "source_url": full_url,  # Store validated URL string
                        }
                    )
                    processed_links.add(full_url)
                except ValueError:
                    logger.warning(f"{log_prefix} Invalid book_id in URL: {full_url}")
                except ValidationError:
                    logger.warning(
                        f"{log_prefix} Invalid source URL format skipped: {full_url}"
                    )
                except Exception as e:
                    logger.warning(
                        f"{log_prefix} Error processing link {full_url}: {e}"
                    )
        logger.info(
            f"{log_prefix} Initial discovery found {len(initial_books_found)} potential book links."
        )
        enqueued_count = 0
        processed_book_ids = set()
        if arq_redis:
            for book_info in initial_books_found:
                book_id = book_info["book_id"]
                if book_id not in processed_book_ids:
                    try:
                        # Enqueue the task to fetch details for this book
                        await arq_redis.enqueue_job(
                            "fetch_book_details", book_info=book_info
                        )
                        processed_book_ids.add(book_id)
                        enqueued_count += 1
                    except Exception as e:
                        logger.error(
                            f"{log_prefix} Failed to enqueue fetch_book_details for book {book_id}: {e}"
                        )
            logger.info(
                f"{log_prefix} Enqueued {enqueued_count} fetch_book_details tasks."
            )
        else:
            logger.error(
                f"{log_prefix} Cannot enqueue fetch_book_details: ARQ Redis unavailable."
            )
        return f"{log_prefix} Initial discovery finished. Found {len(initial_books_found)} links. Enqueued {enqueued_count} detail fetch tasks."
    except Exception as e:
        logger.exception(f"{log_prefix} Error during initial discovery: {e}")
        raise


async def fetch_book_details(ctx, book_info: dict, *args, **kwargs) -> str:
    """
    ARQ Task 2 (Phase 1): Fetches sidebar HTML and full page URL list for a single book
    using its tree-view endpoint and upserts the complete initial Book document.
    *** This task NO LONGER enqueues any further tasks. ***
    """
    book_id = book_info.get("book_id")
    page_id = book_info.get("page_id")
    source_url_str = book_info.get("source_url")
    log_prefix = f"[FetchDetails|Book {book_id}]"
    logger.info(f"{log_prefix} Starting details fetch for source URL: {source_url_str}")

    if not all([book_id, page_id, source_url_str]):
        return f"{log_prefix} Failed: Missing essential book_info (book_id, page_id, source_url)."
    if not settings or not http_client:
        return f"{log_prefix} Failed: Settings or HTTP client missing."

    # arq_redis = _get_arq_redis_from_context(ctx) # No longer needed for enqueueing
    try:
        mongo_db = get_mongo_db()
    except Exception as e:
        return f"{log_prefix} Failed to get DB connection: {e}"

    base_url = str(settings.target_base_url).rstrip("/")
    sidebar_html: Optional[str] = None
    valid_page_url_strings: List[str] = []
    sidebar_status = "fetch_failed"  # Default status

    tree_view_url = (
        f"{base_url}/category-details/{page_id}/{book_id}/get-categories-tree-view"
    )

    try:
        logger.info(f"{log_prefix} Fetching tree-view/sidebar from: {tree_view_url}")
        tree_view_html = await http_client.get(
            tree_view_url,
            use_proxy=True,
            response_format="text",
            timeout_seconds=settings.scraper_request_timeout_seconds,
        )

        sidebar_html = _extract_sidebar_html(tree_view_html)
        if sidebar_html:
            sidebar_status = "complete"
            valid_page_url_strings = _extract_page_urls_from_sidebar(
                sidebar_html, str(settings.target_base_url), book_id
            )
            logger.success(
                f"{log_prefix} Fetched and parsed sidebar. Found {len(valid_page_url_strings)} page URLs."
            )
            if source_url_str not in valid_page_url_strings:
                try:
                    HttpUrl(source_url_str)
                    valid_page_url_strings.append(source_url_str)
                    valid_page_url_strings = sorted(list(set(valid_page_url_strings)))
                    logger.debug(
                        f"{log_prefix} Added original source URL string to page list."
                    )
                except ValidationError:
                    logger.warning(
                        f"{log_prefix} Original source URL '{source_url_str}' is invalid, not adding."
                    )
        else:
            sidebar_status = "parse_failed"
            logger.warning(
                f"{log_prefix} Could not extract sidebar from {tree_view_url}. Using source URL only."
            )
            try:
                HttpUrl(source_url_str)
                valid_page_url_strings = [source_url_str]
            except ValidationError:
                logger.error(
                    f"{log_prefix} Original source URL '{source_url_str}' is invalid. No page URLs available."
                )
                valid_page_url_strings = []

    except Exception as e:
        logger.error(
            f"{log_prefix} Failed to fetch or process sidebar from {tree_view_url}: {e}"
        )
        sidebar_status = "fetch_failed"
        try:
            HttpUrl(source_url_str)
            valid_page_url_strings = [source_url_str]
        except ValidationError:
            logger.error(
                f"{log_prefix} Original source URL '{source_url_str}' is invalid. No page URLs available."
            )
            valid_page_url_strings = []

    # --- Prepare and Upsert Book Data ---
    try:
        validated_source_url_obj = HttpUrl(source_url_str)

        # Create the BookCreate model instance with Phase 1 statuses
        book_data = BookCreate(
            book_id=book_id,  # Pass the book_id for the model
            title_sa=book_info["title_sa"],
            title_en=book_info["title_en"],
            section=book_info["section"],
            order_in_section=book_info["order"],
            source_url=validated_source_url_obj,
            page_urls=valid_page_url_strings,  # Pass the List[str]
            sidebar_html=sidebar_html,
            # Set statuses according to BookCreate definition for Phase 1
            discovery_status="complete",
            sidebar_status=sidebar_status,
            sutra_discovery_status="pending",  # Mark Phase 2 as pending
            content_fetch_status="pending",  # Mark Phase 2 as pending
            # Reset Phase 2 progress/counts
            sutra_discovery_progress=0.0,
            content_fetch_progress=0.0,
            total_sutras_discovered=0,
            sutras_fetched_count=0,
            sutras_failed_count=0,
        )

        collection = mongo_db[BOOKS_COLLECTION]
        filter_query = {"_id": book_id}
        # Exclude book_id when dumping as it's used in the filter (_id)
        update_data = book_data.model_dump(
            mode="json", exclude_unset=True, exclude={"book_id"}
        )
        now = datetime.now()
        update_doc = {
            # Set all fields from the model
            "$set": update_data,
            # Update timestamp
            "$currentDate": {"updated_at": True},
            # Set creation timestamp only if inserting
            "$setOnInsert": {"created_at": now},
        }

        logger.info(f"{log_prefix} Upserting final book details (Phase 1) into DB...")
        result = await collection.update_one(filter_query, update_doc, upsert=True)
        upsert_msg = (
            f"Matched: {result.matched_count}, Modified: {result.modified_count}"
        )
        if result.upserted_id:
            upsert_msg += f", Upserted ID: {result.upserted_id}"
        logger.success(f"{log_prefix} Book details upsert result: {upsert_msg}")

        # --- DO NOT ENQUEUE NEXT TASK ---
        logger.info(
            f"{log_prefix} Phase 1 complete. Sutra processing will be handled separately."
        )

        return f"{log_prefix} Phase 1 (Book Details) finished for book {book_id}. Sidebar status: {sidebar_status}. Pages found: {len(valid_page_url_strings)}."

    except ValidationError as e:
        logger.error(
            f"{log_prefix} Pydantic validation failed during book detail processing: {e}"
        )
        return f"{log_prefix} Failed: Validation Error"
    except Exception as e:
        logger.exception(f"{log_prefix} Critical error during detail fetch/save: {e}")
        raise  # Let ARQ handle retry/failure if applicable
