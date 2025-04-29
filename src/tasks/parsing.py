# src/tasks/parsing.py

import asyncio
import re
import json
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import defaultdict
from datetime import datetime, timezone  # Ensure timezone is imported
import time
import os
from urllib.parse import urljoin  # Ensure urljoin is imported

from bs4 import BeautifulSoup, Tag, NavigableString, Comment
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

# --- Use ReplaceOne and UpdateOne ---
from pymongo import ReplaceOne, UpdateOne
from devatrans import DevaTrans

from src.db.mongo_client import get_mongo_db
from src.config import settings
from src.models.book import BookUpdate
from pydantic import HttpUrl, ValidationError


# --- Configuration ---
BOOKS_COLLECTION = "books"
SUTRAS_COLLECTION = "sutras"
PREPROCESSED_SUTRAS_COLLECTION = "preprocessed_sutras"
MARKDOWN_PAGES_COLLECTION = "markdown_pages"  # Final output collection
BATCH_SIZE = 500  # For DB writes

# --- Regex for extracting page_id ---
PATH_RE = re.compile(r"/category-details/(?P<page_id>\d+)/(?P<work_id>\d+)")

# --- SIMPLIFIED Preprocessing Constants ---
TAGS_TO_UNWRAP = {"span", "strong", "b", "i", "em", "u"}
TAGS_TO_REMOVE = {
    "script",
    "style",
    "hr",
    "img",
    "button",
    "input",
    "textarea",
    "select",
    "form",
}
EMPTY_TAG_CANDIDATES = {
    "p",
    "div",
    "span",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "pre",
    "ul",
    "ol",
    "li",
    "blockquote",
}
MERGEABLE_TAGS = {"p", "h1", "h2", "h3", "h4", "h5", "h6"}
MERGE_SEPARATOR = " "
MD_HEADING_MAP = {
    "h1": "#",
    "h2": "##",
    "h3": "###",
    "h4": "####",
    "h5": "#####",
    "h6": "######",
}

# --- Initialize DevaTrans ---
try:
    dt = DevaTrans()
except Exception as e:
    logger.error(f"Failed to initialize DevaTrans: {e}")
    dt = None

# --- Helper Functions ---


def _clean_text_content(text: Optional[str]) -> Optional[str]:
    """Basic cleaning for text extracted from tags."""
    if text is None:
        return None
    text = text.replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text if text else None


def _preprocess_html_func(
    html_content: str, sutra_id: Any, book_id: int
) -> Tuple[Optional[str], List[str]]:
    """
    Applies SIMPLIFIED cleaning steps based on user request. (Unchanged from v7)
    """
    flags = []
    log_prefix = f"[{book_id}|{sutra_id}|PreProc]"
    if not html_content:
        flags.append("PREPROCESS_NO_RAW_HTML")
        return None, flags
    try:
        soup = BeautifulSoup(html_content, "lxml")
        content_div = (
            soup.find("div", class_="lazy-1")
            or soup.find("div", id=lambda x: x and x.startswith("article"))
            or soup.find("div", class_="details")
            or soup.body
            or soup
        )
        if not content_div:
            logger.warning(
                f"{log_prefix} Could not find standard content container, processing input directly."
            )
            content_div = soup

        # --- Start of SIMPLIFIED cleaning steps ---
        for comment in content_div.find_all(
            string=lambda text: isinstance(text, Comment)
        ):
            comment.extract()
        for tag_name in TAGS_TO_REMOVE:
            for tag in content_div.find_all(tag_name):
                tag.decompose()
        for tag_name in TAGS_TO_UNWRAP:
            for tag in content_div.find_all(tag_name):
                tag.unwrap()
        for text_node in content_div.find_all(string=True):
            if text_node.parent.name in TAGS_TO_REMOVE:
                continue
            cleaned = _clean_text_content(text_node.string)
            if cleaned:
                text_node.replace_with(cleaned)
            else:
                if (
                    text_node.parent.name == "body"
                    or len(list(text_node.parent.children)) > 1
                ):
                    text_node.extract()
        for tag_name in MERGEABLE_TAGS:
            current_tag = content_div.find(tag_name)
            while current_tag:
                next_sibling = current_tag.find_next_sibling()
                while next_sibling and isinstance(next_sibling, NavigableString):
                    next_sibling = next_sibling.find_next_sibling()
                if next_sibling and next_sibling.name == tag_name:
                    current_text = (
                        _clean_text_content(current_tag.get_text(separator=" ")) or ""
                    )
                    next_text = (
                        _clean_text_content(next_sibling.get_text(separator=" ")) or ""
                    )
                    merged_text = (
                        f"{current_text}{MERGE_SEPARATOR}{next_text}".strip()
                        if current_text and next_text
                        else (current_text or next_text).strip()
                    )
                    current_tag.clear()
                    current_tag.string = merged_text if merged_text else ""
                    next_process_tag = current_tag.find_next(tag_name)
                    next_sibling.decompose()
                    current_tag = next_process_tag
                    continue
                current_tag = current_tag.find_next(tag_name)
        for _ in range(3):
            tags_to_check = content_div.find_all(EMPTY_TAG_CANDIDATES)
            if not tags_to_check:
                break
            made_change = False
            for tag in tags_to_check:
                if not tag.parent:
                    continue
                is_empty = not tag.get_text(strip=True) and not tag.find(
                    lambda t: isinstance(t, Tag) and t.name != "br", recursive=False
                )
                if is_empty:
                    tag.decompose()
                    made_change = True
            if not made_change:
                break
        # --- End of SIMPLIFIED cleaning steps ---
        if content_div == soup:
            processed_html = str(soup)
        else:
            processed_html = content_div.decode_contents().strip()
        if not processed_html and html_content:
            flags.append("PREPROCESS_EMPTY_RESULT")
            logger.warning(f"{log_prefix} Preprocessing resulted in empty HTML.")
        return processed_html, flags
    except Exception as e:
        logger.exception(f"{log_prefix} Error during HTML preprocessing: {e}")
        flags.append("PREPROCESS_ERROR")
        return None, flags


# --- CORRECTED: _build_url_to_nav_map using stack logic based on data-level ---
def _build_url_to_nav_map(html: Optional[str], base_url: str) -> Dict[str, dict]:
    """
    Convert raw sidebar fragment → {absolute_url: {"leaf_title": str, "path": [...]}}.
    Uses the stack-based logic based on iterating through elements with 'data-level'.
    Keys are absolute URLs for matching against source_page_url.
    """
    index: Dict[str, dict] = {}  # Key is str (absolute_url)
    if not html or not base_url:
        logger.warning("Sidebar HTML or base URL missing, cannot build URL map.")
        return index
    try:
        soup = BeautifulSoup(html, "lxml")
        stack: Dict[int, str] = {}  # Stores titles at each level {level: title}

        # --- Find ALL elements with data-level attribute in document order ---
        elements_with_level = soup.select("[data-level]")

        for element in elements_with_level:
            # Find the associated anchor tag (might be the element itself or a child)
            a_tag = (
                element if element.name == "a" else element.find("a", recursive=False)
            )
            # If no link found for this level element, skip (might be a container like LI)
            if not a_tag:
                # Still need to potentially update the stack if this element represents a non-linked level title
                level_str = element.get("data-level")
                level = 0
                try:
                    level = int(level_str) if level_str else 0
                except (ValueError, TypeError):
                    level = 0  # Default level if invalid

                # Try to get a title from the element itself if no <a> tag
                title = _clean_text_content(
                    element.find(string=True, recursive=False)
                )  # Get direct text
                if title:
                    stack[level] = title
                    # discard deeper levels from previous branches
                    for deeper in list(stack.keys()):
                        if deeper > level:
                            del stack[deeper]
                else:
                    logger.debug(
                        f"Element with data-level={level_str} has no link or direct text title. Skipping stack update for this element."
                    )
                continue  # Move to the next element with data-level

            # --- Process elements that have an associated <a> tag ---
            href = a_tag.get("href", "").strip()
            title = _clean_text_content(a_tag.get_text())  # Get title from link
            level_str = element.get(
                "data-level"
            )  # Get level from the element that has it

            try:
                level = int(level_str) if level_str else 0
            except (ValueError, TypeError):
                logger.warning(
                    f"Invalid data-level '{level_str}' for title '{title}'. Using level 0."
                )
                level = 0

            # Skip javascript links or elements without a proper title
            if href.lower().startswith("javascript:") or not title:
                # Update stack even for javascript links if they represent a level title
                stack[level] = title
                for deeper in list(stack.keys()):
                    if deeper > level:
                        del stack[deeper]
                continue  # Skip adding to index if it's not a content link

            # Create and validate absolute URL only for actual content links
            try:
                absolute_url = urljoin(base_url, href)
                HttpUrl(absolute_url)  # Validate
            except ValidationError:
                logger.warning(
                    f"Skipping invalid URL generated from href '{href}' for title '{title}': {absolute_url}"
                )
                continue
            except Exception as url_e:
                logger.warning(
                    f"Error processing href '{href}' for title '{title}': {url_e}"
                )
                continue

            # --- Build path using the stack logic ---
            stack[level] = title
            for deeper in list(stack.keys()):
                if deeper > level:
                    del stack[deeper]
            # Create path as list of dicts {"level": L, "title": T}
            nav_path = [
                {"level": lvl, "title": stack[lvl]} for lvl in sorted(stack.keys())
            ]
            # -----------------------------------------------------------

            if absolute_url in index:
                logger.warning(
                    f"Duplicate absolute_url {absolute_url} found in sidebar map. Overwriting entry for title '{title}'. Previous: '{index[absolute_url].get('leaf_title')}'"
                )
            index[absolute_url] = {"leaf_title": title, "path": nav_path}

    except Exception as e:
        logger.exception(f"Error building URL map from sidebar: {e}")
    return index


def _convert_cleaned_html_to_markdown(
    cleaned_html: Optional[str], sutra_id: Any, book_id: int
) -> Tuple[str, List[str]]:
    """Converts cleaned HTML fragment to basic Markdown string. (Unchanged)"""
    flags = []
    markdown_lines = []
    log_prefix = f"[{book_id}|{sutra_id}|MdConv]"
    if not cleaned_html:
        flags.append("MDCONV_NO_CLEANED_HTML")
        return "", flags
    try:
        soup = BeautifulSoup(cleaned_html, "lxml")
        content_nodes = (
            soup.body.contents if soup.body and soup.body.contents else soup.contents
        )
        for element in content_nodes:
            if isinstance(element, NavigableString):
                text = _clean_text_content(element.string)
                if text:
                    markdown_lines.append(text)
                    markdown_lines.append("")
            elif isinstance(element, Tag):
                tag_name = element.name.lower()
                text = _clean_text_content(element.get_text(separator=" "))
                if tag_name in MD_HEADING_MAP and text:
                    md_heading = MD_HEADING_MAP[tag_name]
                    markdown_lines.append(f"{md_heading} {text}")
                    markdown_lines.append("")
                elif tag_name == "p" and text:
                    markdown_lines.append(text)
                    markdown_lines.append("")
                elif tag_name == "pre" and text:
                    markdown_lines.append("```")
                    markdown_lines.append(element.get_text())
                    markdown_lines.append("```")
                    markdown_lines.append("")
                elif tag_name == "ul":
                    for li in element.find_all("li", recursive=False):
                        li_text = _clean_text_content(li.get_text(separator=" "))
                        if li_text:
                            markdown_lines.append(f"* {li_text}")
                    markdown_lines.append("")
                elif tag_name == "ol":
                    for i, li in enumerate(element.find_all("li", recursive=False)):
                        li_text = _clean_text_content(li.get_text(separator=" "))
                        if li_text:
                            markdown_lines.append(f"{i+1}. {li_text}")
                    markdown_lines.append("")
                elif tag_name == "br":
                    if markdown_lines and markdown_lines[-1] != "":
                        markdown_lines.append("")
        final_markdown = "\n".join(markdown_lines).strip()
        final_markdown = re.sub(r"\n{3,}", "\n\n", final_markdown)
        if not final_markdown and cleaned_html:
            flags.append("MDCONV_EMPTY_OUTPUT")
            logger.warning(
                f"{log_prefix} MD conversion resulted in empty string from non-empty HTML."
            )
        return final_markdown, flags
    except Exception as e:
        logger.exception(f"{log_prefix} Error converting cleaned HTML to Markdown: {e}")
        flags.append("MDCONV_ERROR")
        return "", flags


async def _update_book_status(book_id: int, status_updates: Dict[str, Any]):
    """Updates status fields for a book document in MongoDB. (Unchanged)"""
    status_updates.pop("markdown_output_path", None)
    if not status_updates:
        return
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        updates_to_set = {k: v for k, v in status_updates.items() if v is not None}
        if not updates_to_set:
            return
        updates_to_set["updated_at"] = datetime.now(timezone.utc)
        await books_coll.update_one({"_id": book_id}, {"$set": updates_to_set})
    except Exception as e:
        logger.error(f"Failed to update status for book {book_id}: {e}")


# --- ARQ Task: Phase 3a - Preprocess HTML (Unchanged from v9) ---
async def preprocess_book_html(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3a): Fetches completed sutras, preprocesses raw_html using
    SIMPLIFIED logic, and saves the result to preprocessed_sutras collection.
    Enqueues Phase 3b (Add Nav Path) upon completion.
    """
    log_prefix = f"[PreprocessHTML|Book {book_id}]"
    logger.info(f"{log_prefix} Task started (Simplified Preprocessing).")
    if not settings:
        return f"{log_prefix} Failed: Settings missing."
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        sutras_coll = mongo_db[SUTRAS_COLLECTION]
        preproc_coll = mongo_db[PREPROCESSED_SUTRAS_COLLECTION]
        await preproc_coll.create_index([("book_id", 1), ("sutra_id", 1)], unique=True)
    except Exception as e:
        return f"{log_prefix} Failed: DB connection/setup error: {e}"
    await _update_book_status(book_id, {"phase3_status": "preprocessing"})
    try:
        sutra_cursor = sutras_coll.find(
            {"book_id": book_id, "fetch_status": "complete"},
            {
                "_id": 1,
                "sutra_id": 1,
                "raw_html": 1,
                "tag_html": 1,
                "source_page_url": 1,
                "created_at": 1,
                "updated_at": 1,
            },
        )
        sutra_docs = await sutra_cursor.to_list(length=None)
        if not sutra_docs:
            logger.warning(f"{log_prefix} No completed sutras found for preprocessing.")
            await _update_book_status(book_id, {"phase3_status": "skipped_no_sutras"})
            return f"{log_prefix} Skipped: No completed sutras."
        logger.info(f"{log_prefix} Preprocessing HTML for {len(sutra_docs)} sutras...")
        bulk_ops = []
        processed_count = 0
        error_count = 0
        for sutra_doc in sutra_docs:
            sutra_id = sutra_doc["sutra_id"]
            raw_html = sutra_doc.get("raw_html")
            if not raw_html:
                logger.warning(
                    f"{log_prefix} Sutra {sutra_id} has fetch_status 'complete' but no raw_html. Skipping."
                )
                continue
            processed_html, flags = _preprocess_html_func(raw_html, sutra_id, book_id)
            preprocessed_doc = {
                "_id": sutra_doc["_id"],
                "book_id": book_id,
                "sutra_id": sutra_id,
                "source_page_url": sutra_doc.get("source_page_url"),
                "tag_html": sutra_doc.get("tag_html"),
                "processed_html": processed_html,
                "processing_flags": flags,
                "source_created_at": sutra_doc.get("created_at"),
                "source_updated_at": sutra_doc.get("updated_at"),
                "preprocessed_at": datetime.now(timezone.utc),
            }
            if "PREPROCESS_ERROR" in flags or processed_html is None:
                error_count += 1
            op = ReplaceOne(
                {"_id": preprocessed_doc["_id"]}, preprocessed_doc, upsert=True
            )
            bulk_ops.append(op)
            processed_count += 1
            if len(bulk_ops) >= BATCH_SIZE:
                logger.info(
                    f"{log_prefix} Writing batch of {len(bulk_ops)} preprocessed sutras..."
                )
                try:
                    await preproc_coll.bulk_write(bulk_ops, ordered=False)
                except Exception as e:
                    logger.exception(
                        f"{log_prefix} Bulk write to preprocessed_sutras failed: {e}"
                    )
                bulk_ops = []
        if bulk_ops:
            logger.info(
                f"{log_prefix} Writing final batch of {len(bulk_ops)} preprocessed sutras..."
            )
            try:
                await preproc_coll.bulk_write(bulk_ops, ordered=False)
            except Exception as e:
                logger.exception(
                    f"{log_prefix} Final bulk write to preprocessed_sutras failed: {e}"
                )
        final_status = "preprocessing_complete"
        if error_count > 0:
            final_status = "preprocessing_errors"
            logger.warning(
                f"{log_prefix} Preprocessing completed with {error_count} errors."
            )
        elif processed_count == 0:
            final_status = "skipped_no_sutras"
        await _update_book_status(book_id, {"phase3_status": final_status})
        if error_count < processed_count and final_status != "skipped_no_sutras":
            arq_redis = ctx.get("redis")
            if arq_redis:
                try:
                    logger.info(f"{log_prefix} Enqueueing Add Nav Path task.")
                    await arq_redis.enqueue_job(
                        "add_nav_path_to_preprocessed", book_id=book_id
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Failed to enqueue Add Nav Path task: {e}"
                    )
                    await _update_book_status(
                        book_id, {"phase3_status": "error_navpath_enqueue"}
                    )
            else:
                logger.error(
                    f"{log_prefix} Cannot enqueue Add Nav Path task: ARQ Redis unavailable in context."
                )
                await _update_book_status(
                    book_id, {"phase3_status": "error_navpath_enqueue"}
                )
        elif final_status == "preprocessing_errors":
            logger.error(
                f"{log_prefix} Skipping Add Nav Path task due to preprocessing errors."
            )
        else:
            logger.info(
                f"{log_prefix} No sutras processed or found, skipping Add Nav Path task."
            )
        logger.success(f"{log_prefix} Finished preprocessing {processed_count} sutras.")
        return f"{log_prefix} Phase 3a finished. Status: {final_status}"
    except Exception as e:
        logger.exception(f"{log_prefix} Unhandled error during HTML preprocessing: {e}")
        await _update_book_status(book_id, {"phase3_status": "error"})
        raise


# --- ARQ Task: Phase 3b - Add Navigation Path (Uses Corrected Map Builder) ---
async def add_nav_path_to_preprocessed(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3b): Fetches sidebar, builds nav map using stack logic,
    and updates preprocessed_sutras collection with nav_path, leaf_title, is_orphan.
    Enqueues Phase 3c (Markdown Generation) upon completion.
    """
    log_prefix = f"[AddNavPath|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    if not settings:
        return f"{log_prefix} Failed: Settings missing."
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        preproc_coll = mongo_db[PREPROCESSED_SUTRAS_COLLECTION]
    except Exception as e:
        return f"{log_prefix} Failed: DB connection/setup error: {e}"

    await _update_book_status(book_id, {"phase3_status": "adding_nav_path"})

    try:
        # 1. Fetch Book data for sidebar and base URL
        book_doc = await books_coll.find_one({"_id": book_id})
        if not book_doc:
            await _update_book_status(
                book_id, {"phase3_status": "error_book_not_found"}
            )
            return f"{log_prefix} Failed: Book document not found."
        sidebar_html = book_doc.get("sidebar_html")
        base_url = str(settings.target_base_url) if settings else None
        if not base_url:
            await _update_book_status(
                book_id, {"phase3_status": "error_missing_base_url"}
            )
            return f"{log_prefix} Failed: Target base URL not found in settings."

        # 2. Build URL -> Nav Info Map using CORRECTED stack-based logic
        logger.info(f"{log_prefix} Building URL -> nav_info map from sidebar...")
        url_to_nav_info = _build_url_to_nav_map(sidebar_html, base_url)
        logger.info(
            f"{log_prefix} Sidebar map built with {len(url_to_nav_info)} entries."
        )
        if not url_to_nav_info:
            logger.warning(
                f"{log_prefix} Sidebar map is empty, all pages may be marked as orphan."
            )

        # 3. Iterate through preprocessed sutras and update them
        logger.info(
            f"{log_prefix} Updating preprocessed sutras with navigation info..."
        )
        bulk_ops: List[UpdateOne] = []
        processed_count = 0
        not_found_count = 0

        async for preproc_doc in preproc_coll.find({"book_id": book_id}):
            processed_count += 1
            doc_id = preproc_doc.get("_id")
            source_url = preproc_doc.get("source_page_url")
            sutra_id = preproc_doc.get("sutra_id")  # For logging

            if not source_url or not doc_id:
                logger.warning(
                    f"{log_prefix} Preprocessed doc missing source_page_url or _id (Sutra ID: {sutra_id}). Skipping update."
                )
                continue

            # Find navigation info using the absolute source_url
            nav_info = url_to_nav_info.get(source_url)
            nav_path = []
            leaf_title = f"sutra_{sutra_id}"  # Default
            is_orphan = True

            if nav_info:
                nav_path = nav_info.get(
                    "path", []
                )  # Get the path generated by stack logic
                leaf_title = nav_info.get("leaf_title", leaf_title)
                if nav_path:
                    is_orphan = False
            else:
                not_found_count += 1
                logger.debug(
                    f"{log_prefix} Sutra {sutra_id} (URL: {source_url}) not found in sidebar map. Marking as orphan."
                )

            # Prepare UpdateOne operation
            update_fields = {
                "nav_path": nav_path,
                "leaf_title": leaf_title,
                "is_orphan": is_orphan,
                "nav_path_added_at": datetime.now(timezone.utc),
            }
            op = UpdateOne({"_id": doc_id}, {"$set": update_fields})
            bulk_ops.append(op)

            # Execute batch write
            if len(bulk_ops) >= BATCH_SIZE:
                logger.debug(
                    f"{log_prefix} Writing batch of {len(bulk_ops)} nav_path updates..."
                )
                try:
                    await preproc_coll.bulk_write(bulk_ops, ordered=False)
                except BulkWriteError as bwe:
                    logger.error(
                        f"{log_prefix} Bulk write error during nav_path update: {bwe.details}"
                    )
                except Exception as e:
                    logger.error(f"{log_prefix} Error during nav_path bulk write: {e}")
                bulk_ops = []

        # Write final batch
        if bulk_ops:
            logger.debug(
                f"{log_prefix} Writing final batch of {len(bulk_ops)} nav_path updates..."
            )
            try:
                await preproc_coll.bulk_write(bulk_ops, ordered=False)
            except BulkWriteError as bwe:
                logger.error(
                    f"{log_prefix} Bulk write error during final nav_path update batch: {bwe.details}"
                )
            except Exception as e:
                logger.error(
                    f"{log_prefix} Error during final nav_path bulk write: {e}"
                )

        logger.info(
            f"{log_prefix} Finished updating {processed_count} preprocessed documents. Orphans found: {not_found_count}."
        )

        # 4. Enqueue the final Markdown generation task
        final_status = "nav_path_added"
        await _update_book_status(book_id, {"phase3_status": final_status})

        arq_redis = ctx.get("redis")
        if arq_redis:
            try:
                logger.info(f"{log_prefix} Enqueueing final Markdown generation task.")
                await arq_redis.enqueue_job(
                    "generate_markdown_from_db", book_id=book_id
                )
            except Exception as e:
                logger.error(
                    f"{log_prefix} Failed to enqueue Markdown generation task: {e}"
                )
                await _update_book_status(
                    book_id, {"phase3_status": "error_markdown_enqueue"}
                )
        else:
            logger.error(
                f"{log_prefix} Cannot enqueue Markdown generation task: ARQ Redis unavailable."
            )
            await _update_book_status(
                book_id, {"phase3_status": "error_markdown_enqueue"}
            )

        return f"{log_prefix} Phase 3b finished. Status: {final_status}. Orphans: {not_found_count}."

    except Exception as e:
        logger.exception(f"{log_prefix} Unhandled error during Add Nav Path: {e}")
        await _update_book_status(book_id, {"phase3_status": "error"})
        raise


# --- ARQ Task: Phase 3c - Generate Markdown from DB (Unchanged from v9) ---
async def generate_markdown_from_db(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3c): Fetches preprocessed sutras (including nav_path),
    converts HTML to Markdown, and saves the final document to the
    'markdown_pages' MongoDB collection.
    """
    log_prefix = f"[GenMarkdownDB|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    if not settings:
        return f"{log_prefix} Failed: Settings missing."
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        preproc_coll = mongo_db[PREPROCESSED_SUTRAS_COLLECTION]
        markdown_coll = mongo_db[MARKDOWN_PAGES_COLLECTION]
        await markdown_coll.create_index([("book_id", 1), ("sutra_id", 1)], unique=True)
    except Exception as e:
        return f"{log_prefix} Failed: DB connection/setup error: {e}"

    await _update_book_status(book_id, {"phase3_status": "generating_markdown"})

    try:
        # 1. Fetch Preprocessed Sutras
        sutra_cursor = preproc_coll.find(
            {"book_id": book_id},
            {
                "_id": 1,
                "sutra_id": 1,
                "processed_html": 1,
                "source_page_url": 1,
                "processing_flags": 1,
                "preprocessed_at": 1,
                "nav_path": 1,
                "leaf_title": 1,
                "is_orphan": 1,
            },
        )
        sutra_docs = await sutra_cursor.to_list(length=None)
        if not sutra_docs:
            logger.warning(
                f"{log_prefix} No preprocessed sutras found for this book (maybe nav_path step failed?)."
            )
            await _update_book_status(
                book_id, {"phase3_status": "skipped_no_preprocessed_sutras"}
            )
            return f"{log_prefix} Skipped: No preprocessed sutras."
        logger.info(
            f"{log_prefix} Fetched {len(sutra_docs)} preprocessed sutra documents with nav_path."
        )

        # 2. Convert HTML to Markdown & Prepare DB Operations
        logger.info(
            f"{log_prefix} Converting HTML and preparing final documents for {len(sutra_docs)} pages/sutras..."
        )
        md_conversion_errors = 0
        processed_html_found_count = 0
        bulk_ops: List[ReplaceOne] = []

        for preproc_doc in sutra_docs:
            sutra_id = preproc_doc["sutra_id"]
            processed_html = preproc_doc.get("processed_html")
            doc_id = preproc_doc.get("_id")
            source_url = preproc_doc.get("source_page_url")

            if not doc_id:
                logger.warning(
                    f"{log_prefix} Preprocessed doc missing _id (Sutra ID: {sutra_id}). Skipping."
                )
                continue

            markdown_content = ""
            all_flags = preproc_doc.get("processing_flags", [])
            if processed_html:
                processed_html_found_count += 1
                markdown_string, md_conv_flags = _convert_cleaned_html_to_markdown(
                    processed_html, sutra_id, book_id
                )
                markdown_content = markdown_string
                all_flags.extend(md_conv_flags)
                if "MDCONV_ERROR" in md_conv_flags:
                    md_conversion_errors += 1
            else:
                logger.warning(
                    f"{log_prefix} No processed_html found for sutra {sutra_id}. Saving empty markdown."
                )
                all_flags.append("MDCONV_NO_CLEANED_HTML")

            nav_path = preproc_doc.get("nav_path", [])
            leaf_title = preproc_doc.get("leaf_title", f"sutra_{sutra_id}")
            is_orphan = preproc_doc.get("is_orphan", True if not nav_path else False)

            markdown_page_doc = {
                "_id": doc_id,
                "book_id": book_id,
                "sutra_id": sutra_id,
                "source_page_url": source_url,
                "markdown_content": markdown_content,
                "nav_path": nav_path,
                "leaf_title": leaf_title,
                "is_orphan": is_orphan,
                "processing_flags": all_flags,
                "source_preprocessed_at": preproc_doc.get("preprocessed_at"),
                "generated_at": datetime.now(timezone.utc),
            }
            op = ReplaceOne({"_id": doc_id}, markdown_page_doc, upsert=True)
            bulk_ops.append(op)

            if len(bulk_ops) >= BATCH_SIZE:
                logger.info(
                    f"{log_prefix} Writing batch of {len(bulk_ops)} final markdown documents to DB..."
                )
                try:
                    await markdown_coll.bulk_write(bulk_ops, ordered=False)
                except Exception as e:
                    logger.exception(
                        f"{log_prefix} Bulk write to {MARKDOWN_PAGES_COLLECTION} failed: {e}"
                    )
                bulk_ops = []
        if bulk_ops:
            logger.info(
                f"{log_prefix} Writing final batch of {len(bulk_ops)} final markdown documents to DB..."
            )
            try:
                await markdown_coll.bulk_write(bulk_ops, ordered=False)
            except Exception as e:
                logger.exception(
                    f"{log_prefix} Final bulk write to {MARKDOWN_PAGES_COLLECTION} failed: {e}"
                )

        # 3. Update Final Book Status
        docs_prepared_count = len(sutra_docs)
        final_status = "error_markdown_gen"
        if docs_prepared_count > 0:
            final_status = "complete"
            if md_conversion_errors > 0:
                final_status = "complete_with_md_errors"
        elif len(sutra_docs) > 0:
            final_status = "error_markdown_empty_output"

        await _update_book_status(book_id, {"phase3_status": final_status})
        logger.success(
            f"{log_prefix} Finished processing. Prepared {docs_prepared_count} final markdown documents for DB storage."
        )
        return f"{log_prefix} Phase 3c finished. Status: {final_status}"

    except Exception as e:
        logger.exception(
            f"{log_prefix} Unhandled error during final Markdown DB Generation: {e}"
        )
        await _update_book_status(book_id, {"phase3_status": "error"})
        raise
