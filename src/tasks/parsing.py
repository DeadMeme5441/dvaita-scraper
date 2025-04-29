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

# --- Use ReplaceOne for simpler upsert logic based on _id ---
from pymongo import ReplaceOne
from slugify import slugify
from devatrans import DevaTrans

from src.db.mongo_client import get_mongo_db
from src.config import settings
from src.models.book import BookUpdate
from pydantic import HttpUrl, ValidationError


# --- Configuration ---
BOOKS_COLLECTION = "books"
SUTRAS_COLLECTION = "sutras"
PREPROCESSED_SUTRAS_COLLECTION = "preprocessed_sutras"
MARKDOWN_PAGES_COLLECTION = "markdown_pages"  # Storing output in DB
BATCH_SIZE = 500  # For DB writes

# --- Regex for extracting page_id ---
PATH_RE = re.compile(r"/category-details/(?P<page_id>\d+)/(?P<work_id>\d+)")

# --- SIMPLIFIED Preprocessing Constants ---
# Removed SHLOKA_P_CLASSES as we are not converting based on class
TAGS_TO_UNWRAP = {"span", "strong", "b", "i", "em", "u"}  # Keep these
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
}  # Keep these, added common form/image tags
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
}  # Keep these, added list items etc.
MERGEABLE_TAGS = {"p", "h1", "h2", "h3", "h4", "h5", "h6"}  # Keep these
MERGE_SEPARATOR = " "
MD_HEADING_MAP = {  # Keep basic heading conversion
    "h1": "#",
    "h2": "##",
    "h3": "###",
    "h4": "####",
    "h5": "#####",
    "h6": "######",
}

# --- Initialize DevaTrans (Unchanged) ---
try:
    dt = DevaTrans()
except Exception as e:
    logger.error(f"Failed to initialize DevaTrans: {e}")
    dt = None

# --- Helper Functions ---


def _clean_text_content(text: Optional[str]) -> Optional[str]:
    """Basic cleaning for text extracted from tags. (Unchanged)"""
    if text is None:
        return None
    text = text.replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text if text else None


# --- MODIFIED: _preprocess_html_func (Simplified) ---
def _preprocess_html_func(
    html_content: str, sutra_id: Any, book_id: int
) -> Tuple[Optional[str], List[str]]:
    """
    Applies SIMPLIFIED cleaning steps based on user request.
    1. Remove comments, script, style, hr, img, form elements.
    2. Unwrap formatting tags (span, strong, b, i, em, u).
    3. Clean whitespace in text nodes.
    4. Merge adjacent identical mergeable tags (p, h1-h6).
    5. Remove empty tags.
    Returns cleaned HTML string and flags.
    """
    flags = []
    log_prefix = f"[{book_id}|{sutra_id}|PreProc]"
    if not html_content:
        flags.append("PREPROCESS_NO_RAW_HTML")
        return None, flags
    try:
        soup = BeautifulSoup(html_content, "lxml")
        # Try to find a main content container, but process whole soup if none found
        content_div = (
            soup.find("div", class_="lazy-1")
            or soup.find("div", id=lambda x: x and x.startswith("article"))
            or soup.find("div", class_="details")
            or soup.body  # Fallback to body
            or soup  # Fallback to whole soup if no body/specific container
        )
        if not content_div:
            # This case might happen if html_content is just a fragment
            logger.warning(
                f"{log_prefix} Could not find standard content container, processing input directly."
            )
            content_div = soup  # Process the soup object itself

        # --- Start of SIMPLIFIED cleaning steps ---

        # 1. Remove comments
        for comment in content_div.find_all(
            string=lambda text: isinstance(text, Comment)
        ):
            comment.extract()

        # 2. Remove unwanted tags completely
        for tag_name in TAGS_TO_REMOVE:
            for tag in content_div.find_all(tag_name):
                tag.decompose()

        # 3. Unwrap formatting tags (keep content)
        for tag_name in TAGS_TO_UNWRAP:
            for tag in content_div.find_all(tag_name):
                tag.unwrap()

        # 4. Clean text nodes (strip whitespace, normalize space)
        # Use recursive find_all for text nodes within the container
        for text_node in content_div.find_all(string=True):
            # Important: Check if parent is removable first, otherwise skip
            if text_node.parent.name in TAGS_TO_REMOVE:
                continue
            cleaned = _clean_text_content(text_node.string)
            if cleaned:
                text_node.replace_with(cleaned)  # Replace with cleaned version
            else:
                # Only extract if it's not the sole content of its parent (avoid removing tags just because text is gone)
                # Or if the parent itself is likely just a wrapper (like span after unwrap)
                if (
                    text_node.parent.name == "body"
                    or len(list(text_node.parent.children)) > 1
                ):
                    text_node.extract()  # Remove if empty after cleaning

        # 5. Merge adjacent identical mergeable tags (p, h1-h6)
        for tag_name in MERGEABLE_TAGS:
            # Need to iterate carefully as merging modifies the tree
            current_tag = content_div.find(tag_name)
            while current_tag:
                next_sibling = current_tag.find_next_sibling()
                # Find the *actual* next sibling tag, skipping NavigableStrings
                while next_sibling and isinstance(next_sibling, NavigableString):
                    next_sibling = next_sibling.find_next_sibling()

                if next_sibling and next_sibling.name == tag_name:
                    # Get text, handling potential None values
                    current_text = (
                        _clean_text_content(current_tag.get_text(separator=" ")) or ""
                    )
                    next_text = (
                        _clean_text_content(next_sibling.get_text(separator=" ")) or ""
                    )

                    # Merge text with separator if both exist
                    merged_text = (
                        f"{current_text}{MERGE_SEPARATOR}{next_text}".strip()
                        if current_text and next_text
                        else (current_text or next_text).strip()
                    )

                    # Update current tag's content - Clear existing children and set string
                    current_tag.clear()
                    current_tag.string = merged_text if merged_text else ""

                    # Store the next tag to process *before* decomposing the sibling
                    next_process_tag = current_tag.find_next(tag_name)
                    # Remove the merged sibling
                    next_sibling.decompose()
                    # Continue loop from the *next potential tag*, not the current one we just modified
                    current_tag = next_process_tag
                    continue  # Skip the normal iteration step

                # Move to the next tag of the same type if no merge happened
                current_tag = current_tag.find_next(tag_name)

        # 6. Remove empty tags (run multiple times)
        for _ in range(3):  # Run cleanup a few times
            tags_to_check = content_div.find_all(EMPTY_TAG_CANDIDATES)
            if not tags_to_check:
                break  # Exit if no more candidates
            made_change = False
            for tag in tags_to_check:
                # Check if tag is still in the tree (might have been decomposed)
                if not tag.parent:
                    continue
                # Check if tag is effectively empty
                is_empty = not tag.get_text(strip=True) and not tag.find(
                    lambda t: isinstance(t, Tag) and t.name != "br", recursive=False
                )
                if is_empty:
                    tag.decompose()
                    made_change = True
            if not made_change:
                break  # Exit if no changes made in a pass

        # --- End of SIMPLIFIED cleaning steps ---

        # Extract the final HTML content from the container (or the whole soup if no container)
        # If we processed the whole soup, get its string representation
        if content_div == soup:
            processed_html = str(soup)
        else:
            processed_html = content_div.decode_contents().strip()  # Get inner HTML

        if not processed_html and html_content:
            flags.append("PREPROCESS_EMPTY_RESULT")
            logger.warning(f"{log_prefix} Preprocessing resulted in empty HTML.")

        return processed_html, flags
    except Exception as e:
        logger.exception(f"{log_prefix} Error during HTML preprocessing: {e}")
        flags.append("PREPROCESS_ERROR")
        return None, flags


def _build_url_to_nav_map(html: Optional[str], base_url: str) -> Dict[str, dict]:
    """
    Convert raw sidebar fragment → {absolute_url: {"leaf_title": str, "path": [...]}}.
    (Unchanged from previous version)
    """
    index: Dict[str, dict] = {}
    if not html or not base_url:
        logger.warning("Sidebar HTML or base URL missing, cannot build URL map.")
        return index
    try:
        soup = BeautifulSoup(html, "lxml")
        stack: Dict[int, str] = {}
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            title = _clean_text_content(a.get_text())
            try:
                level = int(a.get("data-level", 0))
            except ValueError:
                level = 0
            if not href or href.lower().startswith("javascript:") or not title:
                continue
            try:
                absolute_url = urljoin(base_url, href)
                HttpUrl(absolute_url)
            except ValidationError:
                logger.warning(
                    f"Skipping invalid URL generated from href '{href}': {absolute_url}"
                )
                continue
            except Exception as url_e:
                logger.warning(f"Error processing href '{href}': {url_e}")
                continue
            stack[level] = title
            for deeper in list(stack.keys()):
                if deeper > level:
                    del stack[deeper]
            nav_path = [
                {"level": lvl, "title": stack[lvl]} for lvl in sorted(stack.keys())
            ]
            if absolute_url in index:
                logger.warning(
                    f"Duplicate absolute_url {absolute_url} found in sidebar map. Overwriting entry for title '{title}'. Previous: '{index[absolute_url].get('leaf_title')}'"
                )
            index[absolute_url] = {"leaf_title": title, "path": nav_path}
    except Exception as e:
        logger.error(f"Error building URL map from sidebar: {e}")
    return index


def _convert_cleaned_html_to_markdown(
    cleaned_html: Optional[str], sutra_id: Any, book_id: int
) -> Tuple[str, List[str]]:
    """
    Converts cleaned HTML fragment to basic Markdown string.
    Handles only p, h1-h6, pre, ul, ol, br. (Unchanged)
    """
    flags = []
    markdown_lines = []
    log_prefix = f"[{book_id}|{sutra_id}|MdConv]"
    if not cleaned_html:
        flags.append("MDCONV_NO_CLEANED_HTML")
        return "", flags
    try:
        soup = BeautifulSoup(cleaned_html, "lxml")
        # Process direct children of the body or the root fragment
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
                text = _clean_text_content(
                    element.get_text(separator=" ")
                )  # Use space separator
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
                # Ignore other tags for Markdown output
        final_markdown = "\n".join(markdown_lines).strip()
        final_markdown = re.sub(
            r"\n{3,}", "\n\n", final_markdown
        )  # Clean up extra blank lines
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
        updates_to_set["updated_at"] = datetime.now(timezone.utc)  # Ensure timezone
        await books_coll.update_one({"_id": book_id}, {"$set": updates_to_set})
    except Exception as e:
        logger.error(f"Failed to update status for book {book_id}: {e}")


# --- ARQ Task: Phase 3a - Preprocess HTML (Uses Simplified Logic) ---
async def preprocess_book_html(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3a): Fetches completed sutras, preprocesses raw_html using
    SIMPLIFIED logic, and saves the result to preprocessed_sutras collection.
    Enqueues Phase 3b (Markdown generation) upon completion.
    """
    log_prefix = f"[PreprocessHTML|Book {book_id}]"
    logger.info(
        f"{log_prefix} Task started (Simplified Preprocessing)."
    )  # Indicate simplified
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
            # --- Call the simplified preprocessing function ---
            processed_html, flags = _preprocess_html_func(raw_html, sutra_id, book_id)
            # --- Rest of the loop is unchanged ---
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
                "preprocessed_at": datetime.now(timezone.utc),  # Ensure timezone
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
                    logger.info(f"{log_prefix} Enqueueing Markdown generation task.")
                    await arq_redis.enqueue_job(
                        "generate_markdown_from_preprocessed", book_id=book_id
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
                    f"{log_prefix} Cannot enqueue Markdown generation task: ARQ Redis unavailable in context."
                )
                await _update_book_status(
                    book_id, {"phase3_status": "error_markdown_enqueue"}
                )
        elif final_status == "preprocessing_errors":
            logger.error(
                f"{log_prefix} Skipping Markdown generation due to preprocessing errors."
            )
        else:
            logger.info(
                f"{log_prefix} No sutras processed or found, skipping Markdown generation."
            )
        logger.success(f"{log_prefix} Finished preprocessing {processed_count} sutras.")
        return f"{log_prefix} Phase 3a finished. Status: {final_status}"
    except Exception as e:
        logger.exception(f"{log_prefix} Unhandled error during HTML preprocessing: {e}")
        await _update_book_status(book_id, {"phase3_status": "error"})
        raise


# --- ARQ Task: Phase 3b - Generate Markdown and Store in DB (Unchanged from v6) ---
async def generate_markdown_from_preprocessed(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3b): Fetches preprocessed sutras, converts HTML to Markdown,
    and saves the Markdown content along with navigation metadata to the
    'markdown_pages' MongoDB collection. (Unchanged from previous - stores in DB)
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
        # 1. Fetch Book data
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

        # 2. Fetch Preprocessed Sutras
        sutra_cursor = preproc_coll.find(
            {"book_id": book_id},
            {
                "_id": 1,
                "sutra_id": 1,
                "processed_html": 1,
                "source_page_url": 1,
                "processing_flags": 1,
                "preprocessed_at": 1,
            },
        )
        sutra_docs = await sutra_cursor.to_list(length=None)
        if not sutra_docs:
            logger.warning(f"{log_prefix} No preprocessed sutras found for this book.")
            await _update_book_status(
                book_id, {"phase3_status": "skipped_no_preprocessed_sutras"}
            )
            return f"{log_prefix} Skipped: No preprocessed sutras."
        logger.info(
            f"{log_prefix} Fetched {len(sutra_docs)} preprocessed sutra documents."
        )

        # 3. Build URL -> Nav Info Map
        logger.info(f"{log_prefix} Building URL -> nav_info map from sidebar...")
        url_to_nav_info = _build_url_to_nav_map(sidebar_html, base_url)
        logger.info(
            f"{log_prefix} Sidebar map built with {len(url_to_nav_info)} entries."
        )
        if not url_to_nav_info and len(sutra_docs) > 0:
            logger.warning(
                f"{log_prefix} Sidebar map is empty, all pages will be treated as orphaned."
            )

        # 4. Convert HTML to Markdown & Prepare DB Operations
        logger.info(
            f"{log_prefix} Converting HTML and preparing DB operations for {len(sutra_docs)} pages/sutras..."
        )
        md_conversion_errors = 0
        processed_html_found_count = 0
        bulk_ops: List[ReplaceOne] = []

        for sutra_doc in sutra_docs:
            sutra_id = sutra_doc["sutra_id"]
            processed_html = sutra_doc.get("processed_html")
            source_url = sutra_doc.get("source_page_url")
            doc_id = sutra_doc.get("_id")

            if not source_url or not doc_id:
                logger.warning(
                    f"{log_prefix} Sutra doc missing source_page_url or _id (Sutra ID: {sutra_id}). Skipping DB operation."
                )
                continue

            markdown_content = ""
            all_flags = sutra_doc.get("processing_flags", [])
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
                    f"{log_prefix} No processed_html found for sutra {sutra_id} (URL: {source_url}). Saving empty markdown."
                )
                all_flags.append("MDCONV_NO_CLEANED_HTML")

            nav_info = url_to_nav_info.get(source_url)
            nav_path = []
            leaf_title = f"sutra_{sutra_id}"
            is_orphan = True
            if nav_info:
                nav_path = nav_info.get("path", [])
                leaf_title = nav_info.get("leaf_title", leaf_title)
                if nav_path:
                    is_orphan = False
                else:
                    logger.warning(
                        f"{log_prefix} URL {source_url} found in map but has empty path. Sutra ID: {sutra_id}"
                    )
            else:
                logger.debug(
                    f"{log_prefix} Sutra {sutra_id} (URL: {source_url}) not found in sidebar map. Treating as orphan."
                )

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
                "source_preprocessed_at": sutra_doc.get("preprocessed_at"),
                "generated_at": datetime.now(timezone.utc),  # Ensure timezone
            }
            op = ReplaceOne({"_id": doc_id}, markdown_page_doc, upsert=True)
            bulk_ops.append(op)

            if len(bulk_ops) >= BATCH_SIZE:
                logger.info(
                    f"{log_prefix} Writing batch of {len(bulk_ops)} markdown documents to DB..."
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
                f"{log_prefix} Writing final batch of {len(bulk_ops)} markdown documents to DB..."
            )
            try:
                await markdown_coll.bulk_write(bulk_ops, ordered=False)
            except Exception as e:
                logger.exception(
                    f"{log_prefix} Final bulk write to {MARKDOWN_PAGES_COLLECTION} failed: {e}"
                )

        # 5. Update Final Book Status
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
            f"{log_prefix} Finished processing. Prepared {docs_prepared_count} markdown documents for DB storage."
        )
        return f"{log_prefix} Phase 3b finished. Status: {final_status}"

    except Exception as e:
        logger.exception(
            f"{log_prefix} Unhandled error during Markdown DB Generation: {e}"
        )
        await _update_book_status(book_id, {"phase3_status": "error"})
        raise
