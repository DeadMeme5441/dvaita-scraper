# src/tasks/parsing.py

import asyncio
import re
import json
import yaml  # For YAML frontmatter
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import defaultdict
from datetime import datetime
import time  # For timestamping
import os  # For path manipulation

from bs4 import BeautifulSoup, Tag, NavigableString, Comment
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import UpdateOne  # Use UpdateOne for upserting
from slugify import slugify
from devatrans import DevaTrans  # Still needed for sidebar index titles potentially

# Assuming project structure allows these imports
from src.db.mongo_client import get_mongo_db
from src.config import settings

# Import model for status updates
from src.models.book import BookUpdate

# --- Configuration ---
BOOKS_COLLECTION = "books"
SUTRAS_COLLECTION = "sutras"  # Source of raw_html for Phase 3a
PREPROCESSED_SUTRAS_COLLECTION = (
    "preprocessed_sutras"  # Target for Phase 3a, Source for 3b
)
MARKDOWN_OUTPUT_DIR_BASE = Path("markdown_review")  # Output for Phase 3b
BATCH_SIZE = 500  # For DB writes

# HTML Processing Constants
SHLOKA_P_CLASSES = {"shloka", "shlok", "verse"}
TAGS_TO_UNWRAP = {"span", "strong", "b", "i", "em", "u"}
TAGS_TO_REMOVE = {"script", "style", "hr"}
EMPTY_TAG_CANDIDATES = {"p", "div", "span", "h1", "h2", "h3", "h4", "h5", "h6", "pre"}
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
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text if text else None


def _preprocess_html_func(
    html_content: str, sutra_id: Any, book_id: int
) -> Tuple[Optional[str], List[str]]:
    """Applies cleaning steps. Returns cleaned HTML string and flags."""
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
            flags.append("PREPROCESS_NO_CONTENT_DIV")
            logger.warning(f"{log_prefix} Could not find any content container.")
            return html_content, flags
        for p_tag in content_div.find_all("p"):
            current_classes = p_tag.get("class", [])
            if any(cls in SHLOKA_P_CLASSES for cls in current_classes):
                p_tag.name = "h2"
                p_tag.attrs.pop("class", None)
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
        for element in content_div.find_all(True):
            direct_texts = [
                t for t in element.contents if isinstance(t, NavigableString)
            ]
            for text_node in direct_texts:
                cleaned = _clean_text_content(text_node.string)
                if cleaned:
                    text_node.replace_with(cleaned)
                else:
                    text_node.extract()
        for tag_name in MERGEABLE_TAGS:
            current_tag = content_div.find(tag_name)
            while current_tag:
                next_sibling = current_tag.find_next_sibling()
                if next_sibling and next_sibling.name == tag_name:
                    current_text = _clean_text_content(
                        current_tag.get_text(separator=" ")
                    )
                    next_text = _clean_text_content(
                        next_sibling.get_text(separator=" ")
                    )
                    merged_text = ""
                    if current_text and next_text:
                        merged_text = (
                            f"{current_text}{MERGE_SEPARATOR}{next_text}".strip()
                        )
                    elif next_text:
                        merged_text = next_text.strip()
                    elif current_text:
                        merged_text = current_text.strip()
                    if merged_text:
                        current_tag.string = merged_text
                    else:
                        current_tag.string = ""
                    for child in list(current_tag.children):
                        if not isinstance(child, NavigableString):
                            child.decompose()
                    next_sibling.decompose()
                    continue
                current_tag = current_tag.find_next(tag_name)
        for tag in content_div.find_all(True):
            if not tag.parent:
                continue
            is_void = tag.name in ["br", "img", "hr"]
            if (
                not is_void
                and not tag.get_text(strip=True)
                and not tag.find(lambda t: isinstance(t, Tag), recursive=False)
            ):
                tag.decompose()
        processed_html = content_div.decode_contents()
        return processed_html, flags
    except Exception as e:
        logger.error(f"{log_prefix} Error during HTML preprocessing: {e}")
        flags.append("PREPROCESS_ERROR")
        return None, flags


def _build_sidebar_index(html: Optional[str]) -> Dict[str, dict]:
    """Convert raw sidebar fragment → {relative_href: {"leaf_title": str, "path": [...]}}"""
    index: Dict[str, dict] = {}
    if not html:
        return index
    try:
        soup = BeautifulSoup(html, "lxml")
        stack: Dict[int, str] = {}
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            level = int(a.get("data-level", 0))
            title = _clean_text_content(a.get_text())
            if not href or href == "javascript:void(0)" or not title:
                continue
            stack[level] = title
            for deeper in list(stack.keys()):
                if deeper > level:
                    del stack[deeper]
            path = [{"level": lvl, "title": stack[lvl]} for lvl in sorted(stack)]
            relative_href = (
                href.split("dvaitavedanta.in", 1)[-1]
                if "dvaitavedanta.in" in href
                else href
            )
            if not relative_href.startswith("/") and "http" not in relative_href:
                relative_href = "/" + relative_href
            index[relative_href] = {"leaf_title": title, "path": path}
            if relative_href != href:
                index[href] = {"leaf_title": title, "path": path}
    except Exception as e:
        logger.error(f"Error building sidebar index: {e}")
    return index


def _convert_cleaned_html_to_markdown(
    cleaned_html: Optional[str], sutra_id: Any, book_id: int
) -> Tuple[str, List[str]]:
    """Converts cleaned HTML fragment to basic Markdown string."""
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
        for i, element in enumerate(content_nodes):
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
                elif tag_name == "br":
                    if markdown_lines and markdown_lines[-1] != "":
                        markdown_lines.append("")
        final_markdown = "\n".join(markdown_lines).strip()
        final_markdown = re.sub(r"\n{3,}", "\n\n", final_markdown)
        if not final_markdown and cleaned_html:
            flags.append("MDCONV_EMPTY_OUTPUT")
            logger.warning(f"{log_prefix} MD conversion resulted in empty string.")
        return final_markdown, flags
    except Exception as e:
        logger.error(f"{log_prefix} Error converting cleaned HTML to Markdown: {e}")
        flags.append("MDCONV_ERROR")
        return "", flags


def _get_hierarchical_filepath(
    base_dir: Path, path_key: Tuple[str, ...], index: int
) -> Path:
    """Creates a hierarchical path and filename from the navigation path."""
    current_path = base_dir
    for i, title in enumerate(path_key):
        dir_slug = slugify(f"L{i+1}_{title}", separator="_", max_length=50)
        current_path /= dir_slug
    if path_key:
        last_title_slug = slugify(
            f"L{len(path_key)}_{path_key[-1]}", separator="_", max_length=50
        )
        filename = f"{index:03d}_{last_title_slug}.md"
    else:
        filename = f"{index:03d}_ORPHANED_PAGES.md"
    return current_path / filename


async def _update_book_status(book_id: int, status_updates: Dict[str, Any]):
    """Updates the status fields for a specific book document in MongoDB."""
    if not status_updates:
        return
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        updates_to_set = {k: v for k, v in status_updates.items() if v is not None}
        if not updates_to_set:
            return
        updates_to_set["updated_at"] = datetime.now()
        await books_coll.update_one({"_id": book_id}, {"$set": updates_to_set})
    except Exception as e:
        logger.error(f"Failed to update status for book {book_id}: {e}")


# --- Function to generate markdown for a subsection ---
# Make sure this function is defined *before* it's called in generate_markdown_from_preprocessed
def _generate_markdown_for_subsection(
    book_id: int,
    path_key: Tuple[str, ...],
    sutras_in_subsection: List[Dict],
) -> str:
    """Generates the Markdown content string for a single subsection."""
    md_lines = []
    # 1. YAML Frontmatter
    frontmatter = {"book_id": book_id, "subsection_path": list(path_key)}
    md_lines.append("---")
    md_lines.append(
        yaml.dump(
            frontmatter,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
            width=1000,
        )
    )
    md_lines.append("---")
    md_lines.append("")
    # 2. Subsection Title
    subsection_title = path_key[-1] if path_key else "Orphaned Pages"
    md_lines.append(f"# {subsection_title}")
    md_lines.append("")
    # 3. Iterate through Sutras
    for sutra_data in sutras_in_subsection:
        sutra_id = sutra_data["sutra_id"]
        leaf_title = sutra_data.get("leaf_title", f"Sutra {sutra_id}")
        source_url = sutra_data["source_page_url"]
        flags = sutra_data.get("flags", [])
        markdown_str = sutra_data.get("markdown", "")
        md_lines.append("---")  # Separator
        md_lines.append(f"## Sutra Page: {sutra_id} ({leaf_title})")
        md_lines.append(f"[Source URL]({source_url})")
        if flags:
            md_lines.append(f"")
        md_lines.append("---")
        md_lines.append("")
        md_lines.append(markdown_str if markdown_str else "")
        md_lines.append("")
    return "\n".join(md_lines)


# --- ARQ Task: Phase 3a - Preprocess HTML ---


async def preprocess_book_html(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3a): Fetches completed sutras, preprocesses raw_html,
    and saves the result (including metadata) to preprocessed_sutras collection.
    Enqueues Phase 3b (Markdown generation) upon completion.
    """
    log_prefix = f"[PreprocessHTML|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

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
                error_count += 1
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
                "preprocessed_at": datetime.now(),
            }
            if "PREPROCESS_ERROR" in flags or processed_html is None:
                error_count += 1

            op = UpdateOne(
                {"_id": preprocessed_doc["_id"]},
                {"$set": preprocessed_doc},
                upsert=True,
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

        final_status = (
            "preprocessing_complete" if error_count == 0 else "preprocessing_errors"
        )
        await _update_book_status(book_id, {"phase3_status": final_status})

        if error_count < processed_count:
            arq_redis = ctx.get("redis")
            if arq_redis:
                try:
                    await arq_redis.enqueue_job(
                        "generate_markdown_from_preprocessed", book_id=book_id
                    )
                    logger.info(f"{log_prefix} Enqueued Markdown generation task.")
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
        else:
            logger.error(
                f"{log_prefix} Skipping Markdown generation due to preprocessing errors for all sutras."
            )

        logger.success(
            f"{log_prefix} Finished preprocessing {processed_count} sutras with {error_count} errors."
        )
        return f"{log_prefix} Phase 3a finished. Status: {final_status}"

    except Exception as e:
        logger.exception(f"{log_prefix} Unhandled error during HTML preprocessing: {e}")
        await _update_book_status(book_id, {"phase3_status": "error"})
        return f"{log_prefix} Failed with unhandled error."


# --- ARQ Task: Phase 3b - Generate Markdown from Preprocessed ---


async def generate_markdown_from_preprocessed(ctx: dict, book_id: int) -> str:
    """
    ARQ Task (Phase 3b): Fetches preprocessed sutras, converts HTML to Markdown,
    groups by subsection, and generates combined, hierarchical Markdown files.
    """
    log_prefix = f"[GenMarkdown|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    if not settings:
        return f"{log_prefix} Failed: Settings missing."
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        preproc_coll = mongo_db[
            PREPROCESSED_SUTRAS_COLLECTION
        ]  # Read from preprocessed
    except Exception as e:
        return f"{log_prefix} Failed: DB connection error: {e}"

    await _update_book_status(book_id, {"phase3_status": "generating_markdown"})

    try:
        book_doc = await books_coll.find_one({"_id": book_id})
        if not book_doc:
            await _update_book_status(
                book_id, {"phase3_status": "error_book_not_found"}
            )
            return f"{log_prefix} Failed: Book document not found."
        sidebar_html = book_doc.get("sidebar_html")

        sutra_cursor = preproc_coll.find(
            {"book_id": book_id},
            {
                "_id": 1,
                "sutra_id": 1,
                "processed_html": 1,
                "source_page_url": 1,
                "processing_flags": 1,
            },
        ).sort("_id.sutra_id", 1)
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

        logger.info(f"{log_prefix} Building sidebar index...")
        sidebar_index = _build_sidebar_index(sidebar_html)
        logger.info(
            f"{log_prefix} Sidebar index built with {len(sidebar_index)} entries."
        )

        logger.info(
            f"{log_prefix} Converting HTML to Markdown for {len(sutra_docs)} sutras..."
        )
        sutra_markdown_content: Dict[str, Dict] = {}
        md_conversion_errors = 0
        for sutra_doc in sutra_docs:
            sutra_id = sutra_doc["sutra_id"]
            processed_html = sutra_doc.get("processed_html")
            source_url = sutra_doc.get("source_page_url")
            if not source_url:
                continue

            markdown_string, md_conv_flags = _convert_cleaned_html_to_markdown(
                processed_html, sutra_id, book_id
            )
            all_flags = sutra_doc.get("processing_flags", []) + md_conv_flags
            if "MDCONV_ERROR" in md_conv_flags:
                md_conversion_errors += 1

            sutra_markdown_content[source_url] = {
                "sutra_id": sutra_id,
                "markdown": markdown_string,
                "flags": all_flags,
            }

        logger.info(f"{log_prefix} Grouping processed data by subsection...")
        subsections_data: Dict[Tuple[str, ...], List[Dict]] = defaultdict(list)
        processed_urls = set()
        for url, nav_info in sidebar_index.items():
            relative_url = (
                url.replace("https://dvaitavedanta.in", "", 1)
                if "dvaitavedanta.in" in url
                else url
            )
            processed_sutra = sutra_markdown_content.get(
                url
            ) or sutra_markdown_content.get(relative_url)
            path_key = tuple(item["title"] for item in nav_info.get("path", []))
            leaf_title = nav_info.get("leaf_title", f'Page {url.split("/")[-1]}')
            if processed_sutra:
                subsections_data[path_key].append(
                    {
                        "sutra_id": processed_sutra["sutra_id"],
                        "source_page_url": url,
                        "leaf_title": leaf_title,
                        "markdown": processed_sutra["markdown"],
                        "flags": processed_sutra["flags"],
                    }
                )
                processed_urls.add(url)
                processed_urls.add(relative_url)
        orphaned_pages = []
        for url, processed_sutra in sutra_markdown_content.items():
            if url not in processed_urls:
                orphaned_pages.append(
                    {
                        "sutra_id": processed_sutra["sutra_id"],
                        "source_page_url": url,
                        "leaf_title": f"Orphaned Page {processed_sutra['sutra_id']}",
                        "markdown": processed_sutra["markdown"],
                        "flags": processed_sutra["flags"],
                    }
                )
        if orphaned_pages:
            logger.warning(f"{log_prefix} Found {len(orphaned_pages)} orphaned pages.")
            orphaned_pages.sort(key=lambda x: x["sutra_id"])
            subsections_data[("_ORPHANED_",)] = orphaned_pages

        logger.info(
            f"{log_prefix} Generating {len(subsections_data)} hierarchical markdown files..."
        )
        markdown_base_dir = MARKDOWN_OUTPUT_DIR_BASE / f"work_{book_id}"
        output_paths = []
        subsection_counter = 0
        sorted_subsections = sorted(
            subsections_data.items(), key=lambda item: item[0][0] if item[0] else ""
        )
        for path_key, sutras_in_subsection in sorted_subsections:
            subsection_counter += 1
            filepath = _get_hierarchical_filepath(
                markdown_base_dir, path_key, subsection_counter
            )
            try:
                filepath.parent.mkdir(parents=True, exist_ok=True)
                # --- Make sure to call the correctly defined helper ---
                markdown_content = _generate_markdown_for_subsection(
                    book_id=book_id,
                    path_key=path_key,
                    sutras_in_subsection=sutras_in_subsection,
                )
                # ------------------------------------------------------
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(markdown_content)
                output_paths.append(str(filepath))
            except Exception as e:
                logger.error(
                    f"{log_prefix} Failed to generate/save markdown for subsection {path_key} to {filepath}: {e}"
                )

        final_status = (
            "pending_markdown_review" if output_paths else "error_markdown_gen"
        )
        if md_conversion_errors > 0:
            final_status = "error_markdown_conv"

        await _update_book_status(
            book_id,
            {
                "phase3_status": final_status,
                "markdown_output_path": (
                    str(markdown_base_dir) if output_paths else None
                ),
            },
        )
        logger.success(
            f"{log_prefix} Finished processing. Generated {len(output_paths)} markdown files in '{markdown_base_dir}'. MD Conversion Errors: {md_conversion_errors}"
        )
        return f"{log_prefix} Phase 3b finished. Status: {final_status}"

    except Exception as e:
        logger.exception(
            f"{log_prefix} Unhandled error during Markdown generation: {e}"
        )
        await _update_book_status(book_id, {"phase3_status": "error"})
        return f"{log_prefix} Failed with unhandled error."
