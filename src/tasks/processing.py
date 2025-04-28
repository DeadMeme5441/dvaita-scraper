# src/tasks/processing.py

import re
import asyncio

# import math # Not needed here directly
import time
from typing import List, Dict, Optional, Set, Tuple, Any  # Keep necessary types
from urllib.parse import urlparse, urljoin
from contextlib import suppress
from datetime import datetime  # Use standard datetime

from bs4 import BeautifulSoup
from loguru import logger
from pymongo import UpdateOne
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic import ValidationError, HttpUrl
from arq.connections import ArqRedis

# No WorkerContext import

from src.config import settings
from src.utils.http_client import http_client, AsyncHTTPClient
from src.db.mongo_client import get_mongo_db
from src.models.sutra import SutraCreate
from src.models.book import BookInDB

# --- Import adaptive logic function from its new location ---
from src.utils.adaptive_logic import update_adaptive_state

# --- Constants ---
SUTRAS_COLLECTION = "sutras"
BOOKS_COLLECTION = "books"
PATH_RE = re.compile(r"/category-details/(?P<page_id>\d+)/(?P<work_id>\d+)")
SUTRA_ID_SELECTOR = "p.explanation-text[id]"
# Task names for enqueueing
FETCH_SUTRAS_TASK_NAME = "fetch_sutras_for_book"
# Placeholder for Phase 3 trigger
PROCESS_SUTRAS_TASK_NAME = "process_sutra_html"  # Example name
GENERATE_MAP_TASK_NAME = "generate_work_heading_map"  # Example name


# --- Helper Functions ---


def _get_arq_redis_from_context(ctx: dict) -> Optional[ArqRedis]:  # Use dict for ctx
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


def _get_adaptive_state(ctx: dict) -> Optional[dict]:  # Use dict for ctx
    """Safely retrieves the adaptive state dictionary from the worker context."""
    state = ctx.get("adaptive_state")
    if (
        state is None
        or "current_concurrency" not in state
        or "current_timeout_seconds" not in state
    ):
        logger.error("Adaptive state not found or incomplete in worker context.")
        return None
    return state


def _parse_sutra_ids_from_html(html: str, source_url: str) -> Set[int]:
    """Parses paragraph tags with integer IDs from HTML content."""
    sutra_ids: Set[int] = set()
    if not html:
        return sutra_ids
    try:
        soup = BeautifulSoup(html, "lxml")
        for p_tag in soup.select(SUTRA_ID_SELECTOR):
            tag_id = p_tag.get("id")
            if tag_id:
                try:
                    sutra_ids.add(int(tag_id))
                except (ValueError, TypeError):
                    logger.warning(
                        f"Non-integer sutra id '{tag_id}' found on {source_url}"
                    )
    except Exception as e:
        logger.exception(f"Error parsing sutra IDs from {source_url}: {e}")
    return sutra_ids


async def _update_book_status(book_id: int, status_updates: Dict[str, Any]):
    """Updates the status/progress fields for a specific book document in MongoDB."""
    if not status_updates:
        return
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        # Remove None values to avoid overwriting existing valid values with None
        updates_to_set = {k: v for k, v in status_updates.items() if v is not None}
        if not updates_to_set:
            logger.debug(f"No non-None status updates for book {book_id}.")
            return

        # --- FIX: Combine all updates under a single $set operator ---
        # Add the updated_at timestamp to the dictionary being set
        updates_to_set["updated_at"] = datetime.now()

        logger.debug(f"Updating status/progress for book {book_id}: {updates_to_set}")
        await books_coll.update_one(
            {"_id": book_id},
            {
                "$set": updates_to_set
            },  # Single $set with all updates including timestamp
        )
        # -------------------------------------------------------------
    except Exception as e:
        logger.error(f"Failed to update status for book {book_id}: {e}")


async def _fetch_and_parse_page_sutras(
    page_url: str,
    book_id: int,
    http_client_instance: AsyncHTTPClient,
    semaphore: asyncio.Semaphore,
    timeout: float,
    log_prefix: str,
) -> Tuple[str, Set[int], Optional[int], float, Optional[Exception]]:
    """
    Fetches tree-view HTML, parses sutra IDs, respecting semaphore and timeout.
    Returns page_url, found sutra IDs, status code, duration, and error.
    """
    async with semaphore:
        sutra_ids_found: Set[int] = set()
        status_code: Optional[int] = None
        duration: float = 0.0
        error: Optional[Exception] = None
        page_id = "unknown"
        tree_view_url = page_url

        try:
            path_match = PATH_RE.search(page_url)
            if not path_match or not path_match.group("page_id"):
                raise ValueError(f"Could not extract page_id from URL: {page_url}")
            page_id = path_match.group("page_id")
            base_url = str(settings.target_base_url).rstrip("/")
            tree_view_url = f"{base_url}/category-details/{page_id}/{book_id}/get-categories-tree-view"

            # logger.debug(f"{log_prefix} Fetching sutra list from: {tree_view_url} (Timeout: {timeout:.1f}s)") # DEBUG
            status_code, page_html_bytes, duration, error = (
                await http_client_instance.get(
                    tree_view_url,
                    use_proxy=True,
                    response_format="bytes",
                    timeout_seconds=timeout,
                )
            )

            if error is None and status_code == 200 and page_html_bytes:
                try:
                    page_html = page_html_bytes.decode("utf-8")
                    sutra_ids_found = _parse_sutra_ids_from_html(
                        page_html, tree_view_url
                    )
                    # logger.debug(f"{log_prefix} Found {len(sutra_ids_found)} sutra IDs on page {page_id} ({tree_view_url})") # DEBUG
                except (UnicodeDecodeError, Exception) as parse_error:
                    logger.error(
                        f"{log_prefix} Failed to decode/parse HTML from {tree_view_url}: {parse_error}"
                    )
                    error = parse_error
            elif error is None and status_code != 200:
                logger.warning(
                    f"{log_prefix} Received non-200 status {status_code} for {tree_view_url}"
                )
                error = ValueError(f"HTTP status {status_code}")

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in _fetch_and_parse_page_sutras for {page_url}: {type(e).__name__} - {e}"
            )
            if error is None:
                error = e
            if duration == 0.0:
                duration = timeout  # Approximate duration on error

        return page_url, sutra_ids_found, status_code, duration, error


async def _fetch_and_update_sutra_content(
    book_id: int,
    sutra_id: int,
    http_client_instance: AsyncHTTPClient,
    sutras_coll: AsyncIOMotorCollection,
    semaphore: asyncio.Semaphore,
    timeout: float,
    log_prefix: str,
) -> Tuple[int, str, Optional[int], float, Optional[Exception]]:
    """
    Fetches sutra content, updates DB, respecting semaphore and timeout.
    Returns sutra_id, fetch status string, HTTP status code, duration, and error.
    """
    async with semaphore:
        fetch_status = "fetch_failed"
        raw_html: Optional[str] = None
        tag_html: Optional[str] = None
        status_code: Optional[int] = None
        duration: float = 0.0
        error: Optional[Exception] = None

        base_url = str(settings.target_base_url).rstrip("/")
        load_data_url = f"{base_url}/load-data?book_id={book_id}&id={sutra_id}"

        try:
            # logger.debug(f"{log_prefix} Fetching content from: {load_data_url} (Timeout: {timeout:.1f}s)") # DEBUG
            status_code, response_data, duration, error = (
                await http_client_instance.get(
                    load_data_url,
                    use_proxy=True,
                    response_format="json",
                    timeout_seconds=timeout,
                )
            )

            if error is None and status_code == 200 and isinstance(response_data, dict):
                raw_html = response_data.get("html")
                tag_html = response_data.get("tag")
                fetch_status = "complete" if raw_html is not None else "fetch_nodata"
                # log_level = "SUCCESS" if fetch_status == "complete" else "WARNING" # DEBUG
                # logger.log(log_level, f"{log_prefix} Content fetch status for sutra {sutra_id}: {fetch_status}") # DEBUG
            elif (
                error is None
                and status_code == 200
                and not isinstance(response_data, dict)
            ):
                fetch_status = "fetch_bad_response"
                logger.error(
                    f"{log_prefix} Received non-dict JSON response for sutra {sutra_id} from {load_data_url}: Type {type(response_data)}"
                )
                error = ValueError("Bad JSON response format")
            elif error is None and status_code != 200:
                fetch_status = "fetch_failed"
                logger.warning(
                    f"{log_prefix} Received non-200 status {status_code} for {load_data_url}"
                )
                error = ValueError(f"HTTP status {status_code}")

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in _fetch_and_update_sutra_content for sutra {sutra_id}: {type(e).__name__} - {e}"
            )
            if error is None:
                error = e
            fetch_status = "fetch_failed"
            if duration == 0.0:
                duration = timeout

        # Update DB regardless of fetch outcome
        try:
            filter_query = {"_id": {"book_id": book_id, "sutra_id": sutra_id}}
            update_doc = {
                "$set": {
                    "fetch_status": fetch_status,
                    "raw_html": raw_html,
                    "tag_html": tag_html,
                    "updated_at": datetime.now(),  # Use standard datetime.now()
                }
            }
            await sutras_coll.update_one(filter_query, update_doc)
        except Exception as db_e:
            logger.error(f"{log_prefix} Failed DB update for sutra {sutra_id}: {db_e}")
            if error is None:
                error = db_e

        return sutra_id, fetch_status, status_code, duration, error


# --- Phase 2 Task 1: Discover Sutras for a Book (Adaptive) ---


async def discover_book_sutras(ctx: dict, book_id: int) -> str:  # Use dict for ctx
    """
    ARQ Task (Phase 2): Discovers sutra IDs for a book using adaptive concurrency
    and timeouts. Upserts placeholders and enqueues content fetching task.
    """
    log_prefix = f"[DiscoverSutras|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    # --- Get Dependencies & Adaptive State ---
    if not settings or not http_client:
        return f"{log_prefix} Failed: Settings or HTTP client missing."
    adaptive_state = _get_adaptive_state(ctx)
    if not adaptive_state:
        return f"{log_prefix} Failed: Adaptive state missing."
    arq_redis = _get_arq_redis_from_context(ctx)

    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        sutras_coll = mongo_db[SUTRAS_COLLECTION]
    except Exception as e:
        return f"{log_prefix} Failed: DB connection error: {e}"

    # --- Fetch Book Details ---
    try:
        book_data = await books_coll.find_one({"_id": book_id})
        if not book_data:
            return f"{log_prefix} Failed: Book not found."
        # logger.debug(f"{log_prefix} Book data keys: {list(book_data.keys())}") # DEBUG
        book = BookInDB.model_validate(book_data)
        page_urls = book.page_urls if book.page_urls else []
        if not page_urls:
            await _update_book_status(
                book_id,
                {
                    "sutra_discovery_status": "no_pages",
                    "content_fetch_status": "skipped",
                },
            )
            return f"{log_prefix} Completed: No page URLs."
        if book.sutra_discovery_status == "complete":
            logger.info(f"{log_prefix} Sutra discovery already marked complete.")
            return f"{log_prefix} Completed: Discovery already done."

    except ValidationError as e:
        return f"{log_prefix} Failed: Invalid book data in DB: {e}"
    except Exception as e:
        return f"{log_prefix} Failed: Error fetching book details: {e}"

    # --- Sutra Discovery Phase ---
    total_pages = len(page_urls)
    logger.info(f"{log_prefix} Starting sutra discovery across {total_pages} pages.")
    await _update_book_status(
        book_id,
        {"sutra_discovery_status": "in_progress", "sutra_discovery_progress": 0.0},
    )

    current_concurrency = adaptive_state["current_concurrency"]
    current_timeout = adaptive_state["current_timeout_seconds"]
    logger.info(
        f"{log_prefix} Using Concurrency={current_concurrency}, Timeout={current_timeout:.1f}s"
    )
    sutra_discovery_semaphore = asyncio.Semaphore(current_concurrency)

    discovery_tasks = [
        _fetch_and_parse_page_sutras(
            url,
            book_id,
            http_client,
            sutra_discovery_semaphore,
            current_timeout,
            log_prefix,
        )
        for url in page_urls
    ]

    all_discovered_sutras: Dict[int, SutraCreate] = {}
    batch_results_for_adaptation = []
    processed_page_count = 0

    gather_results = await asyncio.gather(*discovery_tasks, return_exceptions=True)

    for i, result in enumerate(gather_results):
        processed_page_count += 1
        page_url_processed = page_urls[i]

        if isinstance(result, Exception):
            logger.error(
                f"{log_prefix} Task-level exception for page {page_url_processed}: {result}"
            )
            batch_results_for_adaptation.append((None, None, current_timeout, result))
        elif isinstance(result, tuple) and len(result) == 5:
            _, sutra_ids_found, status, duration, error = result
            batch_results_for_adaptation.append((status, None, duration, error))

            if error is None and status == 200:
                for sutra_id in sutra_ids_found:
                    if sutra_id not in all_discovered_sutras:
                        try:
                            sutra_data = SutraCreate(
                                sutra_id=sutra_id,
                                book_id=book_id,
                                source_page_url=str(page_url_processed),
                                fetch_status="pending",
                            )
                            all_discovered_sutras[sutra_id] = sutra_data
                        except Exception as model_e:
                            logger.error(
                                f"{log_prefix} Error creating SutraCreate model for {sutra_id} from {page_url_processed}: {model_e}"
                            )
        else:
            logger.error(
                f"{log_prefix} Unexpected result type for page {page_url_processed}: {type(result)}"
            )
            batch_results_for_adaptation.append(
                (None, None, current_timeout, ValueError("Unknown task result type"))
            )

        if (
            processed_page_count % settings.progress_update_interval == 0
            or processed_page_count == total_pages
        ):
            progress = (
                round((processed_page_count / total_pages) * 100, 2)
                if total_pages > 0
                else 100.0
            )
            logger.info(
                f"{log_prefix} Discovery progress: {progress}% ({processed_page_count}/{total_pages})"
            )
            await _update_book_status(book_id, {"sutra_discovery_progress": progress})

    total_sutras_found = len(all_discovered_sutras)
    logger.info(
        f"{log_prefix} Discovery phase finished. Found {total_sutras_found} unique sutras."
    )

    # --- Adapt Settings Based on Batch Results ---
    update_adaptive_state(ctx, batch_results_for_adaptation, log_prefix)

    # --- Database Update and Enqueue Next Task ---
    final_discovery_status = "unknown"
    enqueue_next = False

    if total_sutras_found > 0:
        bulk_ops = []
        for sutra_id, sutra in all_discovered_sutras.items():
            filter_query = {"_id": {"book_id": book_id, "sutra_id": sutra_id}}
            update_data = sutra.model_dump(exclude_unset=True, mode="json")
            now = datetime.now()
            update_doc = {
                "$set": {"updated_at": now},  # Use $set for timestamp
                "$setOnInsert": {**update_data, "created_at": now},
            }
            update_doc["$setOnInsert"].pop("_id", None)
            update_doc["$setOnInsert"].pop("id", None)
            op = UpdateOne(filter_query, update_doc, upsert=True)
            bulk_ops.append(op)

        if bulk_ops:
            logger.info(f"{log_prefix} Upserting {len(bulk_ops)} sutra placeholders...")
            try:
                result = await sutras_coll.bulk_write(bulk_ops, ordered=False)
                logger.success(
                    f"{log_prefix} Sutra placeholder upsert: Upserted={result.upserted_count}, Matched={result.matched_count}"
                )
                final_discovery_status = "complete"
                enqueue_next = True
            except Exception as e:
                logger.exception(f"{log_prefix} Error sutra bulk write: {e}")
                final_discovery_status = "db_error"
        else:
            final_discovery_status = "error"

    else:
        fetch_errors = sum(
            1 for _, _, _, err in batch_results_for_adaptation if err is not None
        )
        if fetch_errors > 0:
            logger.error(
                f"{log_prefix} Sutra discovery failed due to page fetch/parse errors ({fetch_errors})."
            )
            final_discovery_status = "fetch_errors"
        else:
            logger.info(f"{log_prefix} No sutras were found for this book.")
            final_discovery_status = "no_sutras"

    # Get last known progress before potentially overwriting with 100%
    current_book_prog = await books_coll.find_one(
        {"_id": book_id}, {"sutra_discovery_progress": 1}
    )
    last_progress = (
        current_book_prog.get("sutra_discovery_progress", 0.0)
        if current_book_prog
        else 0.0
    )
    final_progress = (
        100.0 if final_discovery_status in ["complete", "no_sutras"] else last_progress
    )

    await _update_book_status(
        book_id,
        {
            "sutra_discovery_status": final_discovery_status,
            "total_sutras_discovered": total_sutras_found,
            "sutra_discovery_progress": final_progress,
            "sutras_fetched_count": 0,
            "sutras_failed_count": 0,
            "content_fetch_status": (
                "pending"
                if enqueue_next
                else (
                    "skipped"
                    if final_discovery_status in ["no_sutras", "no_pages"]
                    else "pending"
                )
            ),
        },
    )

    if enqueue_next:
        if arq_redis:
            try:
                logger.info(
                    f"{log_prefix} Enqueueing content fetch task '{FETCH_SUTRAS_TASK_NAME}'."
                )
                await arq_redis.enqueue_job(FETCH_SUTRAS_TASK_NAME, book_id=book_id)
            except Exception as e:
                logger.error(
                    f"{log_prefix} Failed to enqueue '{FETCH_SUTRAS_TASK_NAME}': {e}"
                )
                await _update_book_status(
                    book_id, {"content_fetch_status": "enqueue_failed"}
                )
        else:
            logger.error(
                f"{log_prefix} Cannot enqueue content fetch task: ARQ Redis unavailable."
            )
            await _update_book_status(
                book_id, {"content_fetch_status": "enqueue_failed"}
            )

    return f"{log_prefix} Finished. Status: {final_discovery_status}. Sutras found: {total_sutras_found}."


# --- Phase 2 Task 2: Fetch Content for Sutras of a Book (Adaptive) ---


async def fetch_sutras_for_book(ctx: dict, book_id: int) -> str:  # Use dict for ctx
    """
    ARQ Task (Phase 2): Fetches content for pending sutras of a book using
    adaptive concurrency and timeouts. Updates sutra docs and book status.
    Triggers Phase 3 processing upon completion.
    """
    log_prefix = f"[FetchContent|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    # --- Get Dependencies & Adaptive State ---
    if not settings or not http_client:
        return f"{log_prefix} Failed: Settings or HTTP client missing."
    adaptive_state = _get_adaptive_state(ctx)
    if not adaptive_state:
        return f"{log_prefix} Failed: Adaptive state missing."
    arq_redis = _get_arq_redis_from_context(ctx)

    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        sutras_coll = mongo_db[SUTRAS_COLLECTION]
    except Exception as e:
        return f"{log_prefix} Failed: DB connection error: {e}"

    # --- Find Sutras to Fetch ---
    sutras_to_fetch_ids: List[int] = []
    try:
        fetch_needed_statuses = [
            "pending",
            "fetch_failed",
            "fetch_bad_response",
            "fetch_nodata",
            "enqueue_failed",
        ]
        cursor = sutras_coll.find(
            {"book_id": book_id, "fetch_status": {"$in": fetch_needed_statuses}},
            {"_id.sutra_id": 1},
        )
        sutras_to_fetch_ids = [
            doc["_id"]["sutra_id"]
            async for doc in cursor
            if doc.get("_id")
            and isinstance(doc["_id"], dict)
            and "sutra_id" in doc["_id"]
        ]

        if not sutras_to_fetch_ids:
            logger.info(f"{log_prefix} No sutras found needing content fetch.")
            total_sutras_exist = await sutras_coll.count_documents({"book_id": book_id})
            final_book_status = "complete" if total_sutras_exist > 0 else "skipped"
            current_book_status_doc = await books_coll.find_one(
                {"_id": book_id}, {"content_fetch_status": 1}
            )
            current_status = (
                current_book_status_doc.get("content_fetch_status")
                if current_book_status_doc
                else None
            )
            if current_status != "complete":
                await _update_book_status(
                    book_id,
                    {
                        "content_fetch_status": final_book_status,
                        "content_fetch_progress": 100.0,
                    },
                )
            if final_book_status == "complete":
                await _trigger_phase3_processing(arq_redis, book_id, log_prefix)
            return f"{log_prefix} Completed: No pending sutras."

    except Exception as e:
        await _update_book_status(book_id, {"content_fetch_status": "db_error"})
        return f"{log_prefix} Failed: Error querying sutras: {e}"

    # --- Content Fetching Phase ---
    num_to_fetch = len(sutras_to_fetch_ids)
    logger.info(f"{log_prefix} Starting content fetching for {num_to_fetch} sutras.")
    await _update_book_status(
        book_id, {"content_fetch_status": "in_progress", "content_fetch_progress": 0.0}
    )

    current_concurrency = adaptive_state["current_concurrency"]
    current_timeout = adaptive_state["current_timeout_seconds"]
    logger.info(
        f"{log_prefix} Using Concurrency={current_concurrency}, Timeout={current_timeout:.1f}s"
    )
    content_fetch_semaphore = asyncio.Semaphore(current_concurrency)

    content_tasks = [
        _fetch_and_update_sutra_content(
            book_id,
            s_id,
            http_client,
            sutras_coll,
            content_fetch_semaphore,
            current_timeout,
            log_prefix,
        )
        for s_id in sutras_to_fetch_ids
    ]

    batch_results_for_adaptation = []
    content_fetch_outcomes: Dict[str, int] = {
        "complete": 0,
        "fetch_failed": 0,
        "fetch_nodata": 0,
        "fetch_bad_response": 0,
        "task_exception": 0,
    }
    processed_content_count = 0

    gather_results = await asyncio.gather(*content_tasks, return_exceptions=True)

    for i, result in enumerate(gather_results):
        processed_content_count += 1
        sutra_id_processed = sutras_to_fetch_ids[i]

        if isinstance(result, Exception):
            logger.error(
                f"{log_prefix} Task-level exception for sutra {sutra_id_processed}: {result}"
            )
            content_fetch_outcomes["task_exception"] += 1
            content_fetch_outcomes["fetch_failed"] += 1
            batch_results_for_adaptation.append((None, None, current_timeout, result))
        elif isinstance(result, tuple) and len(result) == 5:
            _, final_status, status_code, duration, error = result
            batch_results_for_adaptation.append((status_code, None, duration, error))

            if final_status in content_fetch_outcomes:
                content_fetch_outcomes[final_status] += 1
            else:
                logger.warning(
                    f"{log_prefix} Unknown status '{final_status}' for sutra {sutra_id_processed}"
                )
                content_fetch_outcomes["fetch_failed"] += 1
        else:
            logger.error(
                f"{log_prefix} Unexpected result type for sutra {sutra_id_processed}: {type(result)}"
            )
            content_fetch_outcomes["task_exception"] += 1
            content_fetch_outcomes["fetch_failed"] += 1
            batch_results_for_adaptation.append(
                (None, None, current_timeout, ValueError("Unknown task result type"))
            )

        if (
            processed_content_count % settings.progress_update_interval == 0
            or processed_content_count == num_to_fetch
        ):
            progress = (
                round((processed_content_count / num_to_fetch) * 100, 2)
                if num_to_fetch > 0
                else 100.0
            )
            logger.info(
                f"{log_prefix} Content fetch progress: {progress}% ({processed_content_count}/{num_to_fetch})"
            )
            await _update_book_status(book_id, {"content_fetch_progress": progress})

    logger.info(
        f"{log_prefix} Content fetching phase finished. Outcomes: {content_fetch_outcomes}"
    )

    # --- Adapt Settings Based on Batch Results ---
    update_adaptive_state(ctx, batch_results_for_adaptation, log_prefix)

    # --- Determine Final Book Status ---
    final_content_status = "unknown"
    total_succeeded = content_fetch_outcomes["complete"]
    total_failed = (
        content_fetch_outcomes["fetch_failed"]
        + content_fetch_outcomes["task_exception"]
    )
    total_nodata_badresp = (
        content_fetch_outcomes["fetch_nodata"]
        + content_fetch_outcomes["fetch_bad_response"]
    )

    if total_succeeded > 0 and total_failed == 0 and total_nodata_badresp == 0:
        final_content_status = "complete"
    elif total_succeeded > 0 and total_failed == 0:
        final_content_status = "complete"
    elif total_succeeded > 0 and total_failed > 0:
        final_content_status = "partial_failure"
    elif total_succeeded == 0 and total_failed > 0:
        final_content_status = "failed"
    elif total_succeeded == 0 and total_failed == 0 and total_nodata_badresp > 0:
        final_content_status = "no_content_found"
    elif num_to_fetch > 0:
        logger.warning(
            f"{log_prefix} Inconsistent final status calc. Outcomes: {content_fetch_outcomes}"
        )
        final_content_status = "error"
    else:
        final_content_status = "skipped"

    # --- Final Book Status Update & Trigger Phase 3 ---
    current_book_prog = await books_coll.find_one(
        {"_id": book_id}, {"content_fetch_progress": 1}
    )
    last_progress = (
        current_book_prog.get("content_fetch_progress", 0.0)
        if current_book_prog
        else 0.0
    )
    final_progress = (
        100.0
        if final_content_status in ["complete", "no_content_found", "skipped"]
        else last_progress
    )

    await _update_book_status(
        book_id,
        {
            "content_fetch_status": final_content_status,
            "content_fetch_progress": final_progress,
            "sutras_fetched_count": total_succeeded,
            "sutras_failed_count": total_failed,
        },
    )

    if final_content_status in ["complete", "no_content_found"]:
        await _trigger_phase3_processing(arq_redis, book_id, log_prefix)

    return f"{log_prefix} Finished. Status: {final_content_status}. Success: {total_succeeded}, Fail: {total_failed}, NoData/BadResp: {total_nodata_badresp}"


async def _trigger_phase3_processing(
    arq_redis: Optional[ArqRedis], book_id: int, log_prefix: str
):
    """Helper to enqueue the first task of Phase 3."""
    if not arq_redis:
        logger.error(f"{log_prefix} Cannot trigger Phase 3: ARQ Redis unavailable.")
        # Consider adding a 'phase3_status' field to the book model
        # await _update_book_status(book_id, {"phase3_status": "enqueue_failed"})
        return

    try:
        logger.info(
            f"{log_prefix} Phase 2 complete. Phase 3 processing would be triggered for book {book_id}."
        )
        # Example: await arq_redis.enqueue_job(GENERATE_MAP_TASK_NAME, book_id=book_id)
        # await _update_book_status(book_id, {"phase3_status": "pending"})
    except Exception as e:
        logger.error(
            f"{log_prefix} Failed to enqueue Phase 3 task for book {book_id}: {e}"
        )
        # await _update_book_status(book_id, {"phase3_status": "enqueue_failed"})
