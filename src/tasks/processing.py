# src/tasks/processing.py

import re
import asyncio
from typing import List, Dict, Optional, Set, Tuple, Any
from urllib.parse import urlparse, urljoin
from contextlib import suppress

from bs4 import BeautifulSoup
from loguru import logger
from pymongo import UpdateOne
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic import ValidationError, HttpUrl
from datetime import datetime, timezone
from arq.connections import ArqRedis

from src.config import settings
from src.utils.http_client import http_client, AsyncHTTPClient
from src.db.mongo_client import get_mongo_db
from src.models.sutra import SutraCreate
from src.models.book import BookInDB

# --- Constants ---
SUTRAS_COLLECTION = "sutras"
BOOKS_COLLECTION = "books"
PATH_RE = re.compile(r"/category-details/(?P<page_id>\d+)/(?P<work_id>\d+)")
SUTRA_ID_SELECTOR = "p.explanation-text[id]"
# Task names for enqueueing
FETCH_SUTRAS_TASK_NAME = "fetch_sutras_for_book"


# --- Helper Functions (Shared within this module) ---


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


def _parse_sutra_ids_from_html(html: str, source_url: str) -> Set[int]:
    """
    Parses paragraph tags with integer IDs from HTML content.

    Args:
        html: The HTML content string.
        source_url: The URL from which the HTML was fetched (for logging).

    Returns:
        A set of unique integer IDs found.
    """
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
    """
    Updates the status fields for a specific book document in MongoDB.

    Args:
        book_id: The ID of the book to update.
        status_updates: A dictionary containing the fields and values to set.
    """
    if not status_updates:
        return
    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        logger.debug(f"Updating status for book {book_id}: {status_updates}")
        # Use $currentDate for updated_at timestamp
        await books_coll.update_one(
            {"_id": book_id},
            {"$set": status_updates, "$currentDate": {"updated_at": True}},
        )
    except Exception as e:
        # Log error but allow task to potentially continue if possible
        logger.error(f"Failed to update status for book {book_id}: {e}")


async def _fetch_and_parse_page_sutras(
    page_url: str,
    book_id: int,
    http_client_instance: AsyncHTTPClient,
    semaphore: asyncio.Semaphore,
    log_prefix: str,
) -> Tuple[str, Set[int]]:
    """
    Fetches the tree-view HTML for a single page URL, parses sutra IDs,
    respecting the provided semaphore.

    Args:
        page_url: The specific page URL (category-details/...).
        book_id: The book ID associated with the page.
        http_client_instance: The shared AsyncHTTPClient instance.
        semaphore: The asyncio.Semaphore to control concurrency.
        log_prefix: A string prefix for log messages.

    Returns:
        A tuple containing the original page_url and a set of sutra IDs found.
        Returns an empty set if fetching or parsing fails.
    """
    async with semaphore:
        logger.debug(f"{log_prefix} Acquiring semaphore for page: {page_url}")
        sutra_ids_found: Set[int] = set()
        page_id = "unknown"
        tree_view_url = page_url  # Default in case of parsing failure
        try:
            # Extract page_id and construct the specific tree-view URL
            path_match = PATH_RE.search(page_url)
            if not path_match or not path_match.group("page_id"):
                logger.warning(
                    f"{log_prefix} Could not extract page_id from URL: {page_url}. Skipping."
                )
                return page_url, sutra_ids_found  # Return empty set

            page_id = path_match.group("page_id")
            base_url = str(settings.target_base_url).rstrip("/")
            tree_view_url = f"{base_url}/category-details/{page_id}/{book_id}/get-categories-tree-view"

            logger.debug(f"{log_prefix} Fetching sutra list from: {tree_view_url}")

            # Perform the HTTP GET request using the client and configured timeout
            page_html = await http_client_instance.get(
                tree_view_url,
                use_proxy=True,
                response_format="text",
                timeout_seconds=settings.scraper_request_timeout_seconds,  # Use configured timeout
            )

            # Parse the IDs from the fetched HTML
            sutra_ids_found = _parse_sutra_ids_from_html(page_html, tree_view_url)
            logger.debug(
                f"{log_prefix} Found {len(sutra_ids_found)} sutra IDs on page {page_id} ({tree_view_url})"
            )

        except Exception as e:
            # Log specific errors during fetch or parse for this page
            logger.error(
                f"{log_prefix} Failed fetch/parse for page URL {page_url} (target: {tree_view_url}): {type(e).__name__} - {e}"
            )
            # Return empty set on failure for this specific page
            sutra_ids_found = set()

        return page_url, sutra_ids_found


async def _fetch_and_update_sutra_content(
    book_id: int,
    sutra_id: int,
    http_client_instance: AsyncHTTPClient,
    sutras_coll: AsyncIOMotorCollection,
    semaphore: asyncio.Semaphore,
    log_prefix: str,
) -> Tuple[int, str]:
    """
    Fetches the content for a single sutra_id from the /load-data endpoint,
    updates its status and content in MongoDB, respecting the provided semaphore.

    Args:
        book_id: The book ID.
        sutra_id: The sutra ID to fetch content for.
        http_client_instance: The shared AsyncHTTPClient instance.
        sutras_coll: The Motor collection for sutras.
        semaphore: The asyncio.Semaphore to control concurrency.
        log_prefix: A string prefix for log messages.

    Returns:
        A tuple containing the sutra_id and its final fetch status string
        (e.g., 'complete', 'fetch_failed', 'fetch_nodata').
    """
    async with semaphore:
        logger.debug(f"{log_prefix} Acquiring semaphore for sutra content: {sutra_id}")
        base_url = str(settings.target_base_url).rstrip("/")
        load_data_url = f"{base_url}/load-data?book_id={book_id}&id={sutra_id}"
        fetch_status = "fetch_failed"  # Default status
        raw_html = None
        tag_html = None

        try:
            logger.debug(f"{log_prefix} Fetching content from: {load_data_url}")
            # Perform the HTTP GET request
            response_data = await http_client_instance.get(
                load_data_url,
                use_proxy=True,
                response_format="json",  # Expecting JSON
                timeout_seconds=settings.scraper_request_timeout_seconds,  # Use configured timeout
            )

            # Process the response
            if isinstance(response_data, dict):
                raw_html = response_data.get("html")
                tag_html = response_data.get("tag")
                # Determine status based on presence of raw_html
                fetch_status = "complete" if raw_html is not None else "fetch_nodata"
                log_level = "SUCCESS" if fetch_status == "complete" else "WARNING"
                logger.log(
                    log_level,
                    f"{log_prefix} Content fetch status for sutra {sutra_id}: {fetch_status}",
                )
            else:
                # Handle unexpected response format
                fetch_status = "fetch_bad_response"
                logger.error(
                    f"{log_prefix} Received non-dict response for sutra {sutra_id} from {load_data_url}: Type {type(response_data)}"
                )

        except Exception as e:
            # Log errors during the fetch process
            logger.error(
                f"{log_prefix} Failed to fetch content for sutra {sutra_id} from {load_data_url}: {type(e).__name__} - {e}"
            )
            fetch_status = "fetch_failed"  # Ensure status reflects failure

        # Update the corresponding sutra document in MongoDB regardless of fetch outcome
        try:
            filter_query = {"_id": {"book_id": book_id, "sutra_id": sutra_id}}
            update_doc = {
                "$set": {
                    "fetch_status": fetch_status,
                    "raw_html": raw_html,
                    "tag_html": tag_html,
                },
                "$currentDate": {"updated_at": True},  # Update timestamp
            }
            await sutras_coll.update_one(filter_query, update_doc)
        except Exception as e:
            # Log DB update errors but don't let them crash the whole batch
            logger.error(f"{log_prefix} Failed DB update for sutra {sutra_id}: {e}")
            # The fetch_status returned will still reflect the fetch attempt outcome

        return sutra_id, fetch_status


# --- New Task 1: Discover Sutras for a Book ---


async def discover_book_sutras(ctx, book_id: int) -> str:
    """
    ARQ Task: Discovers all unique sutra IDs for a given book by fetching
    and parsing its associated page tree-views concurrently. Upserts placeholder
    documents for found sutras and enqueues the content fetching task.

    Args:
        ctx: The ARQ worker context (contains redis connection pool).
        book_id: The ID of the book to discover sutras for.

    Returns:
        A status message indicating the outcome.
    """
    log_prefix = f"[DiscoverSutras|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    # --- Dependency Checks ---
    if not settings or not http_client:
        logger.error(f"{log_prefix} Failed: Settings or HTTP client not available.")
        return f"{log_prefix} Failed: Missing dependencies."
    arq_redis = _get_arq_redis_from_context(ctx)
    # Note: We don't necessarily need arq_redis if the next task is enqueued later,
    # but good practice to check context access.

    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        sutras_coll = mongo_db[SUTRAS_COLLECTION]
    except Exception as e:
        logger.error(f"{log_prefix} Failed to get DB collections: {e}")
        return f"{log_prefix} Failed: DB connection error."

    # --- Fetch Book Details ---
    try:
        book_data = await books_coll.find_one({"_id": book_id})
        if not book_data:
            logger.error(f"{log_prefix} Book document not found in DB.")
            return f"{log_prefix} Failed: Book not found."

        # Validate book data (optional, but good practice)
        book = BookInDB.model_validate(book_data)
        page_urls = [
            str(url) for url in book.page_urls if url
        ]  # Ensure URLs are strings

        if not page_urls:
            logger.warning(f"{log_prefix} No page URLs found for this book.")
            await _update_book_status(
                book_id,
                {
                    "sutra_discovery_status": "no_pages",
                    "content_fetch_status": "skipped",  # Skip content fetch if no pages
                },
            )
            return f"{log_prefix} Completed: No page URLs to process."

        # Check if discovery was already completed
        if book.sutra_discovery_status == "complete":
            logger.info(
                f"{log_prefix} Sutra discovery already marked as complete. Checking if content fetch needs enqueueing."
            )
            if book.content_fetch_status not in [
                "complete",
                "in_progress",
                "skipped",
                "no_content_found",
            ]:
                if arq_redis:
                    try:
                        await arq_redis.enqueue_job(
                            FETCH_SUTRAS_TASK_NAME, book_id=book_id
                        )
                        logger.info(
                            f"{log_prefix} Enqueued content fetch task as discovery was done but fetch wasn't."
                        )
                        return f"{log_prefix} Skipped discovery (already done), enqueued content fetch."
                    except Exception as e:
                        logger.error(
                            f"{log_prefix} Failed to enqueue content fetch task: {e}"
                        )
                        # Proceed with discovery status update below, but log the enqueue error
                else:
                    logger.error(
                        f"{log_prefix} Cannot enqueue content fetch task: ARQ Redis unavailable."
                    )
            else:
                logger.info(
                    f"{log_prefix} Content fetch status is '{book.content_fetch_status}'. No action needed."
                )
                return f"{log_prefix} Completed: Discovery and fetch already handled."
            # If enqueueing failed or wasn't needed, still return indicating completion.
            return f"{log_prefix} Completed: Discovery already done."

    except ValidationError as e:
        logger.error(f"{log_prefix} Book data validation failed: {e}")
        # Optionally update status to indicate bad data?
        return f"{log_prefix} Failed: Invalid book data in DB."
    except Exception as e:
        logger.error(f"{log_prefix} Failed to retrieve book details: {e}")
        return f"{log_prefix} Failed: Error fetching book details."

    # --- Sutra Discovery Phase ---
    logger.info(f"{log_prefix} Starting sutra discovery across {len(page_urls)} pages.")
    await _update_book_status(book_id, {"sutra_discovery_status": "in_progress"})

    sutra_discovery_semaphore = asyncio.Semaphore(settings.sutra_discovery_concurrency)
    discovery_tasks = [
        _fetch_and_parse_page_sutras(
            url, book_id, http_client, sutra_discovery_semaphore, log_prefix
        )
        for url in page_urls
    ]

    all_discovered_sutras: Dict[int, SutraCreate] = {}
    page_fetch_errors = 0
    processed_page_count = 0

    # Run discovery tasks concurrently
    discovery_results = await asyncio.gather(*discovery_tasks, return_exceptions=True)

    # Process results
    for i, result in enumerate(discovery_results):
        page_url_processed = page_urls[i]
        processed_page_count += 1

        if isinstance(result, Exception):
            logger.error(
                f"{log_prefix} Error during discovery for page {page_url_processed}: {result}"
            )
            page_fetch_errors += 1
        elif isinstance(result, tuple) and len(result) == 2:
            _, sutra_ids_found = result
            # Check if the helper returned an empty set, indicating a fetch/parse failure for that page
            if not sutra_ids_found and page_url_processed:
                # Consider if an empty set always means error, or could be a page with no sutras.
                # Assuming for now it implies an error if the URL was valid.
                # page_fetch_errors += 1 # Let's not count empty pages as errors for now.
                logger.debug(
                    f"{log_prefix} No sutra IDs found on page {page_url_processed}."
                )

            # Add found sutras to the collection
            for sutra_id in sutra_ids_found:
                if sutra_id not in all_discovered_sutras:
                    try:
                        # Validate the source URL before creating the model
                        # Using string for now based on simplification plan
                        validated_page_url_str = str(page_url_processed)
                        # Create SutraCreate model (using str for URL)
                        sutra_data = SutraCreate(
                            sutra_id=sutra_id,
                            book_id=book_id,
                            # Store URL as string - relax Pydantic validation here
                            source_page_url=validated_page_url_str,
                            fetch_status="pending",  # Initial status
                        )
                        all_discovered_sutras[sutra_id] = sutra_data
                    except (
                        ValidationError
                    ) as e:  # Should be less likely with URL as str
                        logger.error(
                            f"{log_prefix} Pydantic validation error creating SutraCreate model for sutra {sutra_id} from page {page_url_processed}: {e}"
                        )
                    except Exception as e:
                        logger.error(
                            f"{log_prefix} Unexpected error creating SutraCreate model for sutra {sutra_id} from page {page_url_processed}: {e}"
                        )
        else:
            logger.error(
                f"{log_prefix} Unexpected result type from discovery task for page {page_url_processed}: {type(result)}"
            )
            page_fetch_errors += 1

        # Log progress periodically
        if processed_page_count % 20 == 0 or processed_page_count == len(page_urls):
            logger.info(
                f"{log_prefix} Sutra discovery progress: {processed_page_count}/{len(page_urls)} pages checked."
            )

    total_sutras_found = len(all_discovered_sutras)
    logger.info(
        f"{log_prefix} Sutra discovery phase finished. Found {total_sutras_found} unique sutras. Page fetch/parse errors: {page_fetch_errors}."
    )

    # --- Database Update and Enqueue Next Task ---
    final_discovery_status = "unknown"
    enqueue_next = False

    if total_sutras_found > 0:
        bulk_ops = []
        for sutra_id, sutra in all_discovered_sutras.items():
            filter_query = {"_id": {"book_id": book_id, "sutra_id": sutra_id}}
            # Use model_dump, ensuring URL is string
            update_data = sutra.model_dump(exclude_unset=True, mode="json")
            now = datetime.now()
            # Prepare the update operation with $setOnInsert
            update_doc = {
                "$currentDate": {"updated_at": True},
                "$setOnInsert": {**update_data, "created_at": now},
            }
            # Remove internal fields from $setOnInsert if they exist
            update_doc["$setOnInsert"].pop("_id", None)
            update_doc["$setOnInsert"].pop("id", None)

            op = UpdateOne(filter_query, update_doc, upsert=True)
            bulk_ops.append(op)

        if bulk_ops:
            logger.info(
                f"{log_prefix} Upserting {len(bulk_ops)} sutra placeholders into DB..."
            )
            try:
                result = await sutras_coll.bulk_write(bulk_ops, ordered=False)
                logger.success(
                    f"{log_prefix} Sutra placeholder upsert result: Upserted={result.upserted_count}, Matched={result.matched_count}, Modified={result.modified_count}"
                )
                final_discovery_status = "complete"
                enqueue_next = True  # Enqueue content fetch if discovery successful
            except Exception as e:
                logger.exception(
                    f"{log_prefix} Critical error during sutra bulk write: {e}"
                )
                final_discovery_status = "db_error"
                # Do not enqueue next task if DB write failed
        else:
            # Should not happen if total_sutras_found > 0, but handle defensively
            logger.warning(
                f"{log_prefix} No bulk operations generated despite finding sutras."
            )
            final_discovery_status = "error"  # Internal logic error

    elif page_fetch_errors == 0:
        # No sutras found, and no errors fetching pages
        logger.info(f"{log_prefix} No sutras were found for this book.")
        final_discovery_status = "no_sutras"
        # Do not enqueue content fetch task if no sutras were found
    else:
        # No sutras found, but there were errors fetching pages
        logger.error(
            f"{log_prefix} Sutra discovery failed due to page fetch/parse errors."
        )
        final_discovery_status = "fetch_errors"
        # Do not enqueue next task if discovery failed

    # --- Final Status Update and Enqueueing ---
    await _update_book_status(
        book_id,
        {
            "sutra_discovery_status": final_discovery_status,
            "total_sutras_discovered": total_sutras_found,
            # Reset fetch counters if discovery is re-run
            "sutras_fetched_count": 0 if final_discovery_status == "complete" else None,
            "sutras_failed_count": 0 if final_discovery_status == "complete" else None,
            # Set content_fetch_status based on discovery outcome
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

    # Enqueue the content fetching task if discovery was successful
    if enqueue_next:
        if arq_redis:
            try:
                await arq_redis.enqueue_job(FETCH_SUTRAS_TASK_NAME, book_id=book_id)
                logger.info(f"{log_prefix} Successfully enqueued content fetch task.")
            except Exception as e:
                logger.error(
                    f"{log_prefix} Failed to enqueue content fetch task '{FETCH_SUTRAS_TASK_NAME}': {e}"
                )
                # Update status to reflect enqueue failure? Maybe add a new status?
                # For now, just log the error. The task won't run automatically.
                await _update_book_status(
                    book_id, {"content_fetch_status": "enqueue_failed"}
                )
        else:
            logger.error(
                f"{log_prefix} Cannot enqueue content fetch task: ARQ Redis unavailable in context."
            )
            await _update_book_status(
                book_id, {"content_fetch_status": "enqueue_failed"}
            )

    return f"{log_prefix} Finished with status: {final_discovery_status}. Sutras found: {total_sutras_found}."


# --- New Task 2: Fetch Content for Sutras of a Book ---


async def fetch_sutras_for_book(ctx, book_id: int) -> str:
    """
    ARQ Task: Fetches the actual content for all sutras associated with a
    given book_id that are marked with a 'pending' (or similar non-final)
    fetch_status. Updates the sutra documents and the overall book status.

    Args:
        ctx: The ARQ worker context.
        book_id: The ID of the book whose sutras need content fetching.

    Returns:
        A status message indicating the outcome.
    """
    log_prefix = f"[FetchContent|Book {book_id}]"
    logger.info(f"{log_prefix} Task started.")

    # --- Dependency Checks ---
    if not settings or not http_client:
        logger.error(f"{log_prefix} Failed: Settings or HTTP client not available.")
        return f"{log_prefix} Failed: Missing dependencies."

    try:
        mongo_db = get_mongo_db()
        books_coll = mongo_db[BOOKS_COLLECTION]
        sutras_coll = mongo_db[SUTRAS_COLLECTION]
    except Exception as e:
        logger.error(f"{log_prefix} Failed to get DB collections: {e}")
        return f"{log_prefix} Failed: DB connection error."

    # --- Find Sutras to Fetch ---
    sutras_to_fetch_ids: List[int] = []
    try:
        # Find sutras for this book that are not in a final state
        # Allows retrying failed/nodata ones if needed, but primarily targets 'pending'
        pending_status_list = [
            "pending",
            "fetch_failed",
            "fetch_bad_response",
            "fetch_nodata",
        ]  # Add statuses to retry if desired
        cursor = sutras_coll.find(
            {"book_id": book_id, "fetch_status": {"$in": pending_status_list}},
            {"_id.sutra_id": 1},  # Project only the sutra_id from the composite _id
        )
        # Extract sutra_id from the composite key {'book_id': b, 'sutra_id': s}
        sutras_to_fetch_ids = [
            doc["_id"]["sutra_id"]
            async for doc in cursor
            if doc.get("_id")
            and isinstance(doc["_id"], dict)
            and "sutra_id" in doc["_id"]
        ]

        if not sutras_to_fetch_ids:
            logger.info(
                f"{log_prefix} No sutras found with pending status for this book."
            )
            # Check if any sutras exist at all for this book to differentiate scenarios
            total_sutras = await sutras_coll.count_documents({"book_id": book_id})
            final_book_status = (
                "complete" if total_sutras > 0 else "skipped"
            )  # Mark skipped if no sutras ever existed
            await _update_book_status(
                book_id, {"content_fetch_status": final_book_status}
            )
            return f"{log_prefix} Completed: No pending sutras to fetch."

    except Exception as e:
        logger.error(f"{log_prefix} Failed to query for pending sutras: {e}")
        # Update book status to reflect the error?
        await _update_book_status(book_id, {"content_fetch_status": "db_error"})
        return f"{log_prefix} Failed: Error querying sutras."

    # --- Content Fetching Phase ---
    num_to_fetch = len(sutras_to_fetch_ids)
    logger.info(f"{log_prefix} Starting content fetching for {num_to_fetch} sutras.")
    await _update_book_status(book_id, {"content_fetch_status": "in_progress"})

    content_fetch_semaphore = asyncio.Semaphore(settings.content_fetch_concurrency)
    content_tasks = [
        _fetch_and_update_sutra_content(
            book_id, s_id, http_client, sutras_coll, content_fetch_semaphore, log_prefix
        )
        for s_id in sutras_to_fetch_ids
    ]

    # Run content fetching tasks concurrently
    fetch_results_list = await asyncio.gather(*content_tasks, return_exceptions=True)

    # Process results and count outcomes
    content_fetch_outcomes: Dict[str, int] = {
        "complete": 0,
        "fetch_failed": 0,
        "fetch_nodata": 0,
        "fetch_bad_response": 0,
        "task_exception": 0,  # Count exceptions from gather
    }
    processed_content_count = 0

    for i, result in enumerate(fetch_results_list):
        sutra_id_processed = sutras_to_fetch_ids[i]
        processed_content_count += 1

        if isinstance(result, Exception):
            logger.error(
                f"{log_prefix} Exception in content fetch task for sutra {sutra_id_processed}: {result}"
            )
            content_fetch_outcomes["task_exception"] += 1
            # We might not know the specific fetch_status if the task itself failed,
            # but we can assume it wasn't 'complete'. Let's count it towards failed.
            content_fetch_outcomes["fetch_failed"] += 1
        elif isinstance(result, tuple) and len(result) == 2:
            _, final_status = result
            if final_status in content_fetch_outcomes:
                content_fetch_outcomes[final_status] += 1
            else:
                # Handle unexpected status strings
                logger.warning(
                    f"{log_prefix} Received unknown status '{final_status}' from fetch helper for sutra {sutra_id_processed}"
                )
                content_fetch_outcomes[
                    "fetch_failed"
                ] += 1  # Count unknowns as failures
        else:
            logger.error(
                f"{log_prefix} Unexpected result type from content fetch task for sutra {sutra_id_processed}: {type(result)}"
            )
            content_fetch_outcomes["task_exception"] += 1
            content_fetch_outcomes["fetch_failed"] += 1

        # Log progress periodically
        if processed_content_count % 50 == 0 or processed_content_count == num_to_fetch:
            logger.info(
                f"{log_prefix} Content fetch progress: {processed_content_count}/{num_to_fetch} sutras processed."
            )

    logger.info(
        f"{log_prefix} Content fetching phase finished. Outcomes: {content_fetch_outcomes}"
    )

    # --- Determine Final Book Status ---
    final_content_status = "unknown"
    total_processed = (
        content_fetch_outcomes["complete"]
        + content_fetch_outcomes["fetch_failed"]
        + content_fetch_outcomes["fetch_nodata"]
        + content_fetch_outcomes["fetch_bad_response"]
    )
    total_succeeded = content_fetch_outcomes["complete"]
    total_failed = (
        content_fetch_outcomes["fetch_failed"]
        + content_fetch_outcomes["task_exception"]
    )  # Include task exceptions in failure count

    if total_succeeded == num_to_fetch:
        final_content_status = "complete"
    elif total_succeeded > 0 and total_failed > 0:
        final_content_status = "partial_failure"
    elif total_succeeded > 0 and total_failed == 0:
        # Succeeded > 0, no hard failures, but maybe nodata/bad_response
        final_content_status = "complete"  # Consider 'complete' if at least one succeeded and none failed outright
    elif total_succeeded == 0 and total_failed > 0:
        final_content_status = "failed"
    elif (
        total_succeeded == 0
        and total_failed == 0
        and (
            content_fetch_outcomes["fetch_nodata"] > 0
            or content_fetch_outcomes["fetch_bad_response"] > 0
        )
    ):
        # No successes, no failures, just nodata or bad responses
        final_content_status = "no_content_found"
    elif total_succeeded == 0 and total_failed == 0 and num_to_fetch > 0:
        # This case should ideally not happen if num_to_fetch > 0
        logger.warning(
            f"{log_prefix} Inconsistent state: Processed {total_processed} sutras, but counts don't match num_to_fetch {num_to_fetch}. Outcomes: {content_fetch_outcomes}"
        )
        final_content_status = "error"  # Mark as error state
    else:  # num_to_fetch was 0, handled earlier
        final_content_status = "skipped"

    # --- Final Book Status Update ---
    await _update_book_status(
        book_id,
        {
            "content_fetch_status": final_content_status,
            # Use the counts derived from outcomes
            "sutras_fetched_count": total_succeeded,
            "sutras_failed_count": total_failed,
        },
    )

    return f"{log_prefix} Finished with status: {final_content_status}. Successful: {total_succeeded}, Failed: {total_failed}, NoData/BadResp: {content_fetch_outcomes['fetch_nodata'] + content_fetch_outcomes['fetch_bad_response']}"
