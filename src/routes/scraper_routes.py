# src/routes/scraper_routes.py

from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from loguru import logger
from typing import List, Optional

import asyncio
from src.arq_pool import (
    get_arq_pool_instance,
)  # Use the pool managed by FastAPI lifespan
from arq.connections import ArqRedis
from src.db.mongo_client import get_mongo_db
from motor.motor_asyncio import AsyncIOMotorDatabase

# --- Use correct NEW task function names (as strings) ---
INITIAL_DISCOVERY_TASK_NAME = "discover_initial_books"  # Task defined in discovery.py
# Renamed for clarity: This is the entry point for processing a book after details are fetched
PROCESS_ENTRY_TASK_NAME = "discover_book_sutras"  # Task defined in processing.py

router = APIRouter()


# --- Dependency for ARQ Pool ---
async def get_arq_pool() -> ArqRedis:
    """FastAPI dependency to get the ARQ Redis pool for enqueueing jobs."""
    pool = get_arq_pool_instance()
    if pool is None:
        logger.error("ARQ Pool dependency requested but pool is not initialized.")
        raise HTTPException(
            status_code=503, detail="Background task queue is unavailable."
        )
    return pool


# --- Helper Function to Get Book IDs ---
async def _get_book_ids_for_processing(
    db: AsyncIOMotorDatabase, status_filter: Optional[str]
) -> List[int]:
    """
    Retrieves book IDs from MongoDB based on a status filter.
    Focuses on finding books needing the *start* of the processing pipeline.
    """
    try:
        books_coll = db["books"]
        query = {}
        # Default: Find books where details are fetched but sutra discovery hasn't started or failed previously
        if status_filter == "pending_processing" or not status_filter:
            query = {
                "sidebar_status": {
                    "$ne": "fetch_failed"
                },  # Ensure details were likely fetched
                "sutra_discovery_status": {
                    "$in": ["pending", "fetch_errors", "db_error", "enqueue_failed"]
                },  # Statuses indicating discovery needs (re)trying
            }
        elif (
            status_filter == "all"
        ):  # Process all books regardless of status (use with caution)
            query = {}
        # Add more specific filters if needed
        # elif status_filter == "failed_discovery": query = {"sutra_discovery_status": {"$in": ["fetch_errors", "db_error"]}}
        # elif status_filter == "failed_content": query = {"content_fetch_status": {"$in": ["failed", "partial_failure", "db_error", "enqueue_failed"]}} # Requires enqueueing fetch_sutras_for_book directly

        cursor = books_coll.find(query, {"_id": 1})  # Project only the ID
        book_ids = [doc["_id"] async for doc in cursor if doc.get("_id")]
        logger.info(
            f"Found {len(book_ids)} book IDs matching filter '{status_filter}'."
        )
        return book_ids
    except Exception as e:
        logger.error(f"Failed to get book IDs with filter '{status_filter}': {e}")
        return []


# --- API Endpoints ---


@router.post("/discover", status_code=202, summary="Trigger Initial Book Discovery")
async def trigger_book_discovery(redis: ArqRedis = Depends(get_arq_pool)) -> dict:
    """
    Enqueues the task to discover initial book information from the website's homepage.
    This is the starting point for populating the 'books' collection.
    """
    logger.info("API: Received request to trigger initial book discovery.")
    try:
        # Enqueue the first discovery task by its name
        job = await redis.enqueue_job(INITIAL_DISCOVERY_TASK_NAME)
        logger.info(f"API: Enqueued initial book discovery task. Job ID: {job.job_id}")
        return {
            "message": "Initial book discovery task enqueued.",
            "job_id": job.job_id,
        }
    except Exception as e:
        logger.error(f"API: Failed to enqueue discovery task: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to enqueue background task."
        )


@router.post(
    "/process/book/{book_id}",
    status_code=202,
    summary="Trigger Processing for Single Book",
)
async def trigger_process_single_book(
    book_id: int, redis: ArqRedis = Depends(get_arq_pool)
) -> dict:
    """
    Enqueues the task to start processing a specific book
    (discover its sutras, then fetch content).
    Assumes book details have already been fetched by `/discover`.
    """
    logger.info(f"API: Received request to process book with ID: {book_id}")
    try:
        # Enqueue the *new* entry point processing task
        job = await redis.enqueue_job(PROCESS_ENTRY_TASK_NAME, book_id=book_id)
        logger.info(
            f"API: Enqueued '{PROCESS_ENTRY_TASK_NAME}' task for book {book_id}. Job ID: {job.job_id}"
        )
        return {
            "message": f"Book processing task ({PROCESS_ENTRY_TASK_NAME}) for book_id {book_id} enqueued.",
            "job_id": job.job_id,
        }
    except Exception as e:
        logger.error(f"API: Failed to enqueue processing task for book {book_id}: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to enqueue background task."
        )


@router.post(
    "/process/all", status_code=202, summary="Trigger Processing for Multiple Books"
)
async def trigger_process_all_books(
    background_tasks: BackgroundTasks,  # Use BackgroundTasks for DB query
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),  # Dependency for DB access
    status_filter: Optional[str] = Query(
        "pending_processing",
        description="Filter books to process (e.g., 'pending_processing', 'all')",
    ),
) -> dict:
    """
    Finds books matching the filter criteria in the database and enqueues
    the processing task (`discover_book_sutras`) for each.
    The database query runs in the background to avoid blocking the API response.
    """
    logger.info(
        f"API: Received request to process all books (filter: {status_filter})."
    )

    async def enqueue_processing_tasks():
        book_ids = await _get_book_ids_for_processing(db, status_filter=status_filter)
        if not book_ids:
            logger.info(
                f"API BG: No books found matching filter '{status_filter}' for bulk processing."
            )
            return  # No tasks to enqueue

        enqueued_count = 0
        enqueue_errors = 0
        job_ids = []  # Optional: Collect job IDs if needed

        logger.info(
            f"API BG: Enqueueing '{PROCESS_ENTRY_TASK_NAME}' for {len(book_ids)} books..."
        )
        for book_id in book_ids:
            try:
                job = await redis.enqueue_job(PROCESS_ENTRY_TASK_NAME, book_id=book_id)
                job_ids.append(job.job_id)  # Store job ID
                enqueued_count += 1
            except Exception as e:
                logger.error(f"API BG: Failed to enqueue task for book {book_id}: {e}")
                enqueue_errors += 1
            # Add a small sleep to avoid overwhelming Redis/network if enqueueing thousands
            if enqueued_count % 100 == 0:
                await asyncio.sleep(0.1)

        logger.info(
            f"API BG: Finished enqueueing. Success: {enqueued_count}, Errors: {enqueue_errors}."
        )

    # Add the enqueueing logic as a background task
    background_tasks.add_task(enqueue_processing_tasks)

    # Return immediate response to the client
    return {
        "message": f"Bulk processing task initiated for books matching filter '{status_filter}'. Check worker logs for progress.",
        "tasks_being_enqueued_in_background": True,
    }
