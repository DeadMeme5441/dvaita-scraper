# src/routes/scraper_routes.py

import asyncio # Import asyncio for sleep
from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from loguru import logger
from typing import List, Optional

from src.arq_pool import get_arq_pool_instance
from arq.connections import ArqRedis
from src.db.mongo_client import get_mongo_db
from motor.motor_asyncio import AsyncIOMotorDatabase

# --- Task Names ---
INITIAL_DISCOVERY_TASK_NAME = "discover_initial_books" # Phase 1
FETCH_BOOK_DETAILS_TASK_NAME = "fetch_book_details" # Phase 1
# Phase 2 Entry Point Task
DISCOVER_SUTRAS_TASK_NAME = "discover_book_sutras"
# Phase 2 Content Fetch Task (usually triggered internally by DISCOVER_SUTRAS_TASK_NAME)
FETCH_SUTRAS_TASK_NAME = "fetch_sutras_for_book"

router = APIRouter()

# --- Dependency for ARQ Pool ---
async def get_arq_pool() -> ArqRedis:
    """FastAPI dependency to get the ARQ Redis pool for enqueueing jobs."""
    pool = get_arq_pool_instance()
    if pool is None:
        logger.error("ARQ Pool dependency requested but pool is not initialized.")
        raise HTTPException(status_code=503, detail="Background task queue is unavailable.")
    return pool

# --- Helper Function to Get Book IDs for Phase 2 ---
async def _get_book_ids_for_phase2(db: AsyncIOMotorDatabase, work_id: Optional[int] = None) -> List[int]:
    """
    Retrieves book IDs ready for Phase 2 processing (sutra discovery).
    Looks for books where Phase 1 (sidebar fetch) succeeded but Phase 2 hasn't started.
    """
    try:
        books_coll = db["books"]
        query = {
            "sidebar_status": {"$in": ["complete", "parse_failed"]}, # Sidebar fetch attempted (even if parse failed, might have source URL)
            "sutra_discovery_status": {"$in": ["pending", "fetch_errors", "db_error", "enqueue_failed", "error"]} # Needs (re)trying
        }
        if work_id is not None:
            query["_id"] = work_id # Filter by specific work_id if provided

        cursor = books_coll.find(query, {"_id": 1}) # Project only the ID
        book_ids = [doc["_id"] async for doc in cursor if doc.get("_id")]
        count_str = f"book {work_id}" if work_id is not None else f"{len(book_ids)} books"
        logger.info(f"Found {count_str} ready for Phase 2 processing.")
        return book_ids
    except Exception as e:
        logger.error(f"Failed to get book IDs for Phase 2 (work_id={work_id}): {e}")
        return []

# --- API Endpoints ---

# Phase 1 Endpoints (Unchanged)
@router.post("/discover", status_code=202, summary="Trigger Initial Book Discovery (Phase 1)")
async def trigger_book_discovery(
    redis: ArqRedis = Depends(get_arq_pool)
) -> dict:
    """Enqueues the task to discover initial book information from the homepage."""
    logger.info("API: Received request to trigger initial book discovery.")
    try:
        job = await redis.enqueue_job(INITIAL_DISCOVERY_TASK_NAME)
        logger.info(f"API: Enqueued '{INITIAL_DISCOVERY_TASK_NAME}'. Job ID: {job.job_id}")
        return {"message": "Initial book discovery task enqueued.", "job_id": job.job_id}
    except Exception as e:
        logger.error(f"API: Failed to enqueue discovery task: {e}")
        raise HTTPException(status_code=500, detail="Failed to enqueue background task.")

# Phase 2 Endpoints

@router.post("/process/phase2/book/{book_id}", status_code=202, summary="Trigger Sutra Discovery for Single Book (Phase 2)")
async def trigger_phase2_single_book(
    book_id: int,
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db) # Check status before enqueueing
) -> dict:
    """
    Checks if a specific book is ready and enqueues the task to discover its sutras (Phase 2 start).
    """
    logger.info(f"API: Received request to start Phase 2 for book ID: {book_id}")
    # Check if the book is actually ready for Phase 2
    ready_ids = await _get_book_ids_for_phase2(db, work_id=book_id)
    if book_id not in ready_ids:
         logger.warning(f"API: Book {book_id} not found or not ready for Phase 2 processing.")
         raise HTTPException(status_code=404, detail=f"Book {book_id} not found or not ready for Phase 2 (check sidebar_status and sutra_discovery_status).")

    try:
        # Enqueue the Phase 2 entry point task
        job = await redis.enqueue_job(DISCOVER_SUTRAS_TASK_NAME, book_id=book_id)
        logger.info(f"API: Enqueued '{DISCOVER_SUTRAS_TASK_NAME}' task for book {book_id}. Job ID: {job.job_id}")
        return {"message": f"Phase 2 task ({DISCOVER_SUTRAS_TASK_NAME}) for book_id {book_id} enqueued.", "job_id": job.job_id}
    except Exception as e:
        logger.error(f"API: Failed to enqueue Phase 2 task for book {book_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to enqueue background task.")


@router.post("/process/phase2/all", status_code=202, summary="Trigger Sutra Discovery for All Ready Books (Phase 2)")
async def trigger_phase2_all_books(
    background_tasks: BackgroundTasks, # Use BackgroundTasks for DB query/loop
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db) # Dependency for DB access
) -> dict:
    """
    Finds all books ready for Phase 2 (sutra discovery) and enqueues the
    `discover_book_sutras` task for each in the background.
    """
    logger.info(f"API: Received request to start Phase 2 for all ready books.")

    async def enqueue_phase2_tasks():
        book_ids = await _get_book_ids_for_phase2(db)
        if not book_ids:
            logger.info(f"API BG: No books found ready for Phase 2 processing.")
            return

        enqueued_count = 0
        enqueue_errors = 0
        job_ids = []

        logger.info(f"API BG: Enqueueing '{DISCOVER_SUTRAS_TASK_NAME}' for {len(book_ids)} books...")
        for book_id in book_ids:
            try:
                job = await redis.enqueue_job(DISCOVER_SUTRAS_TASK_NAME, book_id=book_id)
                job_ids.append(job.job_id)
                enqueued_count += 1
            except Exception as e:
                logger.error(f"API BG: Failed to enqueue Phase 2 task for book {book_id}: {e}")
                enqueue_errors += 1
            # Small sleep to avoid overwhelming Redis/network
            if enqueued_count % 100 == 0: await asyncio.sleep(0.1)

        logger.info(f"API BG: Finished Phase 2 enqueueing. Success: {enqueued_count}, Errors: {enqueue_errors}.")

    # Add the potentially long-running enqueueing logic as a background task
    background_tasks.add_task(enqueue_phase2_tasks)

    return {"message": "Phase 2 processing task initiation started in background for all ready books. Check worker logs.", "tasks_being_enqueued_in_background": True}
