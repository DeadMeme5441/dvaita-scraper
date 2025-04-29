# src/routes/scraper_routes.py

import asyncio
from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from loguru import logger
from typing import List, Optional

from src.arq_pool import get_arq_pool_instance
from arq.connections import ArqRedis
from src.db.mongo_client import get_mongo_db
from motor.motor_asyncio import AsyncIOMotorDatabase

# --- Task Names ---
INITIAL_DISCOVERY_TASK_NAME = "discover_initial_books"
FETCH_BOOK_DETAILS_TASK_NAME = "fetch_book_details"
DISCOVER_SUTRAS_TASK_NAME = "discover_book_sutras"
FETCH_SUTRAS_TASK_NAME = "fetch_sutras_for_book"
# --- Phase 3 Task Names ---
PREPROCESS_HTML_TASK_NAME = "preprocess_book_html"  # Phase 3a
GENERATE_MARKDOWN_TASK_NAME = "generate_markdown_from_preprocessed"  # Phase 3b

router = APIRouter()


# --- Dependencies ---
async def get_arq_pool() -> ArqRedis:
    pool = get_arq_pool_instance()
    if pool is None:
        raise HTTPException(status_code=503, detail="BG queue unavailable.")
    return pool


# --- Helper Functions ---
async def _get_book_ids_for_phase2(
    db: AsyncIOMotorDatabase, work_id: Optional[int] = None
) -> List[int]:
    try:
        books_coll = db["books"]
        query = {
            "sidebar_status": {"$in": ["complete", "parse_failed"]},
            "sutra_discovery_status": {
                "$in": [
                    "pending",
                    "fetch_errors",
                    "db_error",
                    "enqueue_failed",
                    "error",
                ]
            },
        }
        if work_id is not None:
            query["_id"] = work_id
        cursor = books_coll.find(query, {"_id": 1})
        book_ids = [doc["_id"] async for doc in cursor if doc.get("_id")]
        return book_ids
    except Exception as e:
        logger.error(f"Failed to get book IDs for Phase 2 (work_id={work_id}): {e}")
        return []


async def _get_book_ids_for_phase3a(
    db: AsyncIOMotorDatabase, work_id: Optional[int] = None
) -> List[int]:
    """Retrieves book IDs ready for Phase 3a processing (HTML Preprocessing)."""
    try:
        books_coll = db["books"]
        query = {
            "content_fetch_status": {
                "$in": ["complete", "no_content_found", "partial_failure"]
            },
            # "phase3_status": {
            #     "$nin": [
            #         "preprocessing",
            #         "preprocessing_complete",
            #         "generating_markdown",
            #         "complete",
            #         "skipped_no_sutras",
            #         "skipped_no_preprocessed_sutras",
            #     ]
            # },
        }
        if work_id is not None:
            query["_id"] = work_id
        cursor = books_coll.find(query, {"_id": 1})
        book_ids = [doc["_id"] async for doc in cursor if doc.get("_id")]
        return book_ids
    except Exception as e:
        logger.error(f"Failed to get book IDs for Phase 3a (work_id={work_id}): {e}")
        return []


# --- API Endpoints ---


# Phase 1 Endpoints (Unchanged)
@router.post(
    "/discover", status_code=202, summary="Trigger Initial Book Discovery (Phase 1)"
)
async def trigger_book_discovery(redis: ArqRedis = Depends(get_arq_pool)) -> dict:
    logger.info("API: Received request to trigger initial book discovery.")
    try:
        job = await redis.enqueue_job(INITIAL_DISCOVERY_TASK_NAME)
        return {
            "message": "Initial book discovery task enqueued.",
            "job_id": job.job_id,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail="Failed to enqueue background task."
        )


# Phase 2 Endpoints (Unchanged)
@router.post(
    "/process/phase2/book/{book_id}",
    status_code=202,
    summary="Trigger Sutra Discovery for Single Book (Phase 2)",
)
async def trigger_phase2_single_book(
    book_id: int,
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
) -> dict:
    logger.info(f"API: Received request to start Phase 2 for book ID: {book_id}")
    ready_ids = await _get_book_ids_for_phase2(db, work_id=book_id)
    if book_id not in ready_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Book {book_id} not found or not ready for Phase 2.",
        )
    try:
        job = await redis.enqueue_job(DISCOVER_SUTRAS_TASK_NAME, book_id=book_id)
        return {
            "message": f"Phase 2 task ({DISCOVER_SUTRAS_TASK_NAME}) for book_id {book_id} enqueued.",
            "job_id": job.job_id,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail="Failed to enqueue background task."
        )


@router.post(
    "/process/phase2/all",
    status_code=202,
    summary="Trigger Sutra Discovery for All Ready Books (Phase 2)",
)
async def trigger_phase2_all_books(
    background_tasks: BackgroundTasks,
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
) -> dict:
    logger.info(f"API: Received request to start Phase 2 for all ready books.")

    async def enqueue_phase2_tasks():
        book_ids = await _get_book_ids_for_phase2(db)
        if not book_ids:
            logger.info(f"API BG: No books found ready for Phase 2 processing.")
            return
        enqueued_count = 0
        enqueue_errors = 0
        logger.info(
            f"API BG: Enqueueing '{DISCOVER_SUTRAS_TASK_NAME}' for {len(book_ids)} books..."
        )
        for book_id in book_ids:
            try:
                await redis.enqueue_job(DISCOVER_SUTRAS_TASK_NAME, book_id=book_id)
                enqueued_count += 1
            except Exception as e:
                logger.error(
                    f"API BG: Failed to enqueue Phase 2 task for book {book_id}: {e}"
                )
                enqueue_errors += 1
            if enqueued_count % 100 == 0:
                await asyncio.sleep(0.1)
        logger.info(
            f"API BG: Finished Phase 2 enqueueing. Success: {enqueued_count}, Errors: {enqueue_errors}."
        )

    background_tasks.add_task(enqueue_phase2_tasks)
    return {
        "message": "Phase 2 processing task initiation started in background.",
        "tasks_being_enqueued_in_background": True,
    }


# --- Phase 3 Endpoints ---


@router.post(
    "/process/phase3a/preprocess/book/{book_id}",
    status_code=202,
    summary="Trigger HTML Preprocessing for Single Book (Phase 3a)",
)
async def trigger_phase3a_single_book(
    book_id: int,
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
) -> dict:
    """Checks if a book is ready and enqueues the HTML preprocessing task (Phase 3a)."""
    logger.info(
        f"API: Received request to start Phase 3a (Preprocess) for book ID: {book_id}"
    )
    ready_ids = await _get_book_ids_for_phase3a(db, work_id=book_id)
    if book_id not in ready_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Book {book_id} not found or not ready for Phase 3a.",
        )
    try:
        job = await redis.enqueue_job(
            PREPROCESS_HTML_TASK_NAME, book_id=book_id
        )  # Enqueue Phase 3a task
        logger.info(
            f"API: Enqueued '{PREPROCESS_HTML_TASK_NAME}' task for book {book_id}. Job ID: {job.job_id}"
        )
        return {
            "message": f"Phase 3a task ({PREPROCESS_HTML_TASK_NAME}) for book_id {book_id} enqueued.",
            "job_id": job.job_id,
        }
    except Exception as e:
        logger.error(f"API: Failed to enqueue Phase 3a task for book {book_id}: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to enqueue background task."
        )


@router.post(
    "/process/phase3a/preprocess/all",
    status_code=202,
    summary="Trigger HTML Preprocessing for All Ready Books (Phase 3a)",
)
async def trigger_phase3a_all_books(
    background_tasks: BackgroundTasks,
    redis: ArqRedis = Depends(get_arq_pool),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
) -> dict:
    """Finds all books ready for Phase 3a (HTML Preprocessing) and enqueues the task."""
    logger.info(
        f"API: Received request to start Phase 3a (Preprocess) for all ready books."
    )

    async def enqueue_phase3a_tasks():
        book_ids = await _get_book_ids_for_phase3a(db)
        if not book_ids:
            logger.info(f"API BG: No books found ready for Phase 3a processing.")
            return
        enqueued_count = 0
        enqueue_errors = 0
        logger.info(
            f"API BG: Enqueueing '{PREPROCESS_HTML_TASK_NAME}' for {len(book_ids)} books..."
        )
        for book_id in book_ids:
            try:
                await redis.enqueue_job(PREPROCESS_HTML_TASK_NAME, book_id=book_id)
                enqueued_count += 1
            except Exception as e:
                logger.error(
                    f"API BG: Failed to enqueue Phase 3a task for book {book_id}: {e}"
                )
                enqueue_errors += 1
            if enqueued_count % 100 == 0:
                await asyncio.sleep(0.1)
        logger.info(
            f"API BG: Finished Phase 3a enqueueing. Success: {enqueued_count}, Errors: {enqueue_errors}."
        )

    background_tasks.add_task(enqueue_phase3a_tasks)
    return {
        "message": "Phase 3a (HTML Preprocessing) initiation started in background.",
        "tasks_being_enqueued_in_background": True,
    }


# --- Endpoint to trigger Phase 3b (Markdown Generation) ---
# This is now triggered automatically by the preprocess_book_html task.
# If you want a manual trigger instead, uncomment and adapt this.
# @router.post("/process/phase3b/markdown/book/{book_id}", status_code=202, summary="Trigger Markdown Generation for Single Book (Phase 3b)")
# async def trigger_phase3b_single_book(book_id: int, redis: ArqRedis = Depends(get_arq_pool), db: AsyncIOMotorDatabase = Depends(get_mongo_db)) -> dict:
#     """Checks if a book has completed preprocessing and enqueues Markdown generation."""
#     logger.info(f"API: Received request to start Phase 3b (Markdown) for book ID: {book_id}")
#     # Add logic here to check if book status is 'preprocessing_complete' or 'preprocessing_errors'
#     # book = await db["books"].find_one({"_id": book_id, "phase3_status": {"$in": ["preprocessing_complete", "preprocessing_errors"]}})
#     # if not book: raise HTTPException(status_code=404, detail=f"Book {book_id} not found or not ready for Phase 3b.")
#     try:
#         job = await redis.enqueue_job(GENERATE_MARKDOWN_TASK_NAME, book_id=book_id)
#         logger.info(f"API: Enqueued '{GENERATE_MARKDOWN_TASK_NAME}' task for book {book_id}. Job ID: {job.job_id}")
#         return {"message": f"Phase 3b task ({GENERATE_MARKDOWN_TASK_NAME}) for book_id {book_id} enqueued.", "job_id": job.job_id}
#     except Exception as e: logger.error(f"API: Failed to enqueue Phase 3b task for book {book_id}: {e}"); raise HTTPException(status_code=500, detail="Failed to enqueue background task.")
