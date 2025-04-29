# migrate_data.py
import asyncio
import os
import sys
import time  # Import time for timing
from datetime import datetime, timezone
from typing import Dict, List, Set, Optional, Any, Tuple

import motor.motor_asyncio
from dotenv import load_dotenv
from loguru import logger
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError, ConnectionFailure

# --- Configuration ---
BATCH_SIZE = 500  # Adjust as needed

# Old DB Collection Names
OLD_WORKS_COLLECTION = "works"
OLD_SIDEBAR_COLLECTION = "works_sidebar"
OLD_PAGES_COLLECTION = "pages"

# New DB Collection Names
NEW_BOOKS_COLLECTION = "books"
NEW_SUTRAS_COLLECTION = "sutras"

# --- Logging Setup ---
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    colorize=True,
)


# --- Database Connection ---
async def connect_db(
    uri: str, db_name_desc: str
) -> Optional[motor.motor_asyncio.AsyncIOMotorDatabase]:
    """Connects to MongoDB and returns the database object."""
    if not uri:
        logger.critical(f"MongoDB URI for {db_name_desc} is not provided.")
        return None
    try:
        logger.info(
            f"Attempting to connect to {db_name_desc} MongoDB at {uri.split('@')[-1].split('/')[0]}..."
        )  # Obfuscate credentials
        client = motor.motor_asyncio.AsyncIOMotorClient(
            uri, serverSelectionTimeoutMS=5000, appName="migrationScript"
        )
        await client.admin.command("ismaster")  # Verify connection
        db = client.get_database()

        # Check if db object is None correctly
        if db is None:
            logger.critical(
                f"Could not determine database for {db_name_desc} from URI: {uri}"
            )
            return None

        logger.success(f"Successfully connected to {db_name_desc} DB: {db.name}")
        return db
    except ConnectionFailure as e:
        logger.critical(f"Failed to connect to {db_name_desc} MongoDB: {e}")
        return None
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred connecting to {db_name_desc} DB: {e}"
        )
        return None


# --- Migration Functions ---


async def migrate_books(
    old_db: motor.motor_asyncio.AsyncIOMotorDatabase,
    new_db: motor.motor_asyncio.AsyncIOMotorDatabase,
) -> None:
    """Migrates data from old 'works' and 'works_sidebar' to new 'books'."""
    logger.info("Starting migration of books and sidebars...")
    if old_db is None or new_db is None:
        logger.error("Database connection not established. Cannot migrate books.")
        return

    old_works_coll = old_db[OLD_WORKS_COLLECTION]
    old_sidebar_coll = old_db[OLD_SIDEBAR_COLLECTION]
    new_books_coll = new_db[NEW_BOOKS_COLLECTION]

    try:
        # 1. Load old works data
        logger.info(f"Loading data from old '{OLD_WORKS_COLLECTION}' collection...")
        all_works_docs = await old_works_coll.find({}).to_list(length=None)
        if not all_works_docs:
            logger.warning(
                f"No documents found in old '{OLD_WORKS_COLLECTION}'. Skipping book migration."
            )
            return
        logger.info(
            f"Loaded {len(all_works_docs)} documents from '{OLD_WORKS_COLLECTION}'."
        )

        book_id_to_work_map: Dict[int, Dict] = {}
        for work_doc in all_works_docs:
            book_id = work_doc.get("book_id")
            if isinstance(book_id, int) and book_id is not None:
                book_id_to_work_map[book_id] = work_doc
            else:
                logger.warning(
                    f"Skipping work document with missing or invalid 'book_id': {work_doc.get('_id')}"
                )

        # 2. Load old sidebar data
        logger.info(f"Loading data from old '{OLD_SIDEBAR_COLLECTION}' collection...")
        all_sidebar_docs = await old_sidebar_coll.find({}).to_list(length=None)
        book_id_to_sidebar_map: Dict[int, str] = {
            doc.get("book_id"): doc.get("sidebar")
            for doc in all_sidebar_docs
            if isinstance(doc.get("book_id"), int)
            and doc.get("book_id") is not None
            and doc.get("sidebar")
        }
        logger.info(
            f"Loaded {len(book_id_to_sidebar_map)} sidebars with valid book_id."
        )

        # 3. Prepare bulk operations
        book_ops: List[ReplaceOne] = []
        processed_count = 0
        skipped_count = 0

        logger.info("Processing and preparing documents for new 'books' collection...")
        for book_id, work_doc in book_id_to_work_map.items():
            processed_count += 1
            sidebar_html = book_id_to_sidebar_map.get(book_id)
            source_url = work_doc.get("source_url")
            page_urls = work_doc.get("pages", [])

            if not source_url or not isinstance(page_urls, list):
                logger.warning(
                    f"Skipping book_id {book_id} due to missing source_url or invalid pages field."
                )
                skipped_count += 1
                continue

            sidebar_status = "complete" if sidebar_html else "parse_failed"
            created_ts_raw = work_doc.get("discovered_at")
            if isinstance(created_ts_raw, datetime):
                created_ts = created_ts_raw
            else:
                created_ts = datetime.now(timezone.utc)
                logger.warning(
                    f"Missing or invalid 'discovered_at' for book {book_id}. Using current time."
                )
            if created_ts.tzinfo is None:
                created_ts = created_ts.replace(tzinfo=timezone.utc)

            new_book_document = {
                "_id": book_id,
                "title_sa": work_doc.get("title_sa"),
                "title_en": work_doc.get("title_en"),
                "section": work_doc.get("section"),
                "order_in_section": work_doc.get("order"),
                "source_url": source_url,
                "page_urls": page_urls,
                "sidebar_html": sidebar_html,
                "discovery_status": "complete",
                "sidebar_status": sidebar_status,
                "sutra_discovery_status": "complete",
                "content_fetch_status": "complete",
                "phase3_status": "pending",
                "sutra_discovery_progress": 100.0,
                "content_fetch_progress": 100.0,
                "total_sutras_discovered": 0,
                "sutras_fetched_count": 0,
                "sutras_failed_count": 0,
                "created_at": created_ts,
                "updated_at": datetime.now(timezone.utc),
            }
            op = ReplaceOne({"_id": book_id}, new_book_document, upsert=True)
            book_ops.append(op)

            if len(book_ops) >= BATCH_SIZE:
                logger.info(f"Writing batch of {len(book_ops)} book documents...")
                try:
                    await new_books_coll.bulk_write(book_ops, ordered=False)
                except BulkWriteError as bwe:
                    logger.error(
                        f"Bulk write error during book migration: {bwe.details}"
                    )
                except Exception as e:
                    logger.error(f"Error during book bulk write: {e}")
                book_ops = []
        if book_ops:
            logger.info(f"Writing final batch of {len(book_ops)} book documents...")
            try:
                await new_books_coll.bulk_write(book_ops, ordered=False)
            except BulkWriteError as bwe:
                logger.error(
                    f"Bulk write error during final book migration batch: {bwe.details}"
                )
            except Exception as e:
                logger.error(f"Error during final book bulk write: {e}")

        logger.success(
            f"Book migration finished. Processed: {processed_count}, Skipped: {skipped_count}."
        )

    except Exception as e:
        logger.exception(f"An error occurred during book migration: {e}")


async def migrate_sutras(
    old_db: motor.motor_asyncio.AsyncIOMotorDatabase,
    new_db: motor.motor_asyncio.AsyncIOMotorDatabase,
) -> Tuple[Set[int], int, int]:
    """
    Migrates data from old 'pages' to new 'sutras'.
    Returns: Set of processed book_ids, count of documents processed from source, count of documents skipped.
    """
    logger.info("Starting migration of sutra pages...")
    if old_db is None or new_db is None:
        logger.error("Database connection not established. Cannot migrate sutras.")
        return set(), 0, 0  # Return empty set and zero counts

    old_pages_coll = old_db[OLD_PAGES_COLLECTION]
    new_sutras_coll = new_db[NEW_SUTRAS_COLLECTION]

    processed_book_ids: Set[int] = set()
    sutra_ops: List[ReplaceOne] = []
    processed_count = 0
    skipped_count = 0
    total_docs_source = 0
    try:
        total_docs_source = await old_pages_coll.count_documents({})  # Get exact count
    except Exception as count_e:
        logger.warning(
            f"Could not count documents for old pages: {count_e}. Progress log may be inaccurate."
        )
        # Try estimated count as fallback
        try:
            total_docs_source = await old_pages_coll.estimated_document_count()
        except Exception:
            pass  # Ignore error in fallback

    logger.info(f"Found {total_docs_source} documents in old '{OLD_PAGES_COLLECTION}'.")

    try:
        async for page_doc in old_pages_coll.find({}):
            processed_count += 1  # Count every doc we iterate over from source
            book_id = page_doc.get("work_id")
            sutra_id = page_doc.get("sutra_id")
            source_url = page_doc.get("source_url")
            raw_html = page_doc.get("raw_html")
            tag_html = page_doc.get("tag_html")
            fetched_at_raw = page_doc.get("fetched_at")

            # --- Stricter validation ---
            valid = True
            if not isinstance(book_id, int) or book_id is None:
                logger.warning(
                    f"Skipping page doc due to invalid/missing 'work_id': {page_doc.get('_id')}, Value: {book_id}"
                )
                valid = False
            if not isinstance(sutra_id, int) or sutra_id is None:
                logger.warning(
                    f"Skipping page doc due to invalid/missing 'sutra_id': {page_doc.get('_id')}, Value: {sutra_id}"
                )
                valid = False
            if not source_url:
                logger.warning(
                    f"Skipping page doc due to missing 'source_url': {page_doc.get('_id')}"
                )
                valid = False
            # Optional: Check if raw_html exists? Depends if you want to migrate placeholders
            # if raw_html is None:
            #    logger.warning(f"Skipping page doc due to missing 'raw_html': {page_doc.get('_id')}")
            #    valid = False

            if not valid:
                skipped_count += 1
                continue  # Skip this document

            if isinstance(fetched_at_raw, datetime):
                fetched_at = fetched_at_raw
            else:
                fetched_at = datetime.now(timezone.utc)
                logger.warning(
                    f"Missing or invalid 'fetched_at' for page {page_doc.get('_id')}. Using current time."
                )
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)

            processed_book_ids.add(book_id)
            composite_id = {"book_id": book_id, "sutra_id": sutra_id}
            new_sutra_document = {
                "_id": composite_id,
                "book_id": book_id,
                "sutra_id": sutra_id,
                "source_page_url": source_url,
                "raw_html": raw_html,
                "tag_html": tag_html,
                "fetch_status": "complete",
                "created_at": fetched_at,
                "updated_at": datetime.now(timezone.utc),
            }
            op = ReplaceOne({"_id": composite_id}, new_sutra_document, upsert=True)
            sutra_ops.append(op)

            if len(sutra_ops) >= BATCH_SIZE:
                if total_docs_source > 0 and processed_count % (BATCH_SIZE * 10) == 0:
                    logger.info(
                        f"Processed {processed_count}/{total_docs_source} source sutra docs..."
                    )
                elif processed_count % (BATCH_SIZE * 10) == 0:
                    logger.info(f"Processed {processed_count} source sutra docs...")
                try:
                    await new_sutras_coll.bulk_write(sutra_ops, ordered=False)
                except BulkWriteError as bwe:
                    logger.error(
                        f"Bulk write error during sutra migration: {bwe.details}"
                    )
                except Exception as e:
                    logger.error(f"Error during sutra bulk write: {e}")
                sutra_ops = []
        if sutra_ops:
            logger.info(f"Writing final batch of {len(sutra_ops)} sutra documents...")
            try:
                await new_sutras_coll.bulk_write(sutra_ops, ordered=False)
            except BulkWriteError as bwe:
                logger.error(
                    f"Bulk write error during final sutra migration batch: {bwe.details}"
                )
            except Exception as e:
                logger.error(f"Error during final sutra bulk write: {e}")

        # --- Log final counts ---
        final_target_count = await new_sutras_coll.count_documents({})
        logger.success(
            f"Sutra migration finished. Source Docs Iterated: {processed_count}, Skipped Validation: {skipped_count}, Target Docs Written: {final_target_count}."
        )
        if (
            total_docs_source > 0
            and (processed_count - skipped_count) != final_target_count
        ):
            logger.warning(
                f"Potential discrepancy due to duplicates in source. Source Valid = {processed_count - skipped_count}, Target = {final_target_count}"
            )

        return processed_book_ids, processed_count, skipped_count

    except Exception as e:
        logger.exception(f"An error occurred during sutra migration: {e}")
        # Return counts processed so far
        return processed_book_ids, processed_count, skipped_count


async def update_book_counts(
    new_db: motor.motor_asyncio.AsyncIOMotorDatabase, book_ids: Set[int]
) -> None:
    """Updates sutra counts in the new 'books' collection."""
    if not book_ids:
        logger.warning("No book IDs provided to update counts.")
        return
    if new_db is None:
        logger.error("Database connection not established. Cannot update book counts.")
        return

    logger.info(f"Updating sutra counts for {len(book_ids)} books...")
    new_books_coll = new_db[NEW_BOOKS_COLLECTION]
    new_sutras_coll = new_db[NEW_SUTRAS_COLLECTION]
    updated_count = 0
    error_count = 0

    for book_id in book_ids:
        try:
            # Count documents in the *new* sutras collection
            sutra_count = await new_sutras_coll.count_documents({"book_id": book_id})
            update_result = await new_books_coll.update_one(
                {"_id": book_id},
                {
                    "$set": {
                        "total_sutras_discovered": sutra_count,
                        "sutras_fetched_count": sutra_count,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            if update_result.modified_count > 0:
                updated_count += 1
            elif update_result.matched_count == 0:
                logger.warning(
                    f"Book ID {book_id} not found in new books collection during count update."
                )
        except Exception as e:
            logger.error(f"Failed to update counts for book_id {book_id}: {e}")
            error_count += 1

    logger.success(
        f"Book count update finished. Updated: {updated_count}, Errors: {error_count}."
    )


# --- Main Execution ---


async def run_migration():
    """Runs the complete data migration process."""
    logger.info("--- Starting Data Migration ---")
    load_dotenv()

    old_mongo_uri = "mongodb://localhost:27017/dvaita"
    new_mongo_uri = "mongodb://localhost:27017/dvaita_scraper"

    if not old_mongo_uri or not new_mongo_uri:
        logger.critical(
            "OLD_MONGO_URI and NEW_MONGO_URI must be set in environment or .env file."
        )
        return

    old_db = None
    new_db = None
    processed_book_ids = set()
    processed_sutra_count = 0
    skipped_sutra_count = 0
    final_sutra_target_count = 0

    try:
        old_db = await connect_db(old_mongo_uri, "OLD")
        new_db = await connect_db(new_mongo_uri, "NEW")

        if old_db is None or new_db is None:
            logger.critical("Database connection failed. Aborting migration.")
            return

        start_time = time.time()

        # Step 1: Migrate Books
        await migrate_books(old_db, new_db)

        # Step 2: Migrate Sutras
        processed_book_ids, processed_sutra_count, skipped_sutra_count = (
            await migrate_sutras(old_db, new_db)
        )

        # Step 3: Update Counts
        if processed_book_ids:
            await update_book_counts(new_db, processed_book_ids)
        else:
            logger.warning(
                "No book IDs were processed during sutra migration, skipping count update."
            )

        # --- Get final count in target collection ---
        if new_db:
            try:
                final_sutra_target_count = await new_db[
                    NEW_SUTRAS_COLLECTION
                ].count_documents({})
            except Exception as e:
                logger.error(
                    f"Could not get final count from target sutra collection: {e}"
                )

        end_time = time.time()
        duration = end_time - start_time
        logger.info("--- Data Migration Summary ---")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Source Sutra Docs Iterated: {processed_sutra_count}")
        logger.info(f"Source Sutra Docs Skipped (Validation): {skipped_sutra_count}")
        logger.info(f"Target Sutra Docs Final Count: {final_sutra_target_count}")
        if (processed_sutra_count - skipped_sutra_count) != final_sutra_target_count:
            logger.warning(
                f"Difference between source valid docs and target count likely due to duplicates in source."
            )
        logger.info("--- Migration Process Finished ---")

    finally:
        # Ensure connections are closed even if errors occur
        # --- FIX: Correct truthiness check ---
        if old_db is not None and old_db.client:
            try:
                old_db.client.close()
                logger.info("Closed connection to OLD DB.")
            except Exception as close_e:
                logger.error(f"Error closing OLD DB connection: {close_e}")
        if new_db is not None and new_db.client:
            try:
                new_db.client.close()
                logger.info("Closed connection to NEW DB.")
            except Exception as close_e:
                logger.error(f"Error closing NEW DB connection: {close_e}")


if __name__ == "__main__":
    try:
        asyncio.run(run_migration())
    except KeyboardInterrupt:
        logger.warning("Migration interrupted by user.")
    except Exception as e:
        logger.exception("An unexpected error occurred during migration.")
