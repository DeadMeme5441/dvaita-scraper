# export_markdown_for_review.py

import asyncio
import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
import yaml  # For writing frontmatter
from datetime import datetime  # Import datetime

import motor.motor_asyncio

# --- REMOVED: dotenv import ---
# from dotenv import load_dotenv
from loguru import logger

# --- REMOVED: slugify import ---
# from slugify import slugify
from pymongo.errors import ConnectionFailure

# --- ADDED: Import settings from src.config ---
try:
    from src.config import settings
except ImportError:
    logger.error(
        "Could not import settings from src.config. Make sure PYTHONPATH is set correctly or run from project root."
    )
    settings = None  # Set to None to handle failure gracefully

# --- Configuration ---
MARKDOWN_PAGES_COLLECTION = "markdown_pages"
BOOKS_COLLECTION = "books"  # Still needed to check if book exists
DEFAULT_OUTPUT_DIR = Path("./review_output_numeric")  # Changed default output dir name

# --- Logging Setup ---
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    colorize=True,
)


# --- Database Connection (Unchanged) ---
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
        )
        client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            serverSelectionTimeoutMS=5000,
            appName="markdownExportScript",  # Identify the script
        )
        await client.admin.command("ismaster")  # Verify connection
        db = client.get_database()  # Gets default DB from URI

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


# --- File Path Generation (MODIFIED) ---
def get_filepath_for_page(
    base_dir: Path,
    nav_path: List[Dict[str, Any]],  # Full nav_path [{level: L, title: T}, ...]
    # leaf_title: str, # No longer needed for path/filename
    sutra_id: int,
    book_id: int,
    # book_slug: str # No longer needed
) -> Path:
    """Creates hierarchical path/filename based on numeric IDs and levels."""
    # Use book_id for the top-level directory
    current_path = base_dir / str(book_id)

    # Create directories for each level using only the level number
    for item in nav_path:
        level = item.get("level", 0)
        # --- Use only level number for directory ---
        dir_name = f"L{level}"
        current_path /= dir_name

    # --- Use only sutra_id (padded) for the filename ---
    filename = f"{sutra_id:05d}.md"

    return current_path / filename


# --- Main Export Function (MODIFIED) ---
async def export_markdown(
    db: motor.motor_asyncio.AsyncIOMotorDatabase,
    book_id_to_export: Optional[int],
    export_all: bool,
    output_dir: Path,
):
    """Exports markdown content from MongoDB to local files using numeric paths."""
    if db is None:
        logger.error("Database connection not available. Aborting export.")
        return

    markdown_coll = db[MARKDOWN_PAGES_COLLECTION]
    books_coll = db[BOOKS_COLLECTION]  # Keep for checking book existence
    book_ids_to_process: List[int] = []

    # Determine which book IDs to process (Unchanged logic)
    if book_id_to_export is not None:
        logger.info(f"Checking if book_id {book_id_to_export} exists...")
        # Check in books collection for existence, markdown_pages might be empty initially
        if await books_coll.find_one({"_id": book_id_to_export}):
            book_ids_to_process.append(book_id_to_export)
            # Check if markdown pages exist for this book
            if not await markdown_coll.find_one({"book_id": book_id_to_export}):
                logger.warning(
                    f"Book ID {book_id_to_export} exists, but no markdown pages found yet in '{MARKDOWN_PAGES_COLLECTION}'. Export might be empty."
                )
        else:
            logger.error(
                f"Book ID {book_id_to_export} not found in '{BOOKS_COLLECTION}'."
            )
            return
    elif export_all:
        logger.info(
            f"Finding all distinct book_ids in '{MARKDOWN_PAGES_COLLECTION}'..."
        )
        distinct_ids = await markdown_coll.distinct("book_id")
        if distinct_ids:
            book_ids_to_process = sorted(
                [int(bid) for bid in distinct_ids if isinstance(bid, (int, float))]
            )  # Ensure int and sort
            logger.info(
                f"Found {len(book_ids_to_process)} book_ids with markdown pages to export."
            )
        else:
            logger.warning(
                f"No documents found in '{MARKDOWN_PAGES_COLLECTION}'. Nothing to export."
            )
            return
    else:
        logger.error("Please specify either --book-id <ID> or --all.")
        return

    total_files_written = 0
    total_errors = 0

    # Process each book ID
    for current_book_id in book_ids_to_process:
        logger.info(f"--- Processing Book ID: {current_book_id} ---")

        # --- No longer need book title/slug for path ---
        # book_doc = await books_coll.find_one({"_id": current_book_id}, {"title_en": 1})
        # book_title_slug = f"work_{current_book_id}"
        # if book_doc and book_doc.get("title_en"):
        #     book_title_slug = slugify(book_doc.get("title_en"), separator="_", max_length=50)
        # else:
        #     logger.warning(f"Could not find title for book_id {current_book_id}.")

        # Iterate through markdown pages for the current book
        page_count = 0
        error_count_book = 0
        try:
            async for md_page_doc in markdown_coll.find({"book_id": current_book_id}):
                page_count += 1
                try:
                    # Extract necessary data
                    sutra_id = md_page_doc.get("sutra_id")
                    nav_path = md_page_doc.get("nav_path", [])
                    # leaf_title = md_page_doc.get("leaf_title", f"sutra_{sutra_id}") # Still needed for frontmatter
                    leaf_title = (
                        md_page_doc.get("leaf_title") or f"sutra_{sutra_id}"
                    )  # Ensure fallback
                    markdown_content = md_page_doc.get("markdown_content", "")

                    if sutra_id is None:
                        logger.warning(
                            f"Skipping document with missing sutra_id: {md_page_doc.get('_id')}"
                        )
                        error_count_book += 1
                        continue

                    # Determine file path using numeric structure
                    filepath = get_filepath_for_page(
                        base_dir=output_dir,
                        nav_path=nav_path,
                        # leaf_title=leaf_title, # Not needed for path
                        sutra_id=sutra_id,
                        book_id=current_book_id,
                        # book_slug=book_title_slug # Not needed
                    )

                    # Prepare frontmatter (extract relevant fields - unchanged)
                    frontmatter = {
                        key: md_page_doc.get(key)
                        for key in [
                            "book_id",
                            "sutra_id",
                            "source_page_url",
                            "nav_path",
                            "leaf_title",
                            "is_orphan",
                            "processing_flags",
                            "source_preprocessed_at",
                            "generated_at",
                        ]
                        if md_page_doc.get(key) is not None
                    }
                    # Ensure timestamps are strings for YAML (unchanged)
                    if isinstance(frontmatter.get("source_preprocessed_at"), datetime):
                        frontmatter["source_preprocessed_at"] = frontmatter[
                            "source_preprocessed_at"
                        ].isoformat()
                    if isinstance(frontmatter.get("generated_at"), datetime):
                        frontmatter["generated_at"] = frontmatter[
                            "generated_at"
                        ].isoformat()

                    try:
                        yaml_fm = yaml.dump(
                            frontmatter,
                            allow_unicode=True,
                            default_flow_style=False,
                            sort_keys=False,
                            width=1000,
                        )
                    except Exception as yaml_e:
                        logger.error(
                            f"Failed to generate YAML frontmatter for sutra {sutra_id} (Book {current_book_id}): {yaml_e}"
                        )
                        yaml_fm = f"# YAML Error: {yaml_e}\n"

                    full_file_content = f"---\n{yaml_fm}---\n\n{markdown_content}"

                    # Create directories and write file (unchanged)
                    filepath.parent.mkdir(parents=True, exist_ok=True)
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(full_file_content)

                    total_files_written += 1

                    if page_count % 100 == 0:
                        logger.info(
                            f"  ... exported {page_count} pages for book {current_book_id}"
                        )

                except Exception as page_err:
                    logger.error(
                        f"Failed to process/write page for sutra_id {md_page_doc.get('sutra_id', 'UNKNOWN')} (Book {current_book_id}): {page_err}"
                    )
                    error_count_book += 1

            logger.success(
                f"Finished processing Book ID: {current_book_id}. Exported: {page_count - error_count_book}, Errors: {error_count_book}"
            )
            total_errors += error_count_book

        except Exception as book_err:
            logger.exception(
                f"An error occurred while processing book_id {current_book_id}: {book_err}"
            )
            total_errors += 1

    logger.info("--- Export Summary ---")
    logger.info(f"Total Books Processed: {len(book_ids_to_process)}")
    logger.info(f"Total Files Written: {total_files_written}")
    logger.info(f"Total Errors Encountered: {total_errors}")
    logger.info(f"Output directory: {output_dir.resolve()}")


# --- Main Execution (MODIFIED) ---
async def main():
    """Parses arguments and runs the export."""
    parser = argparse.ArgumentParser(
        description="Export Markdown pages from MongoDB to local files using numeric paths."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--book-id", type=int, help="Export Markdown for a specific book ID."
    )
    group.add_argument(
        "--all", action="store_true", help="Export Markdown for all books found."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Base directory to export files into (default: {DEFAULT_OUTPUT_DIR})",
    )
    # --- REMOVED: --env-file argument ---
    # parser.add_argument(
    #     "--env-file", type=str, default=".env", help="Path to the .env file (default: .env)",
    # )

    args = parser.parse_args()

    # --- Use settings object for URI ---
    if settings is None:
        logger.critical("Failed to load settings from src.config. Aborting.")
        return

    mongo_uri = settings.mongo_uri  # Get URI from imported settings

    if not mongo_uri:
        logger.critical("mongo_uri not found in settings. Aborting.")
        return

    db_connection = None
    try:
        db_connection = await connect_db(mongo_uri, "dvaita_scraper")
        # --- Check connection before proceeding ---
        if db_connection is None:
            logger.error("Database connection failed. Cannot export.")
            return

        await export_markdown(
            db=db_connection,
            book_id_to_export=args.book_id,
            export_all=args.all,
            output_dir=args.output_dir,
        )
    finally:
        # --- Correct check for closing connection ---
        if db_connection is not None and db_connection.client:
            try:
                db_connection.client.close()
                logger.info("Closed database connection.")
            except Exception as close_e:
                logger.error(f"Error closing DB connection: {close_e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Export script interrupted by user.")
    except Exception as e:
        logger.exception("An unexpected error occurred during export.")
