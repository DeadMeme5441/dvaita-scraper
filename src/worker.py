# src/worker.py
import asyncio
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from loguru import logger
from typing import Optional

from src.config import settings

# --- Import task functions ---
# Phase 1
from src.tasks.discovery import discover_initial_books, fetch_book_details

# Phase 2
from src.tasks.processing import discover_book_sutras, fetch_sutras_for_book

# Phase 3 (Refactored)
from src.tasks.parsing import (
    preprocess_book_html,  # Phase 3a
    add_nav_path_to_preprocessed,  # Phase 3b (NEW)
    generate_markdown_from_db,  # Phase 3c (Renamed)
)

# Import DB/HTTP client managers
from src.db.mongo_client import connect_to_mongo, close_mongo_connection
from src.utils.http_client import http_client
from src.utils.proxy_provider import proxy_provider


async def startup(ctx: dict) -> None:
    """Runs when the ARQ worker starts. (Unchanged)"""
    logger.info("ARQ Worker starting up...")
    if settings is None:
        logger.critical("Settings not loaded.")
        return
    await connect_to_mongo()
    if http_client:
        # Ensure session exists, don't necessarily create new one if already exists
        await http_client._get_session()
    else:
        logger.error("HTTP Client not available during worker startup.")
    if proxy_provider and settings.proxy_api_token:
        logger.info("Refreshing proxy list on worker startup...")
        try:
            await proxy_provider.refresh()
        except Exception as e:
            logger.error(f"Failed proxy refresh during worker startup: {e}")
    # Initialize adaptive state in context
    ctx["adaptive_state"] = {
        "current_concurrency": settings.initial_concurrency,
        "current_timeout_seconds": settings.initial_timeout_seconds,
    }
    logger.info(
        f"Initialized adaptive state: Concurrency={ctx['adaptive_state']['current_concurrency']}, Timeout={ctx['adaptive_state']['current_timeout_seconds']:.1f}s"
    )
    # Store redis pool in context for tasks to use
    ctx["redis"] = await create_pool(WorkerSettings.redis_settings)
    logger.info("ARQ Worker startup complete. Ready for tasks.")


async def shutdown(ctx: dict) -> None:
    """Runs when the ARQ worker shuts down. (Unchanged)"""
    logger.info("ARQ Worker shutting down...")
    redis: Optional[ArqRedis] = ctx.get("redis")
    if redis:
        await redis.close()
    if http_client:
        await http_client.close()
    if proxy_provider:
        await proxy_provider.close()
    await close_mongo_connection()
    logger.info("ARQ Worker shutdown complete.")


class WorkerSettings:
    """ARQ worker settings."""

    # --- Updated list of task functions ---
    functions = [
        # Phase 1
        discover_initial_books,
        fetch_book_details,
        # Phase 2
        discover_book_sutras,
        fetch_sutras_for_book,
        # Phase 3 (Refactored)
        preprocess_book_html,  # 3a
        add_nav_path_to_preprocessed,  # 3b (NEW)
        generate_markdown_from_db,  # 3c (Renamed)
    ]
    # ------------------------------------

    redis_settings = RedisSettings.from_dsn(settings.redis_url) if settings else None
    on_startup = startup
    on_shutdown = shutdown
    # Keep retries disabled at ARQ level for now, rely on task logic/tenacity
    max_tries = 1

    # Handle case where settings might not load during import
    if redis_settings is None:
        logger.critical(
            "Redis settings could not be loaded for ARQ Worker. Worker might not start correctly."
        )
        # Optionally raise an error or exit
