# src/worker.py
import asyncio

# import math # No longer needed here
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from loguru import logger

from src.config import settings

# Import task functions
from src.tasks.discovery import discover_initial_books, fetch_book_details
from src.tasks.processing import discover_book_sutras, fetch_sutras_for_book

# Import Phase 3 tasks (placeholder for now)
# from src.tasks.parsing import generate_work_heading_map, process_sutra_html

# Import DB/HTTP client managers
from src.db.mongo_client import connect_to_mongo, close_mongo_connection
from src.utils.http_client import http_client
from src.utils.proxy_provider import proxy_provider

# NOTE: DO NOT import update_adaptive_state from utils here, tasks will import it.


async def startup(ctx: dict) -> None:  # Removed type hint for ctx
    """Runs when the ARQ worker starts."""
    logger.info("ARQ Worker starting up...")
    if settings is None:
        logger.critical("Settings not loaded. Worker cannot initialize properly.")
        return

    # Initialize database connection
    await connect_to_mongo()
    # Initialize HTTP client session
    if http_client:
        await http_client._get_session()  # Ensure session exists
    else:
        logger.error("HTTP Client not available during worker startup.")
    # Initialize Proxy Provider and refresh list
    if proxy_provider and settings.proxy_api_token:
        logger.info("Refreshing proxy list on worker startup...")
        try:
            await proxy_provider.refresh()
        except Exception as e:
            logger.error(f"Failed proxy refresh during worker startup: {e}")

    # --- Initialize Adaptive Scraping State ---
    ctx["adaptive_state"] = {
        "current_concurrency": settings.initial_concurrency,
        "current_timeout_seconds": settings.initial_timeout_seconds,
    }
    logger.info(
        f"Initialized adaptive state: Concurrency={ctx['adaptive_state']['current_concurrency']}, Timeout={ctx['adaptive_state']['current_timeout_seconds']:.1f}s"
    )
    # -----------------------------------------

    # Create Redis pool for worker internal use
    ctx["redis"] = await create_pool(WorkerSettings.redis_settings)
    logger.info("ARQ Worker startup complete. Ready for tasks.")


async def shutdown(ctx: dict) -> None:  # Removed type hint for ctx
    """Runs when the ARQ worker shuts down."""
    logger.info("ARQ Worker shutting down...")
    # Close worker's internal Redis pool
    redis: ArqRedis = ctx.get("redis")
    if redis:
        await redis.close()
    # Close shared HTTP client session
    if http_client:
        await http_client.close()
    # Close shared Proxy Provider session
    if proxy_provider:
        await proxy_provider.close()
    # Close database connection
    await close_mongo_connection()
    logger.info("ARQ Worker shutdown complete.")


# --- Adaptive State Update Function ---
# MOVED TO src/utils/adaptive_logic.py TO AVOID CIRCULAR IMPORT


class WorkerSettings:
    """ARQ worker settings."""

    functions = [
        # Phase 1
        discover_initial_books,
        fetch_book_details,
        # Phase 2
        discover_book_sutras,
        fetch_sutras_for_book,
        # Phase 3 (Add later)
        # generate_work_heading_map,
        # process_sutra_html,
    ]

    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    on_startup = startup
    on_shutdown = shutdown
    max_tries = 1
