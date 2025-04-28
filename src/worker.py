# src/worker.py
import asyncio
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from loguru import logger

from src.config import settings

# --- Import task functions from their specific modules ---
from src.tasks.discovery import discover_initial_books, fetch_book_details
# --- Import NEW task functions ---
from src.tasks.processing import discover_book_sutras, fetch_sutras_for_book

# Import DB/HTTP client managers
from src.db.mongo_client import connect_to_mongo, close_mongo_connection
from src.utils.http_client import http_client
from src.utils.proxy_provider import proxy_provider


async def startup(ctx: dict) -> None:
    """Runs when the ARQ worker starts."""
    logger.info("ARQ Worker starting up...")
    # Initialize database connection
    await connect_to_mongo()
    # Initialize HTTP client session
    if http_client:
        await http_client._get_session() # Ensure session exists
    else:
        logger.error("HTTP Client not available during worker startup.")
    # Initialize Proxy Provider and refresh list
    if proxy_provider and settings and settings.proxy_api_token:
        logger.info("Refreshing proxy list on worker startup...")
        try:
            await proxy_provider.refresh()
        except Exception as e:
            logger.error(f"Failed proxy refresh during worker startup: {e}")
    # Create Redis pool for worker internal use (e.g., passing to tasks via context)
    # This pool is managed by ARQ's worker lifecycle.
    ctx["redis"] = await create_pool(WorkerSettings.redis_settings)
    logger.info("ARQ Worker startup complete. Ready for tasks.")


async def shutdown(ctx: dict) -> None:
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


class WorkerSettings:
    """ARQ worker settings."""

    # --- List ALL task functions the worker can execute ---
    functions = [
        # Discovery tasks
        discover_initial_books,
        fetch_book_details,
        # Processing tasks (NEW)
        discover_book_sutras,
        fetch_sutras_for_book,
    ]

    # Redis connection settings from main config
    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    # Startup and shutdown hooks for the worker process
    on_startup = startup
    on_shutdown = shutdown

    # Max attempts for a job (1 means no automatic retries by ARQ)
    max_tries = 1

    # Optional: Set a global timeout for jobs if needed
    # job_timeout = 600 # e.g., 10 minutes

    # Keep jobs in Redis for a certain duration after completion/failure
    # keep_result_seconds = 3600 # e.g., 1 hour
