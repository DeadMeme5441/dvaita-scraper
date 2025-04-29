# src/worker.py
import asyncio
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from loguru import logger

from src.config import settings

# Import task functions
from src.tasks.discovery import discover_initial_books, fetch_book_details
from src.tasks.processing import discover_book_sutras, fetch_sutras_for_book

# --- Import Phase 3 Tasks ---
from src.tasks.parsing import (
    preprocess_book_html,
    generate_markdown_from_preprocessed,
)  # Updated task names

# Import DB/HTTP client managers
from src.db.mongo_client import connect_to_mongo, close_mongo_connection
from src.utils.http_client import http_client
from src.utils.proxy_provider import proxy_provider


async def startup(ctx: dict) -> None:  # Removed type hint for ctx
    logger.info("ARQ Worker starting up...")
    if settings is None:
        logger.critical("Settings not loaded.")
        return
    await connect_to_mongo()
    if http_client:
        await http_client._get_session()
    else:
        logger.error("HTTP Client not available during worker startup.")
    if proxy_provider and settings.proxy_api_token:
        logger.info("Refreshing proxy list on worker startup...")
        try:
            await proxy_provider.refresh()
        except Exception as e:
            logger.error(f"Failed proxy refresh during worker startup: {e}")
    ctx["adaptive_state"] = {
        "current_concurrency": settings.initial_concurrency,
        "current_timeout_seconds": settings.initial_timeout_seconds,
    }
    logger.info(
        f"Initialized adaptive state: Concurrency={ctx['adaptive_state']['current_concurrency']}, Timeout={ctx['adaptive_state']['current_timeout_seconds']:.1f}s"
    )
    ctx["redis"] = await create_pool(WorkerSettings.redis_settings)
    logger.info("ARQ Worker startup complete. Ready for tasks.")


async def shutdown(ctx: dict) -> None:  # Removed type hint for ctx
    logger.info("ARQ Worker shutting down...")
    redis: ArqRedis = ctx.get("redis")
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

    functions = [
        # Phase 1
        discover_initial_books,
        fetch_book_details,
        # Phase 2
        discover_book_sutras,
        fetch_sutras_for_book,
        # --- Phase 3 ---
        preprocess_book_html,
        generate_markdown_from_preprocessed,  # Include both tasks
    ]
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    on_startup = startup
    on_shutdown = shutdown
    max_tries = 1
