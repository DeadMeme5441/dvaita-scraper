# src/worker.py
import asyncio
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from loguru import logger

# Import settings
from src.config import settings
# --- Import task functions from their new locations ---
from src.tasks.discovery import discover_initial_books, fetch_book_details
from src.tasks.processing import run_process_book_task
# Import DB/HTTP client managers
from src.db.mongo_client import connect_to_mongo, close_mongo_connection
from src.utils.http_client import http_client
from src.utils.proxy_provider import proxy_provider

async def startup(ctx: dict) -> None:
    """ Runs when the ARQ worker starts. """
    logger.info("ARQ Worker starting up...")
    await connect_to_mongo()
    if http_client: await http_client._get_session()
    else: logger.error("HTTP Client not available during worker startup.")
    if proxy_provider and settings and settings.proxy_api_token:
        logger.info("Refreshing proxy list on worker startup...")
        try: await proxy_provider.refresh()
        except Exception as e: logger.error(f"Failed proxy refresh: {e}")
    # Store redis pool in context
    ctx['redis'] = await create_pool(WorkerSettings.redis_settings)
    logger.info("ARQ Worker startup complete.")

async def shutdown(ctx: dict) -> None:
    """ Runs when the ARQ worker shuts down. """
    logger.info("ARQ Worker shutting down...")
    redis: ArqRedis = ctx.get('redis')
    if redis: await redis.close()
    if http_client: await http_client.close()
    if proxy_provider: await proxy_provider.close()
    await close_mongo_connection()
    logger.info("ARQ Worker shutdown complete.")


class WorkerSettings:
    """ ARQ worker settings. """
    # --- List the new task functions ---
    functions = [
        discover_initial_books,
        fetch_book_details,
        run_process_book_task
    ]

    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    on_startup = startup
    on_shutdown = shutdown
    max_tries = 1 # Disable ARQ default retry for now
    # Optional: job_timeout = 600 # 10 minutes, adjust as needed
