# src/main.py

import sys
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Union

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from prometheus_fastapi_instrumentator import Instrumentator

# Import settings and utility singletons/functions
from src.config import settings
from src.db.mongo_client import connect_to_mongo, close_mongo_connection, get_mongo_db
from src.utils.http_client import http_client
from src.utils.proxy_provider import proxy_provider

# Import the scraper router
from src.routes.scraper_routes import router as scraper_router

# Import ARQ pool management functions
from src.arq_pool import init_arq_pool, close_arq_pool, get_arq_pool_instance

# --- REMOVE Global variable for ARQ Redis Pool ---
# arq_redis_pool: Optional[ArqRedis] = None # REMOVED

# --- Loguru Configuration ---
# (No changes needed here)
DEFAULT_LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"


def setup_logging():
    logger.remove()
    log_level = "INFO"
    use_json = True
    if settings:
        log_level = settings.log_level.upper()
        use_json = settings.log_format_json
    logger.add(
        sys.stderr, level=log_level, format=DEFAULT_LOG_FORMAT, serialize=use_json
    )
    logger.info(
        f"Loguru logger configured. Level: {log_level}, JSON Format: {use_json}"
    )


try:
    setup_logging()
except Exception as e:
    import logging

    logging.basicConfig(level=logging.INFO)
    logging.critical(f"Failed to configure Loguru: {e}")
    logger.critical(f"Failed to configure Loguru: {e}")


# --- Lifespan Management ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages application startup and shutdown events (ARQ Version)."""
    logger.info("Application startup sequence initiated...")
    if settings is None:
        logger.critical(
            "Skipping resource initialization: Application settings failed to load."
        )
    else:
        # 1. Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        try:
            await connect_to_mongo()
            logger.info("MongoDB connection attempt finished.")
        except Exception as e:
            logger.critical(f"MongoDB connection failed during startup lifespan: {e}.")

        # 2. Initialize ARQ Redis Pool (using imported function)
        await init_arq_pool()

        # 3. Initialize/Refresh Proxy Provider
        if proxy_provider and settings.proxy_api_token:
            logger.info("Attempting to refresh proxy list...")
            try:
                await proxy_provider.refresh()
            except Exception as e:
                logger.error(f"Failed to refresh proxy list during startup: {e}.")
        elif settings and not settings.proxy_api_token:
            logger.warning(
                "Proxy provider not initialized: PROXY_API_TOKEN is not set."
            )
        elif not proxy_provider:
            logger.warning(
                "Proxy provider instance is not available (initialization failed)."
            )

    logger.info("Application startup sequence complete.")
    yield
    # --- Shutdown Sequence ---
    logger.info("Application shutdown sequence initiated...")
    # Close ARQ Redis Pool (using imported function)
    await close_arq_pool()
    # Close HTTP Client Session
    if http_client:
        await http_client.close()
    else:
        logger.warning("HTTP Client instance not available for closing.")
    # Close Proxy Provider Session
    if proxy_provider:
        await proxy_provider.close()
    else:
        logger.warning("Proxy Provider instance not available for closing.")
    # Close MongoDB Connection
    await close_mongo_connection()
    logger.info("Application shutdown sequence complete.")


# --- FastAPI Application Instantiation ---
# (No changes needed here)
if settings is None:
    logger.critical("Settings failed to load. Exiting.")
    sys.exit(1)
app = FastAPI(
    title="Dvaita Scraper Service (ARQ)",
    description="Microservice using ARQ",
    version="0.1.0",
    lifespan=lifespan,
)

# --- Middleware ---
# (No changes needed here)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Prometheus Instrumentation ---
# (No changes needed here)
try:
    instrumentator = Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=True,
        should_respect_env_var=True,
        excluded_handlers=["/health", "/metrics"],
        env_var_name="ENABLE_METRICS",
    )
    instrumentator.instrument(app).expose(app)
    logger.info("Prometheus instrumentation configured.")
except Exception as e:
    logger.error(f"Failed Prometheus instrumentation: {e}")

# --- API Routers ---
# (No changes needed here)
app.include_router(scraper_router, prefix="/scrape", tags=["Scraping"])
logger.info("API router '/scrape' included.")


# --- Basic Endpoints ---
# (No changes needed here, but updated health check)
@app.get("/", tags=["Root"])
async def read_root() -> dict[str, str]:
    app_version = "0.1.0"
    if settings and hasattr(settings, "version"):
        app_version = settings.version
    elif settings is None:
        logger.warning("Settings not available for root endpoint version.")
    return {"service": "dvaita-scraper (ARQ)", "version": app_version, "docs": "/docs"}


@app.get("/health", tags=["Health"])
async def health_check() -> JSONResponse:
    if settings is None:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": "Configuration failed"},
        )
    # Check ARQ Redis pool status via the getter function
    pool = get_arq_pool_instance()
    redis_status = "unavailable"
    if pool:
        try:
            await pool.ping()
            redis_status = "connected"
        except Exception as e:
            logger.error(f"Health check: ARQ Redis ping failed: {e}")
            redis_status = "disconnected"
    # Add DB check
    db_status = "unknown"
    try:
        db = get_mongo_db()
        await db.command("ping")
        db_status = "connected"
    except Exception as e:
        db_status = f"disconnected ({type(e).__name__})"

    final_status = (
        "healthy"
        if redis_status == "connected" and db_status == "connected"
        else "unhealthy"
    )
    status_code = 200 if final_status == "healthy" else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": final_status,
            "dependencies": {"redis": redis_status, "mongodb": db_status},
        },
    )


# --- Main Execution ---
# (No changes needed here)
if __name__ == "__main__":
    logger.info("Starting application directly with Uvicorn...")
    run_log_level = "info"
    if settings and hasattr(settings, "log_level"):
        run_log_level = settings.log_level.lower()
    uvicorn.run(
        "src.main:app", host="0.0.0.0", port=8000, reload=True, log_level=run_log_level
    )
