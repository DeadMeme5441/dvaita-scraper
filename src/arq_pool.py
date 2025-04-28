# src/arq_pool.py

from typing import Optional
from loguru import logger
from arq import create_pool
from arq.connections import ArqRedis

# Import worker settings to get Redis config
from src.worker import WorkerSettings

# Global variable to hold the pool instance within this module
arq_redis_pool: Optional[ArqRedis] = None

async def init_arq_pool() -> None:
    """ Initializes the global ARQ Redis pool for enqueueing jobs. """
    global arq_redis_pool
    if arq_redis_pool is None: # Initialize only if not already done
        logger.info("Initializing ARQ Redis pool for enqueueing jobs...")
        try:
            # Use the same Redis settings the worker uses
            arq_redis_pool = await create_pool(WorkerSettings.redis_settings)
            logger.info("ARQ Redis pool initialized successfully.")
        except Exception as e:
            logger.critical(f"Failed to initialize ARQ Redis pool: {e}")
            arq_redis_pool = None # Ensure it's None on failure
    else:
        logger.warning("ARQ Redis pool already initialized.")

async def close_arq_pool() -> None:
    """ Closes the global ARQ Redis pool. """
    global arq_redis_pool
    if arq_redis_pool:
        await arq_redis_pool.close()
        arq_redis_pool = None # Reset the global variable
        logger.info("ARQ Redis pool closed.")
    else:
        logger.warning("Attempted to close ARQ Redis pool, but it was not initialized.")

def get_arq_pool_instance() -> Optional[ArqRedis]:
    """ Returns the initialized ARQ Redis pool instance. """
    # This provides access without needing Depends in places outside routes if necessary
    # Routes should use the dependency injector pattern.
    return arq_redis_pool
