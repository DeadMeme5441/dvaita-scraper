# src/db/mongo_client.py

from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from loguru import logger
from pymongo.errors import ConnectionFailure, ConfigurationError

from src.config import settings

# Module-level variables to hold the client and database instances
_mongo_client: Optional[AsyncIOMotorClient] = None
_mongo_db: Optional[AsyncIOMotorDatabase] = None


async def connect_to_mongo() -> None:
    """
    Establishes an asynchronous connection to the MongoDB database.

    Uses the connection URI and database name specified in the application settings.
    Initializes the module-level _mongo_client and _mongo_db variables.
    Should be called during application startup (e.g., FastAPI lifespan event).
    """
    global _mongo_client, _mongo_db
    if settings is None:
        logger.error("Cannot connect to MongoDB: Application settings are not loaded.")
        return
    if _mongo_client is not None:
        logger.warning("MongoDB connection already established.")
        return

    mongo_uri = settings.mongo_uri
    db_name = settings.mongo_db_name  # Optional setting

    logger.info(
        f"Attempting to connect to MongoDB: {mongo_uri.split('@')[-1].split('/')[0]}"
    )  # Obfuscated log
    try:
        _mongo_client = AsyncIOMotorClient(
            mongo_uri,
            # Optional: Add server selection timeout to fail faster if server is down
            serverSelectionTimeoutMS=5000,
        )

        # Verify connection by trying to fetch server info
        await _mongo_client.admin.command("ping")  # Ping command is lightweight

        # Determine the database name
        if db_name:
            _mongo_db = _mongo_client[db_name]
            logger.info(f"Using specified database name: {db_name}")
        else:
            # Try to get default database from URI if db_name not specified
            _mongo_db = _mongo_client.get_default_database()
            if _mongo_db is None:
                # This happens if the URI doesn't include a database name
                raise ConfigurationError(
                    "MongoDB database name not found in URI and MONGO_DB_NAME setting is not set."
                )
            logger.info(f"Using default database from URI: {_mongo_db.name}")

        logger.success(f"Successfully connected to MongoDB database: {_mongo_db.name}")

    except ConnectionFailure as e:
        logger.critical(f"Failed to connect to MongoDB: Connection failure - {e}")
        _mongo_client = None
        _mongo_db = None
        # Depending on application needs, you might want to raise SystemExit here
    except ConfigurationError as e:
        logger.critical(f"Failed to connect to MongoDB: Configuration error - {e}")
        _mongo_client = None
        _mongo_db = None
    except Exception as e:
        logger.critical(f"An unexpected error occurred during MongoDB connection: {e}")
        _mongo_client = None
        _mongo_db = None


async def close_mongo_connection() -> None:
    """
    Closes the asynchronous connection to the MongoDB database.

    Should be called during application shutdown (e.g., FastAPI lifespan event).
    """
    global _mongo_client, _mongo_db
    if _mongo_client:
        _mongo_client.close()
        _mongo_client = None
        _mongo_db = None
        logger.info("MongoDB connection closed.")
    else:
        logger.warning(
            "Attempted to close MongoDB connection, but no active connection found."
        )


def get_mongo_db() -> AsyncIOMotorDatabase:
    """
    Returns the active MongoDB database instance.

    This function is intended for use with FastAPI's dependency injection system (`Depends`).
    It ensures that a connection attempt has been made (usually during startup).

    :return: The AsyncIOMotorDatabase instance.
    :rtype: AsyncIOMotorDatabase
    :raises RuntimeError: If the database connection has not been initialized
                          (connect_to_mongo was not called or failed).
    """
    if _mongo_db is None:
        # This condition should ideally not be met if connect_to_mongo is called correctly
        # during application startup via lifespan events.
        logger.error(
            "MongoDB connection accessed before initialization or after failure."
        )
        raise RuntimeError("Database connection is not available.")
    return _mongo_db


# You can also provide functions to get specific collections if needed, e.g.:
# def get_works_collection() -> AsyncIOMotorCollection:
#     db = get_mongo_db()
#     return db["works"] # Replace "works" with actual collection name constant
