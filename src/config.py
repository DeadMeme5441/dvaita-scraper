# src/config.py
import os
from typing import List, Optional, Union
from typing_extensions import Annotated

from pydantic import Field, field_validator, AnyHttpUrl, ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict, NoDecode
from loguru import logger

APP_ENV = os.getenv("APP_ENV", "development").lower()
ENV_FILE = f".env.{APP_ENV}" if APP_ENV != "production" else ".env"

logger.info(f"Application Environment (APP_ENV): {APP_ENV}")
logger.info(f"Attempting to load environment variables from: {ENV_FILE}")

if os.path.exists(ENV_FILE):
    logger.info(f"Found environment file: {ENV_FILE}")
else:
    logger.warning(
        f"Environment file not found: {ENV_FILE}. Relying on system environment variables."
    )


class Settings(BaseSettings):
    """ Application settings loaded from environment variables (ARQ Version). """

    # --- Core Application Settings ---
    app_env: str = Field(default=APP_ENV, description="Application environment")
    log_level: str = Field(default="INFO", description="Logging level")
    log_format_json: bool = Field(default=True, description="Output logs in JSON format")

    # --- Target Website Settings ---
    target_base_url: AnyHttpUrl = Field(
        default=AnyHttpUrl("https://dvaitavedanta.in"),
        description="Base URL of the website to scrape",
    )

    # --- MongoDB Settings ---
    mongo_uri: str = Field(
        default="mongodb://localhost:27017/dvaita_scraper",
        description="MongoDB connection URI",
    )
    mongo_db_name: Optional[str] = Field(
        default=None, description="MongoDB database name (if not specified in URI)"
    )

    # --- Redis Settings (for ARQ) ---
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for ARQ job queue",
    )

    # --- Proxy Settings (Webshare) ---
    proxy_api_token: Optional[str] = Field(default=None, description="Webshare API token")
    proxy_api_base: AnyHttpUrl = Field(
        default=AnyHttpUrl("https://proxy.webshare.io/api/v2"),
        description="Webshare API base endpoint",
    )
    proxy_cooldown_seconds: int = Field(default=5, ge=0, description="Proxy cooldown")
    proxy_country_codes: List[str] = Field(default=["-"], description="Webshare country codes")
    proxy_auth_method: str = Field(default="username", description="Webshare auth method")
    proxy_endpoint_mode: str = Field(default="direct", description="Webshare endpoint mode")
    proxy_search: List[str] = Field(default=["-"], description="Webshare search filters")


    # --- Scraping Task Concurrency Settings ---
    sutra_discovery_concurrency: int = Field(
        default=5, gt=0,
        description="Max concurrent requests for fetching page tree views (sutra discovery)."
    )
    content_fetch_concurrency: int = Field(
        default=10, gt=0,
        description="Max concurrent requests for fetching sutra content (load-data)."
    )

    # --- NEW: Request Timeout Setting ---
    scraper_request_timeout_seconds: int = Field(
        default=300, # Default to 5 minutes
        gt=0,
        description="Timeout in seconds for individual HTTP requests during scraping."
    )


    # --- Validators ---
    @field_validator("proxy_country_codes", "proxy_search", mode="before")
    @classmethod
    def _parse_comma_separated_list_for_proxies(cls, v: Optional[Union[str, List[str]]]) -> List[str]:
        default_value = ["-"]
        if v is None: return default_value
        if isinstance(v, list): return v
        if isinstance(v, str):
            items = [item.strip() for item in v.split(",") if item.strip()]
            return items if items else default_value
        logger.warning(f"Unexpected type '{type(v)}' for proxy list field. Using default: {default_value}")
        return default_value

    model_config = SettingsConfigDict(
        env_file=ENV_FILE if os.path.exists(ENV_FILE) else None,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

# --- Singleton Instance ---
try:
    settings = Settings()
    logger.info(f"Loaded settings for environment: {settings.app_env}")
    logger.info(f"MongoDB URI Host (example): {settings.mongo_uri.split('@')[-1].split('/')[0]}")
    logger.info(f"Redis URL Host (example): {settings.redis_url.split('@')[-1].split('/')[0]}")
    logger.info(f"Log format set to JSON: {settings.log_format_json}")
    logger.info(f"Sutra Discovery Concurrency: {settings.sutra_discovery_concurrency}")
    logger.info(f"Content Fetch Concurrency: {settings.content_fetch_concurrency}")
    logger.info(f"Scraper Request Timeout: {settings.scraper_request_timeout_seconds}s") # Log new setting
except Exception as e:
    logger.critical(f"Failed to load application settings: {e}")
    settings = None
