# src/config.py
import os
from typing import List, Optional, Union
from typing_extensions import Annotated

from pydantic import Field, field_validator, AnyHttpUrl, ValidationInfo, model_validator
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

    # --- Phase 2: Adaptive Scraping Settings ---

    # Initial values
    initial_concurrency: int = Field(
        default=20, gt=0, description="Initial concurrency level for scraping tasks."
    )
    initial_timeout_seconds: float = Field(
        default=300.0, gt=0, description="Initial request timeout in seconds."
    )

    # Concurrency bounds and adjustment
    min_concurrency: int = Field(
        default=5, gt=0, description="Minimum allowed concurrency level."
    )
    max_concurrency: int = Field(
        default=50, gt=0, description="Maximum allowed concurrency level."
    )
    concurrency_error_threshold: int = Field(
        default=3, ge=0, description="Number of errors (5xx/timeout) in a batch to trigger concurrency reduction."
    )
    concurrency_reduction_factor: float = Field(
        default=0.9, gt=0, lt=1, description="Factor to multiply concurrency by when reducing (e.g., 0.9 reduces by 10%)."
    )
    concurrency_increase_step: int = Field(
        default=1, gt=0, description="Amount to increase concurrency by if a batch has zero errors."
    )

    # Timeout bounds and adjustment
    min_timeout_seconds: float = Field(
        default=60.0, gt=0, description="Minimum allowed request timeout."
    )
    max_timeout_seconds: float = Field(
        default=600.0, gt=0, description="Maximum allowed request timeout."
    )
    timeout_factor: float = Field(
        default=1.5, gt=1.0, description="Factor to multiply average successful request time by to set new timeout."
    )
    timeout_fallback_seconds: float = Field(
        default=300.0, gt=0, description="Timeout to use if no successful requests were made in a batch."
    )

    # Progress update frequency
    progress_update_interval: int = Field(
        default=25, gt=0, description="Update progress in DB every N items processed (pages or sutras)."
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

    @model_validator(mode='after')
    def check_adaptive_bounds(self) -> 'Settings':
        if self.min_concurrency >= self.max_concurrency:
            raise ValueError("min_concurrency must be less than max_concurrency")
        if self.initial_concurrency < self.min_concurrency or self.initial_concurrency > self.max_concurrency:
            raise ValueError("initial_concurrency must be between min_concurrency and max_concurrency")
        if self.min_timeout_seconds >= self.max_timeout_seconds:
             raise ValueError("min_timeout_seconds must be less than max_timeout_seconds")
        if self.initial_timeout_seconds < self.min_timeout_seconds or self.initial_timeout_seconds > self.max_timeout_seconds:
             raise ValueError("initial_timeout_seconds must be between min_timeout_seconds and max_timeout_seconds")
        if self.timeout_fallback_seconds < self.min_timeout_seconds or self.timeout_fallback_seconds > self.max_timeout_seconds:
             raise ValueError("timeout_fallback_seconds must be between min_timeout_seconds and max_timeout_seconds")
        return self

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
    logger.info(f"Initial Concurrency: {settings.initial_concurrency}, Timeout: {settings.initial_timeout_seconds}s")
    logger.info(f"Concurrency Range: [{settings.min_concurrency}-{settings.max_concurrency}], Timeout Range: [{settings.min_timeout_seconds}-{settings.max_timeout_seconds}]s")
except Exception as e:
    logger.critical(f"Failed to load application settings: {e}")
    settings = None
