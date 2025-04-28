# src/utils/http_client.py

import asyncio
from typing import Optional, Dict, Any, Union

import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)
from loguru import logger

# Import settings and proxy provider instance
# Ensure settings are loaded successfully before initializing
from src.config import settings, Settings
from src.utils.proxy_provider import proxy_provider, ProxyProvider


# Define common exceptions that warrant a retry
RETRYABLE_EXCEPTIONS = (
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    aiohttp.ClientResponseError,  # Retry on server errors (5xx)
    asyncio.TimeoutError,
)

# Define default headers
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}


class AsyncHTTPClient:
    """Async HTTP client with retries, proxy support, and configurable timeouts."""

    def __init__(
        self,
        app_settings: Optional[Settings] = settings,
        proxy_svc: Optional[ProxyProvider] = proxy_provider,
    ) -> None:
        """Initializes the AsyncHTTPClient."""
        if app_settings is None:
            raise ValueError(
                "Application settings are not loaded or available for HTTPClient."
            )

        self._settings = app_settings
        self._proxy_provider = proxy_svc
        self._session: Optional[aiohttp.ClientSession] = None
        # --- FIX: Use timeout from settings ---
        self._default_timeout_seconds = self._settings.scraper_request_timeout_seconds
        self._default_timeout = aiohttp.ClientTimeout(
            total=self._default_timeout_seconds
        )

        self.retry_decorator = retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            reraise=True,
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Gets or creates the aiohttp session."""
        if self._session is None or self._session.closed:
            logger.debug("Initializing new aiohttp.ClientSession for AsyncHTTPClient.")
            # Initialize session with default timeout
            self._session = aiohttp.ClientSession(timeout=self._default_timeout)
        return self._session

    async def request(
        self,
        method: str,
        url: str,
        use_proxy: bool = True,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        # --- FIX: Accept timeout_seconds ---
        timeout_seconds: Optional[int] = None,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse:
        """Performs an asynchronous HTTP request with retry logic and optional proxy."""
        session = await self._get_session()
        proxy_url: Optional[str] = None
        request_headers = DEFAULT_HEADERS.copy()
        if headers:
            request_headers.update(headers)

        if use_proxy:
            if self._proxy_provider:
                proxy_url = await self._proxy_provider.get_proxy()
                if not proxy_url:
                    logger.error(f"Failed proxy for {url}. No proxy.")
                    use_proxy = False
                else:
                    logger.debug(
                        f"Using proxy {proxy_url.split('@')[-1]} for {method} {url}"
                    )
            else:
                logger.warning(f"Proxy requested for {url}, but provider unavailable.")
                use_proxy = False

        # --- FIX: Create specific timeout object if timeout_seconds is provided ---
        request_timeout = self._default_timeout
        if timeout_seconds is not None:
            request_timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            logger.debug(
                f"Using custom timeout of {timeout_seconds}s for {method} {url}"
            )

        request_params = {
            "method": method,
            "url": url,
            "headers": request_headers,
            "params": params,
            "data": data,
            "json": json,
            "proxy": proxy_url if use_proxy else None,
            "timeout": request_timeout,  # Use potentially specific timeout
            **kwargs,
        }

        log_url = url.split("?")[0]
        logger.debug(
            f"Attempting {method} {log_url} (Proxy: {use_proxy}, Timeout: {request_timeout.total}s)"
        )

        try:
            response = await session.request(**request_params)
            response.raise_for_status()
            logger.debug(f"Success: {method} {log_url} -> Status {response.status}")
            return response
        except aiohttp.ClientResponseError as e:
            logger.warning(
                f"HTTP Error: {method} {log_url} -> Status {e.status}, Message: {e.message}"
            )
            raise
        except Exception as e:
            logger.warning(
                f"Request Failed: {method} {log_url}, Error: {type(e).__name__} - {e}"
            )
            raise

    async def get(
        self,
        url: str,
        use_proxy: bool = True,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        # --- FIX: Accept timeout_seconds ---
        timeout_seconds: Optional[int] = None,
        response_format: str = "text",
        **kwargs: Any,
    ) -> Union[str, Dict, bytes, aiohttp.ClientResponse]:
        """Performs a GET request with retry logic, proxy, and specific timeout."""
        decorated_request = self.retry_decorator(self.request)
        try:
            response = await decorated_request(
                method="GET",
                url=url,
                use_proxy=use_proxy,
                headers=headers,
                params=params,
                timeout_seconds=timeout_seconds,  # Pass specific timeout down
                **kwargs,
            )
            async with response:
                if response_format == "json":
                    try:
                        return await response.json(content_type=None)
                    except aiohttp.ContentTypeError as e:
                        logger.error(f"JSON decode failed {url}: {e}")
                        raise ValueError(f"Invalid JSON response from {url}") from e
                elif response_format == "bytes":
                    return await response.read()
                elif response_format == "raw":
                    return response
                else:
                    return await response.text()
        except RetryError as e:
            logger.error(f"GET {url} failed after retries: {e}")
            raise

    async def close(self) -> None:
        """Closes the underlying aiohttp client session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("AsyncHTTPClient's aiohttp session closed.")


# --- Singleton Instance ---
try:
    http_client = AsyncHTTPClient(app_settings=settings, proxy_svc=proxy_provider)
except ValueError as e:
    logger.critical(f"Failed to initialize AsyncHTTPClient: {e}")
    http_client = None
