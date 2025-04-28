# src/utils/http_client.py

import asyncio
import time
import json # Import json for decoding
from typing import Optional, Dict, Any, Union, Tuple

import aiohttp
from tenacity import (
    retry,
    # stop_after_attempt, # REMOVE stop condition for infinite retries
    wait_exponential,
    retry_if_exception_type,
    RetryError,
    before_sleep_log,
)
from loguru import logger

# Import settings and proxy provider instance
from src.config import settings, Settings
from src.utils.proxy_provider import proxy_provider, ProxyProvider


# Define common exceptions that warrant a retry
# --- ADD ClientProxyConnectionError explicitly ---
RETRYABLE_EXCEPTIONS = (
    aiohttp.ClientConnectionError,
    aiohttp.ClientProxyConnectionError, # Explicitly add proxy connection errors
    aiohttp.ClientPayloadError,
    # Consider carefully which server errors to retry infinitely
    # Maybe retry 500, 502, 504 indefinitely, but not 503? For now, keep all ClientResponseError
    aiohttp.ClientResponseError,
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

# Define status codes that indicate errors for adaptive logic
ADAPTIVE_ERROR_STATUS_CODES = {500, 502, 503, 504, 408, 429}


class AsyncHTTPClient:
    """Async HTTP client with retries, proxy support, configurable timeouts, and status/duration reporting."""

    def __init__(
        self,
        app_settings: Optional[Settings] = settings,
        proxy_svc: Optional[ProxyProvider] = proxy_provider,
    ) -> None:
        """Initializes the AsyncHTTPClient."""
        if app_settings is None:
            raise ValueError("Application settings are not loaded or available for HTTPClient.")

        self._settings = app_settings
        self._proxy_provider = proxy_svc
        self._session: Optional[aiohttp.ClientSession] = None
        self._default_timeout_seconds = self._settings.initial_timeout_seconds
        self._default_timeout = aiohttp.ClientTimeout(total=self._default_timeout_seconds)

        # Configure tenacity retry decorator
        self.retry_decorator = retry(
            # --- REMOVE stop condition for infinite retries ---
            # stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=2, max=60), # Increase wait time between retries
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            before_sleep=before_sleep_log(logger, "WARNING"), # Log before retrying
            reraise=True, # Reraise the exception if something *else* goes wrong
        )
        logger.info(f"Tenacity retry configured for: {RETRYABLE_EXCEPTIONS} (Infinite Attempts)")


    async def _get_session(self) -> aiohttp.ClientSession:
        """Gets or creates the aiohttp session."""
        if self._session is None or self._session.closed:
            logger.debug("Initializing new aiohttp.ClientSession for AsyncHTTPClient.")
            connector = aiohttp.TCPConnector(limit_per_host=50)
            self._session = aiohttp.ClientSession(connector=connector, timeout=self._default_timeout)
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
        timeout_seconds: Optional[float] = None,
        **kwargs: Any,
    ) -> Tuple[Optional[int], Optional[Any], float, Optional[Exception]]:
        """
        Performs an asynchronous HTTP request. Handles proxy, timeout, error capture.
        Retries are handled by the calling method (e.g., get).

        Returns:
            Tuple: (status_code, response_content_bytes, duration_seconds, error)
        """
        session = await self._get_session()
        proxy_url: Optional[str] = None
        request_headers = DEFAULT_HEADERS.copy()
        if headers: request_headers.update(headers)

        if use_proxy:
            if self._proxy_provider:
                proxy_url = await self._proxy_provider.get_proxy()
                if not proxy_url:
                    logger.warning(f"Failed to get proxy for {url}. Proceeding without proxy.")
                    use_proxy = False
                else:
                    proxy_host = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
                    logger.debug(f"Using proxy {proxy_host} for {method} {url}")
            else:
                logger.warning(f"Proxy requested for {url}, but provider unavailable.")
                use_proxy = False

        current_timeout_value = timeout_seconds if timeout_seconds is not None else self._default_timeout_seconds
        request_timeout = aiohttp.ClientTimeout(total=current_timeout_value)

        request_params = {
            "method": method, "url": url, "headers": request_headers,
            "params": params, "data": data, "json": json,
            "proxy": proxy_url if use_proxy else None,
            "timeout": request_timeout, **kwargs,
        }

        log_url = url.split("?")[0]
        start_time = time.monotonic()
        status_code: Optional[int] = None
        response_content: Optional[bytes] = None # Read as bytes
        error: Optional[Exception] = None

        try:
            logger.debug(f"Attempting {method} {log_url} (Proxy: {use_proxy}, Timeout: {current_timeout_value:.1f}s)")
            async with session.request(**request_params) as response:
                status_code = response.status
                try:
                    # Read the full body before raising for status
                    response_content = await response.read()
                    # Now check status and raise if needed (will be caught below)
                    response.raise_for_status()
                    logger.debug(f"Success: {method} {log_url} -> Status {status_code}")
                except aiohttp.ClientResponseError as http_error:
                    # This catches non-2xx status codes after reading body
                    logger.warning(f"HTTP Error: {method} {log_url} -> Status {http_error.status}, Message: {http_error.message}")
                    error = http_error # Store the specific HTTP error
                except Exception as read_error:
                    logger.warning(f"Error reading response body for {method} {log_url} (Status {status_code}): {read_error}")
                    error = read_error

        # Catch exceptions during the request itself (connection, timeout, proxy)
        except asyncio.TimeoutError as timeout_error:
            logger.warning(f"Timeout Error: {method} {log_url} after {current_timeout_value:.1f}s")
            error = timeout_error
        except aiohttp.ClientProxyConnectionError as proxy_conn_error:
             logger.warning(f"Proxy Connection Error: {method} {log_url}: {proxy_conn_error}")
             error = proxy_conn_error # Store specific proxy error
        except aiohttp.ClientConnectionError as conn_error: # Catch other connection errors
            logger.warning(f"Connection Error: {method} {log_url}: {conn_error}")
            error = conn_error
        except Exception as e:
            logger.warning(f"Request Failed Unexpectedly: {method} {log_url}, Error: {type(e).__name__} - {e}")
            error = e

        end_time = time.monotonic()
        duration = end_time - start_time
        logger.debug(f"Request {method} {log_url} finished. Status: {status_code}, Duration: {duration:.3f}s, Error: {type(error).__name__ if error else 'None'}")

        # --- IMPORTANT: Raise retryable errors here for tenacity ---
        # If an error occurred that is in our retry list, raise it so tenacity can catch it.
        if error and type(error) in RETRYABLE_EXCEPTIONS:
            raise error # Let tenacity handle the retry

        # If it's not a retryable error, return the result tuple
        return status_code, response_content, duration, error


    async def get(
        self,
        url: str,
        use_proxy: bool = True,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
        response_format: str = "text",
        **kwargs: Any,
    ) -> Tuple[Optional[int], Optional[Union[str, dict, bytes]], float, Optional[Exception]]:
        """
        Performs a GET request with retry logic, proxy, specific timeout,
        and returns status, decoded content, duration, and error info.
        """
        status_code: Optional[int] = None
        content: Optional[Union[str, dict, bytes]] = None
        duration: float = 0.0
        final_error: Optional[Exception] = None
        start_time = time.monotonic() # Record start time for duration calculation on failure

        try:
            # Apply retry decorator to the internal request method
            decorated_request = self.retry_decorator(self.request)
            status_code, raw_content_bytes, duration, final_error = await decorated_request(
                method="GET",
                url=url,
                use_proxy=use_proxy,
                headers=headers,
                params=params,
                timeout_seconds=timeout_seconds,
                **kwargs,
            )

            # Process content only if request succeeded without critical error
            if final_error is None and status_code is not None and raw_content_bytes is not None:
                if response_format == "json":
                    try:
                        content = json.loads(raw_content_bytes.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError) as decode_error:
                        logger.error(f"JSON decode failed for {url} (Status: {status_code}): {decode_error}")
                        final_error = ValueError(f"Invalid JSON response from {url}")
                        content = None
                elif response_format == "bytes":
                    content = raw_content_bytes
                else: # Default to text
                    try:
                        content = raw_content_bytes.decode('utf-8')
                    except UnicodeDecodeError as decode_error:
                        logger.error(f"Text decode failed for {url} (Status: {status_code}): {decode_error}")
                        final_error = ValueError(f"Invalid text encoding from {url}")
                        content = None

        except RetryError as e:
            # This block is hit if all retries failed for a RETRYABLE_EXCEPTION
            logger.error(f"GET {url} failed after multiple retries. Last error: {e.last_exception}")
            final_error = e.last_exception # Store the actual last exception
            duration = time.monotonic() - start_time # Approximate duration over all retries

        except Exception as e:
            # Catch non-retryable errors raised by self.request or unexpected errors here
            logger.error(f"GET {url} failed with non-retryable error: {type(e).__name__} - {e}")
            final_error = e
            duration = time.monotonic() - start_time

        # Ensure content is None if there was any final error
        if final_error is not None:
            content = None

        return status_code, content, duration, final_error


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
