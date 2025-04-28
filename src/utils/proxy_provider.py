# src/utils/proxy_provider.py

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict

import aiohttp
from loguru import logger

from src.config import settings, Settings


class ProxyProvider:
    """
    Manages a pool of Webshare proxies with round-robin selection and cooldown.

    Fetches proxy lists from the Webshare API and ensures proxies are rested
    for a configured duration before reuse.
    """

    def __init__(self, app_settings: Optional["Settings"] = settings) -> None:
        """
        Initializes the ProxyProvider.

        Requires application settings to be loaded, particularly for Webshare API access.

        :param app_settings: The application's configuration object. Defaults to the globally imported settings.
        :type app_settings: Optional[Settings]
        :raises ValueError: If settings are not loaded or required proxy settings are missing.
        """
        if app_settings is None:
            raise ValueError("Application settings are not loaded or available.")

        self._settings = app_settings
        self._base_url: str = str(
            self._settings.proxy_api_base
        )  # Convert AnyHttpUrl to str
        self._token: Optional[str] = self._settings.proxy_api_token
        self._cooldown: timedelta = timedelta(
            seconds=self._settings.proxy_cooldown_seconds
        )

        if not self._token:
            logger.warning(
                "PROXY_API_TOKEN is not set. ProxyProvider will not function."
            )
            # Allow initialization but refresh/get_proxy will fail later if token remains None

        self._session: Optional[aiohttp.ClientSession] = None
        self._proxies: List[str] = []
        self._last_used: Dict[str, datetime] = {}
        self._idx: int = 0
        self._lock = (
            asyncio.Lock()
        )  # Lock for thread-safe access to shared state (_proxies, _last_used, _idx)

    async def _ensure_session(self) -> None:
        """
        Ensures the aiohttp session is initialized with the API token header.

        :raises ValueError: If the proxy API token is not configured.
        """
        if self._session is None or self._session.closed:
            if not self._token:
                raise ValueError(
                    "Cannot create API session: PROXY_API_TOKEN is not set."
                )
            headers = {"Authorization": f"Token {self._token}"}
            self._session = aiohttp.ClientSession(headers=headers)
            logger.debug("Initialized aiohttp session for ProxyProvider.")

    async def refresh(self) -> None:
        """
        Fetches a fresh list of proxies from the Webshare API.

        Uses the configuration from the settings object to construct the API request.
        Resets cooldown timers, making all fetched proxies immediately available.

        :raises ValueError: If the proxy API token is not configured.
        :raises aiohttp.ClientError: If fetching from the Webshare API fails.
        """
        if not self._token:
            logger.error("Cannot refresh proxies: PROXY_API_TOKEN is not set.")
            raise ValueError("Proxy API token is required to refresh proxies.")

        await self._ensure_session()
        assert self._session is not None  # Should be initialized by _ensure_session

        try:
            # 1. Get download token from proxy config endpoint
            cfg_url = f"{self._base_url}/proxy/config/"
            logger.debug(f"Fetching proxy config from: {cfg_url}")
            async with self._session.get(cfg_url) as resp:
                resp.raise_for_status()  # Raise exception for bad status codes
                cfg = await resp.json()
            dl_token = cfg.get("proxy_list_download_token")
            if not dl_token:
                logger.error(
                    "Failed to get proxy_list_download_token from Webshare API response."
                )
                raise ValueError("Missing proxy_list_download_token in API response.")
            logger.info("Webshare proxy download token obtained.")

            # 2. Construct download URL using settings
            # Convert list settings back to comma-separated strings for the URL
            countries = ",".join(self._settings.proxy_country_codes)
            search_filters = ",".join(self._settings.proxy_search)
            dl_url = (
                f"{self._base_url}/proxy/list/download/"
                f"{dl_token}/{countries}/any/"  # 'any' for protocol type seems reasonable
                f"{self._settings.proxy_auth_method}/{self._settings.proxy_endpoint_mode}/"
                f"{search_filters}/"
            )
            logger.debug(f"Downloading proxy list from: {dl_url}")

            # 3. Download the proxy list
            async with self._session.get(dl_url) as resp:
                resp.raise_for_status()
                proxy_list_text = await resp.text()

            # 4. Parse the proxy list (ip:port:username:password format expected)
            fetched_proxies: List[str] = []
            for line in proxy_list_text.splitlines():
                parts = line.strip().split(":")
                if len(parts) == 4:
                    ip, port, user, pwd = parts
                    # Format as http://user:pass@ip:port
                    fetched_proxies.append(f"http://{user}:{pwd}@{ip}:{port}")
                elif line.strip():  # Log if line is not empty but doesn't match format
                    logger.warning(f"Skipping malformed proxy line: {line.strip()}")

            if not fetched_proxies:
                logger.warning(
                    "Proxy list downloaded successfully, but no valid proxies were parsed."
                )
            else:
                logger.info(
                    f"Successfully loaded {len(fetched_proxies)} proxies from Webshare."
                )

            # 5. Reset internal state safely using the lock
            async with self._lock:
                self._proxies = fetched_proxies
                # Reset last used time for all proxies (including potentially removed ones)
                self._last_used = {
                    p: datetime.min.replace(tzinfo=timezone.utc)
                    for p in fetched_proxies
                }
                self._idx = 0  # Reset round-robin index

        except aiohttp.ClientError as e:
            logger.exception(f"HTTP error occurred while refreshing proxies: {e}")
            raise  # Re-raise the exception after logging
        except Exception as e:
            logger.exception(f"An unexpected error occurred during proxy refresh: {e}")
            raise  # Re-raise other unexpected errors

    async def get_proxy(self) -> Optional[str]:
        """
        Returns the next available proxy URL that has met the cooldown requirement.

        Cycles through the proxy list in a round-robin fashion. If no proxies
        are available (either list is empty or all are on cooldown), it waits
        asynchronously until the soonest one becomes available.

        :return: A proxy URL string (e.g., 'http://user:pass@ip:port') or None if no proxies could be loaded/refreshed.
        :rtype: Optional[str]
        :raises ValueError: If the proxy API token is not configured and a refresh is needed.
        """
        if not self._token:
            logger.error("Cannot get proxy: PROXY_API_TOKEN is not set.")
            return None  # Cannot function without token

        async with self._lock:
            # Refresh if the proxy list is initially empty
            if not self._proxies:
                logger.info("Proxy list is empty. Attempting initial refresh...")
                try:
                    # Use await within the lock for the refresh operation
                    # Note: This holds the lock during refresh, which might be long.
                    # Consider alternative patterns if lock contention becomes an issue.
                    await self.refresh()
                except Exception:
                    logger.error(
                        "Initial proxy refresh failed. Cannot provide a proxy."
                    )
                    return None  # Return None if refresh fails

                # Check again if proxies were loaded after refresh
                if not self._proxies:
                    logger.error("Proxy list still empty after refresh attempt.")
                    return None

            now = datetime.now(timezone.utc)
            num_proxies = len(self._proxies)
            checked_indices = 0

            while checked_indices < num_proxies:
                current_proxy_index = self._idx % num_proxies
                proxy = self._proxies[current_proxy_index]
                self._idx += 1  # Increment index for next call
                checked_indices += 1

                last_used_time = self._last_used.get(
                    proxy, datetime.min.replace(tzinfo=timezone.utc)
                )

                # Check if cooldown period has passed
                if now - last_used_time >= self._cooldown:
                    self._last_used[proxy] = now  # Update last used time
                    logger.debug(
                        f"Providing proxy: ...@{proxy.split('@')[-1]}"
                    )  # Basic obfuscation
                    return proxy

            # If loop completes, no proxy is currently available (all on cooldown)
            # Calculate wait time for the earliest available proxy
            if not self._last_used:  # Should not happen if _proxies is populated
                logger.error(
                    "Inconsistent state: Proxies exist but _last_used is empty."
                )
                return None  # Or attempt refresh again?

            earliest_available_time = min(self._last_used.values()) + self._cooldown
            wait_duration = (earliest_available_time - now).total_seconds()
            wait_duration = max(wait_duration, 0.1)  # Ensure minimum wait time

            logger.warning(
                f"All {num_proxies} proxies are on cooldown. Waiting for {wait_duration:.2f} seconds..."
            )

        # Release the lock before sleeping
        await asyncio.sleep(wait_duration)

        # Retry getting a proxy after waiting
        return await self.get_proxy()

    async def reset_cooldowns(self) -> None:
        """
        Resets the last-used timestamp for all currently loaded proxies,
        making them immediately available for use.
        """
        async with self._lock:
            self._last_used = {
                p: datetime.min.replace(tzinfo=timezone.utc) for p in self._proxies
            }
            self._idx = 0  # Reset index as well
        logger.info("All proxy cooldowns have been reset.")

    async def close(self) -> None:
        """
        Closes the internal aiohttp client session if it exists.
        Should be called during application shutdown.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("ProxyProvider's aiohttp session closed.")


# --- Singleton Instance ---
# Instantiate the provider using the loaded settings.
# This instance can be imported and used throughout the application.
try:
    # Pass the settings instance explicitly during initialization
    proxy_provider = ProxyProvider(app_settings=settings)
except ValueError as e:
    logger.critical(f"Failed to initialize ProxyProvider: {e}")
    # Application might not be able to proceed without proxies
    proxy_provider = None  # Indicate failure
