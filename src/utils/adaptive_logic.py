# src/utils/adaptive_logic.py

import asyncio
import math
from typing import List, Tuple, Optional, Any  # Keep necessary types

from loguru import logger

# Import config directly as it's needed for settings
from src.config import settings

# Import required exception types
from tenacity import RetryError

# Define status codes that indicate errors for adaptive logic
# (Copied from http_client for clarity, could be shared constant)
ADAPTIVE_ERROR_STATUS_CODES = {500, 502, 503, 504, 408, 429}


def update_adaptive_state(
    ctx: dict,  # The worker context dictionary
    batch_results: List[
        Tuple[Optional[int], Optional[Any], float, Optional[Exception]]
    ],
    log_prefix: str = "",
) -> None:
    """
    Analyzes results from a batch of HTTP requests and updates the adaptive
    concurrency and timeout settings stored in the worker context (ctx).

    Args:
        ctx: The ARQ worker context dictionary holding 'adaptive_state'.
        batch_results: A list of tuples, where each tuple is the result from
                       http_client.get: (status, content, duration, error).
        log_prefix: Optional prefix for log messages.
    """
    if "adaptive_state" not in ctx or settings is None:
        logger.error(
            f"{log_prefix} Cannot update adaptive state: Context or settings missing."
        )
        return

    # Read current state
    try:
        current_concurrency = int(ctx["adaptive_state"]["current_concurrency"])
        current_timeout = float(ctx["adaptive_state"]["current_timeout_seconds"])
    except (KeyError, ValueError, TypeError) as e:
        logger.error(
            f"{log_prefix} Invalid adaptive state in context: {ctx.get('adaptive_state')}. Error: {e}"
        )
        return

    new_concurrency = float(current_concurrency)  # Start with float for calculations
    new_timeout = current_timeout

    errors_in_batch = 0
    timeouts_in_batch = 0
    server_errors_in_batch = 0  # Specifically 5xx, 408, 429 etc.
    successful_requests = 0
    successful_durations = []

    for status, _, duration, error in batch_results:
        # Check for timeout errors, including those wrapped by tenacity's RetryError
        is_timeout = isinstance(error, asyncio.TimeoutError) or (
            isinstance(error, RetryError)
            and isinstance(error.last_exception, asyncio.TimeoutError)
        )

        is_server_error = status in ADAPTIVE_ERROR_STATUS_CODES

        if is_timeout:
            errors_in_batch += 1
            timeouts_in_batch += 1
        elif (
            error is not None or is_server_error
        ):  # Count other exceptions and defined server errors
            errors_in_batch += 1
            if is_server_error:
                server_errors_in_batch += 1
        elif status is not None and 200 <= status < 300:
            successful_requests += 1
            # Ensure duration is valid before appending
            if isinstance(duration, (int, float)) and duration >= 0:
                successful_durations.append(duration)
            else:
                logger.warning(
                    f"{log_prefix} Invalid duration ({duration}) for successful request (status {status}). Skipping for avg calculation."
                )

        # Note: Other statuses (e.g., 404) are currently ignored for adaptation

    total_requests = len(batch_results)
    logger.info(
        f"{log_prefix} Batch analysis: Total={total_requests}, Success={successful_requests}, Errors={errors_in_batch} (Timeouts={timeouts_in_batch}, Server={server_errors_in_batch})"
    )

    # --- Adjust Concurrency ---
    if errors_in_batch >= settings.concurrency_error_threshold and total_requests > 0:
        # Reduce concurrency
        reduced_concurrency = new_concurrency * settings.concurrency_reduction_factor
        # Use math.ceil to ensure we reduce by at least 1 if factor is high and concurrency low
        new_concurrency = math.ceil(reduced_concurrency)
        new_concurrency = max(settings.min_concurrency, new_concurrency)  # Apply floor
        if int(new_concurrency) < current_concurrency:  # Log only if changed
            logger.warning(
                f"{log_prefix} High error rate ({errors_in_batch}/{total_requests}). Reducing concurrency: {current_concurrency} -> {int(new_concurrency)}"
            )
    elif errors_in_batch == 0 and total_requests > 0:
        # Increase concurrency slightly if no errors
        new_concurrency = current_concurrency + settings.concurrency_increase_step
        new_concurrency = min(
            settings.max_concurrency, new_concurrency
        )  # Apply ceiling
        if int(new_concurrency) > current_concurrency:  # Log only if changed
            logger.info(
                f"{log_prefix} Zero errors in batch. Increasing concurrency: {current_concurrency} -> {int(new_concurrency)}"
            )
    # Else: Keep concurrency the same

    # --- Adjust Timeout ---
    if successful_requests > 0 and successful_durations:  # Ensure list is not empty
        avg_duration = sum(successful_durations) / len(successful_durations)
        calculated_timeout = avg_duration * settings.timeout_factor
        # Apply bounds
        new_timeout = max(
            settings.min_timeout_seconds,
            min(settings.max_timeout_seconds, calculated_timeout),
        )
        if (
            abs(new_timeout - current_timeout) > 0.1
        ):  # Log only if changed significantly
            logger.info(
                f"{log_prefix} Avg success duration: {avg_duration:.3f}s. Setting next timeout: {new_timeout:.1f}s (Factor: {settings.timeout_factor}x)"
            )
    elif total_requests > 0:
        # No successful requests, use fallback timeout
        new_timeout = settings.timeout_fallback_seconds
        if abs(new_timeout - current_timeout) > 0.1:  # Log only if changed
            logger.warning(
                f"{log_prefix} No successful requests in batch. Setting timeout to fallback: {new_timeout:.1f}s"
            )
    # Else: No requests made, keep timeout the same

    # --- Update context state ---
    final_concurrency = int(new_concurrency)
    final_timeout = round(new_timeout, 1)  # Round for consistency

    if final_concurrency != current_concurrency or final_timeout != round(
        current_timeout, 1
    ):
        ctx["adaptive_state"]["current_concurrency"] = final_concurrency
        ctx["adaptive_state"]["current_timeout_seconds"] = final_timeout
        logger.info(
            f"{log_prefix} Updated adaptive state: Concurrency={final_concurrency}, Timeout={final_timeout:.1f}s"
        )
    else:
        logger.info(f"{log_prefix} Adaptive state unchanged.")
