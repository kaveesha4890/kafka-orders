"""Exponential backoff with full jitter."""
import random
import time

from config import MAX_RETRIES, BASE_BACKOFF_SECONDS, MAX_BACKOFF_SECONDS
from errors import TransientError, PermanentError


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter.

    attempt is 1-based. Delay is uniformly random in [0, capped_exponential],
    which spreads retries out instead of having every failed consumer
    retry at the same instant (the 'thundering herd' problem).
    """
    capped = min(BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)), MAX_BACKOFF_SECONDS)
    return random.uniform(0, capped)


def run_with_retry(func, order: dict):
    """Execute func(order), retrying only on TransientError.

    Returns (True, None) on success.
    Returns (False, exception) when retries are exhausted or the error
    is permanent — the caller is responsible for DLQ routing.
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            func(order)
            if attempt > 1:
                print(f"      recovered on attempt {attempt}")
            return True, None

        except PermanentError as exc:
            # Fail fast. Retrying is guaranteed to produce the same result.
            print(f"      PERMANENT: {exc} — no retry")
            return False, exc

        except TransientError as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                delay = backoff_delay(attempt)
                print(
                    f"      transient (attempt {attempt}/{MAX_RETRIES}): {exc}"
                    f" — retrying in {delay:.2f}s"
                )
                time.sleep(delay)
            else:
                print(f"      exhausted {MAX_RETRIES} attempts: {exc}")

    return False, last_error