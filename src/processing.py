"""Business logic for processing a single order, with fault injection."""
import random

from errors import TransientError, PermanentError
from config import TRANSIENT_FAILURE_RATE, PRODUCTS


def validate(order: dict) -> None:
    """Business rules. Violations are permanent — retrying cannot help."""
    if order["price"] <= 0:
        raise PermanentError(f"non-positive price: {order['price']}")
    if order["price"] > 10_000:
        raise PermanentError(f"price exceeds sanity limit: {order['price']}")
    if order["product"] not in PRODUCTS:
        raise PermanentError(f"unknown product: {order['product']!r}")
    if not order["orderId"].isdigit():
        raise PermanentError(f"malformed orderId: {order['orderId']!r}")


def process_order(order: dict) -> None:
    """Simulates writing the order to a downstream system.

    In production this would be a database insert or an HTTP call; the
    random failure stands in for the real-world faults those incur.
    """
    validate(order)

    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientError("downstream service unavailable (simulated)")