"""Central configuration for the Kafka order pipeline."""
import os
import pathlib

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
CONSUMER_GROUP = "order-aggregator"

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "schemas" / "order.avsc"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

# Probability a message hits a simulated transient fault (0.0 - 1.0)
TRANSIENT_FAILURE_RATE = float(os.getenv("TRANSIENT_FAILURE_RATE", "0.3"))

# Retry policy
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0