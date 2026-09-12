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