"""Register the Order schema with Confluent Schema Registry."""
import json
import pathlib

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "schemas" / "order.avsc"
SUBJECT = "orders-value"

client = SchemaRegistryClient({"url": "http://localhost:8081"})

schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
# Validate it's well-formed JSON before shipping it
json.loads(schema_str)

schema_id = client.register_schema(SUBJECT, Schema(schema_str, schema_type="AVRO"))
print(f"Registered '{SUBJECT}' -> schema id {schema_id}")

versions = client.get_versions(SUBJECT)
print(f"Versions of '{SUBJECT}': {versions}")