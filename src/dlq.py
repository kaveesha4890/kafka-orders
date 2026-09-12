"""Routes permanently failed messages to the Dead Letter Queue."""
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext, MessageField, StringSerializer,
)

from config import (
    BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, DLQ_TOPIC,
    DLQ_SCHEMA_PATH, MAX_RETRIES,
)


class DeadLetterQueue:
    """Publishes failed records to the DLQ topic, Avro-serialised."""

    def __init__(self):
        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self._serializer = AvroSerializer(
            registry, DLQ_SCHEMA_PATH.read_text(encoding="utf-8")
        )
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "dlq-producer",
        })

    def send(self, order: dict, error: Exception, msg, retries: int) -> None:
        record = {
            "orderId": order.get("orderId", "UNKNOWN"),
            "product": order.get("product", "UNKNOWN"),
            "price": float(order.get("price", 0.0)),
            "errorType": type(error).__name__,
            "errorMessage": str(error),
            "retryCount": retries,
            "failedAt": datetime.now(timezone.utc),
            "sourceTopic": msg.topic(),
            "sourcePartition": msg.partition(),
            "sourceOffset": msg.offset(),
        }

        ctx = SerializationContext(DLQ_TOPIC, MessageField.VALUE)
        self._producer.produce(
            topic=DLQ_TOPIC,
            key=self._key_serializer(record["orderId"]),
            value=self._serializer(record, ctx),
            on_delivery=self._on_delivery,
        )
        # Block until the DLQ write is confirmed. We must not commit the
        # source offset until the message is safely in the DLQ, or a crash
        # here would lose it entirely.
        self._producer.flush(5)

    @staticmethod
    def _on_delivery(err, msg):
        if err:
            print(f"      DLQ WRITE FAILED: {err}")
        else:
            print(f"      -> DLQ partition {msg.partition()} offset {msg.offset()}")

    def close(self):
        self._producer.flush(10)