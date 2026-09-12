"""Inspect the Dead Letter Queue — read and display failed orders."""
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import (
    BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, DLQ_TOPIC, DLQ_SCHEMA_PATH,
)


def main():
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        registry, DLQ_SCHEMA_PATH.read_text(encoding="utf-8")
    )

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-inspector",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([DLQ_TOPIC])

    print(f"Inspecting DLQ topic '{DLQ_TOPIC}'. Ctrl+C to stop.\n")
    seen = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            rec = deserializer(msg.value(), ctx)
            seen += 1

            ts = rec["failedAt"]
            when = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else str(ts)

            print(f"--- DLQ #{seen} (offset {msg.offset()}) ---")
            print(f"  order     : {rec['orderId']}  {rec['product']}  {rec['price']:.2f}")
            print(f"  error     : {rec['errorType']}: {rec['errorMessage']}")
            print(f"  attempts  : {rec['retryCount']}")
            print(f"  failed at : {when}")
            print(f"  origin    : {rec['sourceTopic']}[{rec['sourcePartition']}]"
                  f"@{rec['sourceOffset']}\n")

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print(f"\nInspected {seen} failed messages.")
    finally:
        consumer.close()