"""Produce randomised order events to Kafka, serialised with Avro."""
import argparse
import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)

from config import (
    BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDERS_TOPIC,
    SCHEMA_PATH,
    PRODUCTS,
)


def build_order(sequence: int) -> dict:
    """Create one randomised order matching the Avro schema."""
    return {
        "orderId": str(1000 + sequence),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    """Called once per message when the broker acknowledges or rejects it."""
    if err is not None:
        print(f"  DELIVERY FAILED: {err}")
        return
    print(
        f"  delivered -> partition {msg.partition()} offset {msg.offset()}"
    )


def main():
    parser = argparse.ArgumentParser(description="Order event producer")
    parser.add_argument("--count", type=int, default=20,
                        help="number of orders to send")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="seconds between messages")
    args = parser.parse_args()

    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(registry, schema_str)
    key_serializer = StringSerializer("utf_8")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 10,
        "client.id": f"order-producer-{uuid.uuid4().hex[:6]}",
    })

    print(f"Producing {args.count} orders to '{ORDERS_TOPIC}'...\n")

    for i in range(args.count):
        order = build_order(i)
        ctx = SerializationContext(ORDERS_TOPIC, MessageField.VALUE)

        producer.produce(
            topic=ORDERS_TOPIC,
            key=key_serializer(order["orderId"]),
            value=avro_serializer(order, ctx),
            on_delivery=delivery_report,
        )
        print(f"[{i + 1}/{args.count}] {order}")

        producer.poll(0)
        time.sleep(args.delay)

    remaining = producer.flush(10)
    if remaining:
        print(f"WARNING: {remaining} messages undelivered")
    else:
        print("\nAll messages delivered.")


if __name__ == "__main__":
    main()