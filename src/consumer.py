"""Consume order events, deserialise Avro, and maintain a running average price."""
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringDeserializer,
)

from config import (
    BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDERS_TOPIC,
    CONSUMER_GROUP,
    SCHEMA_PATH,
)

from retry import run_with_retry
from processing import process_order
from dlq import DeadLetterQueue
from errors import PermanentError
from config import MAX_RETRIES


class RunningAverage:
    """Incremental mean over an unbounded stream.

    Stores only count and sum, so memory is O(1) regardless of how many
    messages arrive. This is the streaming equivalent of an OLAP
    pre-aggregation.
    """

    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, value: float) -> float:
        self.count += 1
        self.total += value
        return self.average

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


def main():
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(registry, schema_str)
    key_deserializer = StringDeserializer("utf_8")
    dlq = DeadLetterQueue()
    dlq_count = 0

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([ORDERS_TOPIC])

    stats = RunningAverage()
    print(f"Consuming from '{ORDERS_TOPIC}' as group '{CONSUMER_GROUP}'")
    print("Ctrl+C to stop.\n")

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
            order = avro_deserializer(msg.value(), ctx)
            key = key_deserializer(msg.key()) if msg.key() else None

            print(f"p{msg.partition()}@{msg.offset():<4} key={key:<6} "
                  f"{order['product']:<6} price={order['price']:>8.2f}")

            ok, error = run_with_retry(process_order, order)

            if ok:
                avg = stats.update(order["price"])
                print(f"      OK | count={stats.count:<4} avg={avg:.4f}")
            else:
                attempts = 1 if isinstance(error, PermanentError) else MAX_RETRIES
                dlq.send(order, error, msg, attempts)
                dlq_count += 1

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print(f"\n\nFinal: {stats.count} orders, average price {stats.average:.4f}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()