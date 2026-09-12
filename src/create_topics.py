"""Create the orders and DLQ topics with explicit configuration."""
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP_SERVERS, ORDERS_TOPIC, DLQ_TOPIC

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

topics = [
    NewTopic(ORDERS_TOPIC, num_partitions=3, replication_factor=1),
    NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=1),
]

for topic, future in admin.create_topics(topics).items():
    try:
        future.result()
        print(f"Created topic '{topic}'")
    except Exception as exc:
        print(f"Topic '{topic}': {exc}")