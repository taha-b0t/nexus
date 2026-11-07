"""Kafka producers (Phase 2). Requires kafka-python; import is guarded so Phase 1 tests pass without Kafka."""
import json
try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer=None

def make_producer(bootstrap="localhost:9092"):
    if KafkaProducer is None: raise RuntimeError("Install kafka-python for Phase 2")
    return KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode(), key_serializer=lambda k: str(k).encode())

def publish_order(producer, order):
    # Key by customer_id: all events for one customer stay in one partition, in order.
    return producer.send("nexus.orders", key=order["customer_id"], value=order)
