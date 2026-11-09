"""Kafka producers (Phase 2). Requires kafka-python; import is guarded so the
rest of the repo (and Phase 1 tests) still works without a broker installed.

Keying rule (see infra/kafka/topics.yaml): a customer's events must stay ordered,
so every event is keyed by the id that defines its ordering unit.
"""
import csv
import json
import time
from pathlib import Path

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

from .schemas import load_schema, validate_event

TOPIC_ORDERS = "nexus.orders"
TOPIC_PAYMENTS = "nexus.payments"
TOPIC_INVENTORY = "nexus.inventory"
TOPIC_CUSTOMERS = "nexus.customers"

_SCHEMAS = {}


def _schema(name):
    if name not in _SCHEMAS:
        _SCHEMAS[name] = load_schema(name)
    return _SCHEMAS[name]


def make_producer(bootstrap="localhost:9092"):
    if KafkaProducer is None:
        raise RuntimeError("Install kafka-python for Phase 2 (pip install -r requirements.txt)")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: str(k).encode(),
        acks="all",  # wait for the leader to confirm: no silent loss on broker restart
        retries=3,
    )


def _send_validated(producer, topic, key, event, schema_name):
    problems = validate_event(_schema(schema_name), event)
    if problems:
        raise ValueError(f"event fails {schema_name} schema: {problems}")
    return producer.send(topic, key=key, value=event)


def order_to_event(row):
    """Map one generated order (CSV row / generator dict) onto the Avro event."""
    return {
        "order_id": int(row["order_id"]),
        "customer_id": int(row["customer_id"]),
        "product_id": int(row["product_id"]),
        "city": str(row["city"]),
        "quantity": int(row["quantity"]),
        "unit_price": float(row["unit_price"]),
        "event_time": str(row["created_at"]),
    }


def payment_to_event(row):
    return {
        "payment_id": int(row["payment_id"]),
        "order_id": int(row["order_id"]),
        "amount": float(row["amount"]),
        "method": str(row["method"]),
        "paid_at": str(row["paid_at"]),
    }


def inventory_to_event(row):
    return {
        "product_id": int(row["product_id"]),
        "city": str(row["city"]),
        "stock": int(row["stock"]),
        "updated_at": str(row.get("updated_at", "")),
    }


def customer_to_event(row):
    return {
        "customer_id": int(row["customer_id"]),
        "full_name": str(row["full_name"]),
        "city": str(row["city"]),
        "segment": str(row["segment"]),
        "created_at": str(row.get("created_at", "")),
    }


def publish_order(producer, order):
    # Key by customer_id: all events for one customer stay in one partition, in order.
    return _send_validated(producer, TOPIC_ORDERS, order["customer_id"],
                           order_to_event(order), "order_event")


def publish_payment(producer, payment):
    return _send_validated(producer, TOPIC_PAYMENTS, payment["order_id"],
                           payment_to_event(payment), "payment_event")


def publish_inventory(producer, item):
    return _send_validated(producer, TOPIC_INVENTORY, item["product_id"],
                           inventory_to_event(item), "inventory_event")


def publish_customer(producer, customer):
    return _send_validated(producer, TOPIC_CUSTOMERS, customer["customer_id"],
                           customer_to_event(customer), "customer_event")


def stream_csv(producer, csv_path, publish, rate_per_sec=0, limit=None):
    """Replay a generated CSV through `publish`, one row at a time.

    rate_per_sec=0 means as fast as the broker accepts; otherwise sleeps between
    sends so a laptop broker is not flooded. Returns the number of rows sent.
    """
    sent = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            publish(producer, row)
            sent += 1
            if limit is not None and sent >= limit:
                break
            if rate_per_sec:
                time.sleep(1.0 / rate_per_sec)
    producer.flush()
    return sent


_PUBLISHERS = {
    TOPIC_ORDERS: publish_order,
    TOPIC_PAYMENTS: publish_payment,
    TOPIC_INVENTORY: publish_inventory,
    TOPIC_CUSTOMERS: publish_customer,
}


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Replay generated CSVs into Kafka topics.")
    ap.add_argument("--csv", required=True, help="CSV file to replay, e.g. data/synthetic/orders.csv")
    ap.add_argument("--topic", default=TOPIC_ORDERS, choices=list(_PUBLISHERS))
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, default=0, help="events per second, 0 = unlimited")
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    producer = make_producer(a.bootstrap)
    sent = stream_csv(producer, Path(a.csv), _PUBLISHERS[a.topic],
                      rate_per_sec=a.rate, limit=a.limit)
    print(f"sent {sent} events to {a.topic} via {a.bootstrap}")


if __name__ == "__main__":
    main()
