"""Phase 2 — Streaming verification gate (ROADMAP.md).

Runs without Docker and without a live broker: schemas, topics, compose and
Debezium config are validated as files, and the producer/consumer round-trip
runs against an in-memory broker that honours the same ordering guarantees
(per-key order, offsets). The live-broker round-trip stays PENDING-HARDWARE
until the full Docker stack runs on Taha's laptop.
"""
import json
import re
import types
from pathlib import Path

import pytest
import yaml

from src.data_generation.generate import gen
from src.ingestion import consumers, producers
from src.ingestion.schemas import cdc_topic_name, load_schema, validate_event

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_NAMES = ["order_event", "payment_event", "inventory_event", "customer_event"]


# ---------------------------------------------------------------- helpers
def _load_topics():
    with open(ROOT / "infra/kafka/topics.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_compose():
    with open(ROOT / "docker-compose.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_connector():
    with open(ROOT / "infra/debezium/register-connector.json", encoding="utf-8") as f:
        return json.load(f)


def _postgres_tables():
    sql = (ROOT / "infra/postgres/init.sql").read_text(encoding="utf-8")
    return sorted(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", sql))


class _Future:
    def get(self, timeout=None):
        return None


class MemoryBroker:
    """In-memory stand-in for Kafka: one ordered log per topic, offsets assigned
    in arrival order. Same contract the tests rely on: per-key order preserved."""

    def __init__(self):
        self.log = {}

    def append(self, topic, key, value):
        records = self.log.setdefault(topic, [])
        records.append(types.SimpleNamespace(
            topic=topic, partition=0, offset=len(records),
            key=str(key), value=json.dumps(value)))
        return _Future()

    def records(self, topic):
        return list(self.log.get(topic, []))


class MemoryProducer:
    def __init__(self, broker):
        self.broker = broker
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        return self.broker.append(topic, key, value)

    def flush(self):
        pass


class MemoryConsumer:
    """Polls the MemoryBroker like KafkaConsumer.poll: {partition: [records]}."""

    def __init__(self, broker, topics):
        self.broker = broker
        self.topics = list(topics)
        self.positions = {t: 0 for t in self.topics}
        self.closed = False

    def poll(self, timeout_ms=0, max_records=100):
        out, taken = {}, 0
        for topic in self.topics:
            records = self.broker.records(topic)
            start = self.positions[topic]
            batch = records[start:start + max(0, max_records - taken)]
            if batch:
                # Key mirrors kafka-python's poll() shape: {TopicPartition: [records]}.
                # A plain tuple is hashable; the consumer only iterates the values.
                out[(topic, 0)] = batch
                self.positions[topic] = start + len(batch)
                taken += len(batch)
        return out

    def close(self):
        self.closed = True


# ---------------------------------------------------------------- schemas
def test_avro_schemas_parse():
    for name in SCHEMA_NAMES:
        schema = load_schema(name)
        assert schema["type"] == "record" and schema["namespace"] == "nexus"
        assert {f["name"] for f in schema["fields"]}
    fastavro = pytest.importorskip("fastavro", reason="fastavro not installed")
    for name in SCHEMA_NAMES:
        with open(ROOT / "infra/kafka/schemas" / f"{name}.json", encoding="utf-8") as f:
            fastavro.schema.parse_schema(json.load(f))  # raises on invalid Avro


def test_event_validation_accepts_and_rejects():
    schema = load_schema("order_event")
    good = {"order_id": 1, "customer_id": 2, "product_id": 3, "city": "Rabat",
            "quantity": 2, "unit_price": 1299.0, "event_time": "2026-01-05T10:00:00+00:00"}
    assert validate_event(schema, good) == []
    assert any("missing field: quantity" in p
               for p in validate_event(schema, {k: v for k, v in good.items() if k != "quantity"}))
    assert any("unit_price" in p
               for p in validate_event(schema, {**good, "unit_price": "1299.00"}))
    assert any("unknown field: discount" in p
               for p in validate_event(schema, {**good, "discount": 5}))
    # bool is an int subclass in Python; Avro long must reject it
    assert any("customer_id" in p
               for p in validate_event(schema, {**good, "customer_id": True}))


def test_generator_rows_match_event_schemas():
    data = gen(seed=3, n_customers=20, n_products=10, n_orders=50)
    schemas = {"orders": ("order_event", producers.order_to_event),
               "payments": ("payment_event", producers.payment_to_event),
               "inventory": ("inventory_event", producers.inventory_to_event),
               "customers": ("customer_event", producers.customer_to_event)}
    for table, (schema_name, to_event) in schemas.items():
        schema = load_schema(schema_name)
        for row in data[table]:
            assert validate_event(schema, to_event(row)) == [], (table, row)


# ---------------------------------------------------------------- topics
def test_topics_yaml_valid():
    cfg = _load_topics()
    app = cfg["topics"]
    assert len(app) == 4 and len({t["name"] for t in app}) == 4
    for t in app:
        assert t["partitions"] >= 1 and t["replication_factor"] == 1
        assert t["retention_ms"] == 604800000
        schema = load_schema(Path(t["value_schema"]).stem)
        assert t["key"] in {f["name"] for f in schema["fields"]}
        assert (ROOT / "infra/kafka" / t["value_schema"]).exists()
    cdc = cfg["cdc_topics"]
    assert {t["name"] for t in cdc} == {f"cdc.public.{t}" for t in _postgres_tables()}
    assert all(t["partitions"] == 1 and t["replication_factor"] == 1 for t in cdc)


def test_create_topics_script_covers_topics_yaml():
    script = (ROOT / "infra/kafka/create-topics.sh").read_text(encoding="utf-8")
    for t in _load_topics()["topics"]:
        assert f"create {t['name']} {t['partitions']}" in script


# ---------------------------------------------------------------- compose
def test_compose_kafka_kraft_single_node():
    compose = _load_compose()
    kafka = compose["services"]["kafka"]
    env = kafka["environment"]
    assert env["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert env["CLUSTER_ID"] == "nexus-cluster-1"
    assert env["KAFKA_CONTROLLER_QUORUM_VOTERS"] == "1@kafka:9093"
    # Single broker cannot satisfy the default RF=3 for internal topics
    assert str(env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"]) == "1"
    assert str(env["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"]) == "1"
    assert str(env["KAFKA_DEFAULT_REPLICATION_FACTOR"]) == "1"
    assert str(env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"]).lower() == "false"
    assert "healthcheck" in kafka and "kafkadata:/var/lib/kafka/data" in kafka["volumes"]
    assert "full" in kafka["profiles"]

    connect = compose["services"]["connect"]
    assert "full" in connect["profiles"]
    assert connect["environment"]["BOOTSTRAP_SERVERS"] == "kafka:9092"
    assert connect["environment"]["GROUP_ID"] == "nexus-connect"
    for key in ("CONFIG_STORAGE_REPLICATION_FACTOR",
                "OFFSET_STORAGE_REPLICATION_FACTOR",
                "STATUS_STORAGE_REPLICATION_FACTOR"):
        assert str(connect["environment"][key]) == "1"
    assert "HEAP_OPTS" in connect["environment"]

    init = compose["services"]["kafka-init"]
    assert "full" in init["profiles"]
    assert "create-topics.sh" in " ".join(init["command"])

    pg_command = " ".join(compose["services"]["postgres"]["command"])
    assert "wal_level=logical" in pg_command


# ---------------------------------------------------------------- debezium
def test_debezium_config_matches_postgres_schema():
    cfg = _load_connector()["config"]
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["database.hostname"] == "postgres" and str(cfg["database.port"]) == "5432"
    assert cfg["database.dbname"] == "nexus" and cfg["database.user"] == "nexus"
    assert cfg["plugin.name"] == "pgoutput"
    assert re.fullmatch(r"[a-z0-9_]{1,63}", cfg["slot.name"])
    assert cfg["topic.prefix"] == "cdc"
    assert cfg["publication.autocreate.mode"] == "filtered"
    assert cfg["snapshot.mode"] == "initial"
    # Debezium's default topic-creation RF is 3; a single broker cannot do that
    assert cfg["topic.creation.default.replication.factor"] == 1
    assert cfg["topic.creation.default.partitions"] == 1
    included = sorted(t.split(".", 1)[1] for t in cfg["table.include.list"].split(","))
    assert included == _postgres_tables(), f"CDC tables {included} != Postgres tables {_postgres_tables()}"


def test_cdc_topic_names():
    for table in _postgres_tables():
        name = cdc_topic_name("cdc", "public", table)
        assert name in {t["name"] for t in _load_topics()["cdc_topics"]}


# ---------------------------------------------------------------- producer / consumer
def test_producer_sends_to_correct_topic_and_key():
    class RecordingProducer:
        def __init__(self):
            self.calls = []

        def send(self, topic, key=None, value=None):
            self.calls.append((topic, key, value))
            return _Future()

        def flush(self):
            pass

    rec = RecordingProducer()
    producers.publish_order(rec, {"order_id": 1, "customer_id": 7, "product_id": 2,
                                  "city": "Fes", "quantity": 1, "unit_price": 99.5,
                                  "created_at": "2026-01-01T00:00:00+00:00"})
    assert rec.calls[0][0] == "nexus.orders" and rec.calls[0][1] == 7
    producers.publish_payment(rec, {"payment_id": 1, "order_id": 1, "amount": 99.5,
                                    "method": "card", "paid_at": "2026-01-01T00:00:00+00:00"})
    assert rec.calls[1][0] == "nexus.payments" and rec.calls[1][1] == 1


def test_producer_rejects_invalid_event_before_send():
    class RecordingProducer:
        def __init__(self):
            self.calls = []

        def send(self, topic, key=None, value=None):
            self.calls.append((topic, key, value))
            return _Future()

    rec = RecordingProducer()
    bad = {"order_id": 1, "customer_id": 7, "product_id": 2, "city": "Fes",
           "quantity": "two", "unit_price": 99.5, "created_at": "2026-01-01T00:00:00+00:00"}
    with pytest.raises(ValueError):
        producers.publish_order(rec, bad)
    assert rec.calls == []


def test_producer_consumer_roundtrip_memory_broker():
    broker = MemoryBroker()
    producer = MemoryProducer(broker)
    orders = gen(seed=11, n_customers=10, n_products=5, n_orders=50)["orders"]
    for order in orders:
        producers.publish_order(producer, order)
    assert len(producer.sent) == 50

    consumer = MemoryConsumer(broker, ["nexus.orders"])
    events, skipped = consumers.consume_batch(consumer, timeout_ms=100, max_records=100)
    assert skipped == 0 and len(events) == 50
    schema = load_schema("order_event")
    assert [e["offset"] for e in events] == list(range(50))  # log order preserved
    assert [e["key"] for e in events] == [str(o["customer_id"]) for o in orders]
    assert all(validate_event(schema, e["value"]) == [] for e in events)
    # Per-key order: each customer's events arrive in the order they were sent
    seen = {}
    for e in events:
        seen.setdefault(e["key"], []).append(e["value"]["order_id"])
    for key, ids in seen.items():
        assert ids == sorted(ids)


def test_consumer_skips_malformed_json():
    broker = MemoryBroker()
    broker.append("nexus.orders", "1", {"order_id": 1, "customer_id": 1, "product_id": 1,
                                        "city": "Rabat", "quantity": 1, "unit_price": 10.0,
                                        "event_time": "2026-01-01T00:00:00+00:00"})
    # Inject a record whose value is not valid JSON at all
    broker.log["nexus.orders"].append(types.SimpleNamespace(
        topic="nexus.orders", partition=0, offset=1, key="2", value="{not json"))
    consumer = MemoryConsumer(broker, ["nexus.orders"])
    events, skipped = consumers.consume_batch(consumer, timeout_ms=100, max_records=10)
    assert len(events) == 1 and skipped == 1


def test_run_consumer_counts_and_closes():
    broker = MemoryBroker()
    producer = MemoryProducer(broker)
    for order in gen(seed=5, n_customers=5, n_products=3, n_orders=5)["orders"]:
        producers.publish_order(producer, order)
    consumer = MemoryConsumer(broker, ["nexus.orders"])
    seen = []
    handled, skipped = consumers.run_consumer(consumer, seen.append, max_records=5)
    assert (handled, skipped) == (5, 0) and len(seen) == 5 and consumer.closed


def test_make_producer_consumer_require_kafka_python(monkeypatch):
    monkeypatch.setattr(producers, "KafkaProducer", None)
    monkeypatch.setattr(consumers, "KafkaConsumer", None)
    with pytest.raises(RuntimeError):
        producers.make_producer()
    with pytest.raises(RuntimeError):
        consumers.make_consumer()
