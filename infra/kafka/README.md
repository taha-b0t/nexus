# Kafka — streaming layer (Phase 2)

## Topics
Defined in `topics.yaml`. Application topics (`nexus.*`) are created by
`create-topics.sh`, run once by the `kafka-init` service in
`docker-compose.yml` (`--profile full`). CDC topics (`cdc.public.*`) are
created automatically by the Debezium connector.

| Topic | Partitions | Key | Why |
|---|---|---|---|
| `nexus.orders` | 6 | `customer_id` | Highest volume; key keeps one customer's events ordered |
| `nexus.payments` | 3 | `order_id` | Joins to its order by key |
| `nexus.inventory` | 3 | `product_id` | One partition stream per product |
| `nexus.customers` | 3 | `customer_id` | Customer profile changes |
| `cdc.public.*` | 1 | table PK | One ordered change log per table |

Retention is 7 days (`retention.ms=604800000`); replication factor is 1
(single-broker dev cluster — never ship RF=1).

## Schemas
Avro records in `schemas/`, one per event type. Producers validate every event
against its schema before sending (`src/ingestion/schemas.py`); a bad event
raises `ValueError` instead of poisoning consumers. Schemas are plain
primitives only (no unions) in v1, on purpose: evolution (adding an optional
field) is a Phase 4 discussion.

## Produce / consume
```bash
docker compose --profile full up -d kafka        # broker on localhost:9092
make generate                                    # data/synthetic/*.csv
python -m src.ingestion.producers --csv data/synthetic/orders.csv --rate 10
python -m src.ingestion.consumers  # (see module docstring; Phase 3 wires it to Bronze)
```

## Full-stack run (needs Docker — PENDING-HARDWARE on this VM)
```bash
docker compose --profile full up -d   # postgres + minio + kafka + connect + kafka-init
```

## Single-node KRaft notes
The broker runs `broker,controller` in one process (no ZooKeeper). The
internal topics default to 3 replicas, which one broker cannot satisfy, so
`docker-compose.yml` pins `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`,
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` and
`KAFKA_DEFAULT_REPLICATION_FACTOR` to 1, and disables auto topic creation
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`) so stray typos never create topics.
