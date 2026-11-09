# Debezium CDC — Postgres → Kafka (Phase 2)

## What it does
Streams every committed row change (insert / update / delete) from the five
operational tables in `infra/postgres/init.sql` into Kafka topics named
`<topic.prefix>.<schema>.<table>` — here `cdc.public.orders`, etc.
Analytics reads the log instead of re-querying the operational DB.

## Register the connector
```bash
docker compose --profile full up -d postgres kafka connect
curl -X POST localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @infra/debezium/register-connector.json
curl localhost:8083/connectors/nexus-postgres-cdc/status   # should say RUNNING
```

## Why each non-obvious setting exists
- `plugin.name: pgoutput` — Postgres 10+ built-in logical decoding plugin; no
  extra extension to install. Requires `wal_level=logical` (set in
  `docker-compose.yml` for the postgres service).
- `slot.name: nexus_slot` — the logical replication slot that pins WAL segments
  until Debezium has read them. Lowercase letters/digits/underscore only.
- `publication.name: nexus_publication` + `publication.autocreate.mode: filtered`
  — the publication contains only the five tables in `table.include.list`,
  not every table in the database.
- `snapshot.mode: initial` — on first start, Debezium snapshots current rows,
  then streams changes. A row update after that appears as one event.
- `topic.creation.default.replication.factor: 1` — Debezium's default is 3;
  a single-broker dev cluster cannot create a 3-replica topic, so this must be 1.
- `decimal.handling.mode: string` — Postgres `NUMERIC(10,2)` arrives as a plain
  JSON string (`"1299.00"`) instead of Debezium's precise base64 struct, so
  downstream Python/Spark code parses it with `Decimal(...)` directly.
- `include.schema.changes: false` — schema-change events are noise for this
  pipeline; table DDL is tracked in Git, not in Kafka.

## Preconditions (validated by tests/test_streaming.py)
- Every table in `table.include.list` exists in `infra/postgres/init.sql`.
- `wal_level=logical` is set for postgres in `docker-compose.yml`.
- The `nexus` DB user can create a publication and a replication slot
  (it is the `POSTGRES_USER`, i.e. a superuser in the dev container).
