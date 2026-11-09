# Verification log

Rule (Taha, 2026-11-07): **verify every step.** A step is verified only when its
evidence is recorded here: what ran, the result, and where it was read back.

| Date | Step | Evidence | Result |
|---|---|---|---|
| 2026-11-07 | Phase 1 build | pytest in clean venv | ✅ 6 passed in 0.04s |
| 2026-11-07 | Phase 1 data generator | Ran generator (100 orders) | ✅ 5 CSVs written (customers/products/orders/payments/inventory) |
| 2026-11-07 | GitHub publish | Pushed commit `56524a0` to taha-b0t/nexus via SSH-over-443; read back README + commit list through the GitHub connector | ✅ Live at github.com/taha-b0t/nexus |
| 2026-11-07 | Full Docker stack run | Docker not installed on this VM, ~460 MB free RAM | ⏳ PENDING-HARDWARE — runs on Taha's laptop |
| 2026-11-09 | Phase 2 build — Kafka KRaft compose | `docker-compose.yml` parsed by PyYAML in test; single-node fixes verified: `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`, `KAFKA_DEFAULT_REPLICATION_FACTOR=1`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`; Connect service (`quay.io/debezium/connect:2.7`, `HEAP_OPTS`) + `kafka-init` one-shot service added | ✅ config valid; live `docker compose --profile full up` PENDING-HARDWARE |
| 2026-11-09 | Phase 2 build — topics + Avro schemas | 4 app topics + 5 CDC topics in `infra/kafka/topics.yaml`; 4 Avro schemas parse with `fastavro.schema.parse_schema`; fastavro binary round-trip of a generated order event read back identical; `create-topics.sh` passes `sh -n` and covers all 4 app topics with matching partitions | ✅ 20/20 tests (see below) |
| 2026-11-09 | Phase 2 build — producers/consumers | `src/ingestion/producers.py` (4 publishers, schema validation before send, CSV replay CLI) + `src/ingestion/consumers.py` (poll/decode, malformed JSON counted not fatal, print CLI) + `src/ingestion/schemas.py` (strict stdlib validation); 50-event in-memory round-trip: offsets 0–49 in order, keys = customer_id, per-key order preserved | ✅ 20/20 tests; live-broker round-trip PENDING-HARDWARE |
| 2026-11-09 | Phase 2 build — Debezium CDC config | `register-connector.json` validated against `init.sql`: all 5 tables in `table.include.list` match the 5 `CREATE TABLE`s; `plugin.name=pgoutput`, `slot.name=nexus_slot`, `topic.prefix=cdc`, `publication.autocreate.mode=filtered`, `topic.creation.default.replication.factor=1` (Debezium default 3 would fail on one broker); `wal_level=logical` confirmed in compose | ✅ 20/20 tests; live connector registration PENDING-HARDWARE |
| 2026-11-09 | Phase 2 test suite | `.venv/bin/python -m pytest tests/ -q` in clean venv (pytest 9.2.0, kafka-python, fastavro, pyyaml installed from `requirements.txt`) | ✅ 20 passed in 0.39s (6 Phase 1 + 14 new Phase 2) |
| 2026-11-09 | Phase 2 GitHub publish | Pushed via SSH-over-443; read back commit list through the GitHub connector | ⏳ SHA recorded below after push |
