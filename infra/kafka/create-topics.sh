#!/bin/sh
# Create NEXUS application topics (Phase 2). Idempotent: --if-not-exists.
# Runs inside the kafka-init one-shot service (docker-compose.yml, profile: full).
# Usage on a running broker: sh infra/kafka/create-topics.sh [bootstrap-server]
set -e
BOOTSTRAP="${1:-kafka:9092}"

create() {
  # $1 = topic, $2 = partitions
  kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms=604800000
}

# Wait for the broker to accept connections (up to ~60s).
for i in $(seq 1 30); do
  if kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

create nexus.orders 6
create nexus.payments 3
create nexus.inventory 3
create nexus.customers 3

echo "topics ready:"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | grep '^nexus\.' || true
