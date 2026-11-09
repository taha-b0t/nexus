"""Kafka consumers (Phase 2). Requires kafka-python; import is guarded so the
rest of the repo still works without a broker installed.

Design notes:
- `auto_offset_reset="earliest"` so a fresh consumer group replays the retained
  log (Kafka is a log, not a queue) instead of silently skipping history.
- Malformed records never crash the loop: they are counted as skipped and the
  consumer moves on. Phase 3 routes them to a dead-letter topic; here the count
  is the observable signal.
- `consume_batch` is the testable core; `run_consumer` is the long-running loop.
"""
import json

try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None


def make_consumer(bootstrap="localhost:9092", group_id="nexus-consumers",
                  topics=("nexus.orders",), auto_offset_reset="earliest"):
    if KafkaConsumer is None:
        raise RuntimeError("Install kafka-python for Phase 2 (pip install -r requirements.txt)")
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
        key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
    )
    return consumer


def consume_batch(consumer, timeout_ms=1000, max_records=100):
    """Poll once and decode. Returns (events, skipped).

    events: list of {"topic", "partition", "offset", "key", "value"} with value
    already parsed from JSON. skipped: number of records whose value was not
    valid JSON (counted, never fatal).
    """
    events, skipped = [], 0
    raw = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
    for _partition, records in raw.items():
        for record in records:
            try:
                value = json.loads(record.value) if record.value is not None else None
            except (json.JSONDecodeError, TypeError):
                skipped += 1
                continue
            events.append({
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "key": record.key,
                "value": value,
            })
    return events, skipped


def run_consumer(consumer, handler, max_records=None, timeout_ms=1000):
    """Long-running loop: poll, hand each decoded event to `handler(event)`.

    Stops after max_records events (None = run forever, Ctrl-C to stop).
    Returns (handled, skipped).
    """
    handled, skipped = 0, 0
    try:
        while max_records is None or handled < max_records:
            events, bad = consume_batch(consumer, timeout_ms=timeout_ms,
                                        max_records=100)
            skipped += bad
            for event in events:
                handler(event)
                handled += 1
                if max_records is not None and handled >= max_records:
                    break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return handled, skipped


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Consume Kafka topics and print events.")
    ap.add_argument("--topics", nargs="+", default=["nexus.orders"])
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group-id", default="nexus-print")
    ap.add_argument("--max-records", type=int, default=10)
    a = ap.parse_args()
    consumer = make_consumer(a.bootstrap, a.group_id, tuple(a.topics))
    handled, skipped = run_consumer(consumer, lambda e: print(e), max_records=a.max_records)
    print(f"handled {handled} events, skipped {skipped} malformed records")


if __name__ == "__main__":
    main()
