"""Event-schema loading and validation (Phase 2, stdlib only).

Avro schemas live in infra/kafka/schemas/*.json. Validation is strict on purpose:
an event must carry exactly the fields its schema declares, with matching types.
That is what keeps a bad producer from silently breaking every consumer.
"""
import json
from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "infra" / "kafka" / "schemas"

# Avro primitive -> Python type check. "long"/"int" accept int (not bool);
# "double"/"float" accept int or float (JSON has no int/float split for numbers).
_AVRO_CHECKS = {
    "long": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "double": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "boolean": lambda v: isinstance(v, bool),
}


def load_schema(name):
    """Load an Avro schema by file stem, e.g. load_schema('order_event')."""
    path = SCHEMA_DIR / f"{name}.json"
    with open(path, encoding="utf-8") as f:
        schema = json.load(f)
    _check_schema_shape(schema, path)
    return schema


def _check_schema_shape(schema, path):
    if not isinstance(schema, dict) or schema.get("type") != "record":
        raise ValueError(f"{path}: Avro schema must be a record, got {schema.get('type')!r}")
    if not schema.get("name"):
        raise ValueError(f"{path}: Avro record needs a name")
    fields = schema.get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError(f"{path}: Avro record needs a non-empty fields list")
    seen = set()
    for field in fields:
        fname = field.get("name")
        ftype = field.get("type")
        if not fname or fname in seen:
            raise ValueError(f"{path}: bad or duplicate field name {fname!r}")
        seen.add(fname)
        # Union types (e.g. ["null", "string"]) are not used in NEXUS v1; reject them
        # loudly instead of guessing which branch applies.
        if ftype not in _AVRO_CHECKS:
            raise ValueError(f"{path}: unsupported field type {ftype!r} for {fname!r} "
                             "(NEXUS v1 schemas use plain primitives only)")


def validate_event(schema, event):
    """Return a list of problems; empty list = event is valid for the schema."""
    problems = []
    if not isinstance(event, dict):
        return ["event must be a JSON object"]
    fields = {f["name"]: f["type"] for f in schema["fields"]}
    for name in fields:
        if name not in event:
            problems.append(f"missing field: {name}")
        elif not _AVRO_CHECKS[fields[name]](event[name]):
            problems.append(
                f"field {name!r}: expected {fields[name]}, got {type(event[name]).__name__} "
                f"({event[name]!r})")
    for name in event:
        if name not in fields:
            problems.append(f"unknown field: {name}")
    return problems


def is_valid(schema, event):
    return not validate_event(schema, event)


def cdc_topic_name(topic_prefix, schema_name, table_name):
    """Debezium topic naming: <topic.prefix>.<schema>.<table> (Debezium docs)."""
    return f"{topic_prefix}.{schema_name}.{table_name}"
