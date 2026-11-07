"""Structured Streaming Kafka->Bronze (Phase 3 scaffold). Checkpointing + watermark = resumable, late-event tolerant."""
STREAMING_NOTES="checkpointLocation=data/checkpoints/kafka_bronze; watermark=10 minutes; outputMode=append; DLQ for unparseable JSON"
