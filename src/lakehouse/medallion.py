"""Medallion helpers (Phase 4). Bronze is append-only raw; Silver is cleaned; Gold is modelled. Iceberg merge-into makes CDC upserts idempotent."""
LAYERS={"bronze":"s3a://nexus-bronze/","silver":"s3a://nexus-silver/","gold":"s3a://nexus-gold/"}
ICEBERG_MERGE_SQL="MERGE INTO gold.fact_sales t USING staging s ON t.order_id=s.order_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
