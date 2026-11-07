# Data model (Gold — star schema)

- `fact_sales` — grain: one order line. Keys: order_id, customer_key, product_key, city_key, date_key. Measures: quantity, unit_price, revenue, cost, margin.
- `dim_customer` — customer_key, customer_id, city, segment, signup_date (SCD Type 2 candidate).
- `dim_product` — product_key, product_id, name, category, brand, unit_cost.
- `dim_city` — city_key, city, region.
- `fact_inventory_snapshot` — daily stock by product/city; feeds stock-out prediction.
- `agg_revenue_daily_city` — revenue by day/city for dashboards and NL analytics.

Bronze tables mirror sources 1:1 plus `_ingested_at`, `_source`, `_kafka_offset`. Silver tables carry the same names with cleaned types, deduplication on natural keys, and quality flags.
