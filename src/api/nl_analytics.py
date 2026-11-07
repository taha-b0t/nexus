"""Controlled NL analytics (Phase 7). The validator is deterministic and runs BEFORE any execution:
SELECT only, Gold tables only, LIMIT injected, no comments/semicolons (no stacked queries)."""
import re
ALLOWED_TABLES={"fact_sales","dim_customer","dim_product","dim_city","agg_revenue_daily_city"}
FORBIDDEN=re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|copy)\b", re.I)
def validate_sql(sql: str, max_rows=200) -> str:
    s=sql.strip().rstrip(";")
    if ";" in s: raise ValueError("Stacked queries are not allowed")
    if not s.lower().startswith("select"): raise ValueError("Only SELECT is allowed")
    if FORBIDDEN.search(s): raise ValueError("Forbidden keyword")
    tables=set(re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", s, re.I))
    unknown={t.lower() for t in tables} - ALLOWED_TABLES
    if unknown: raise ValueError(f"Tables not allowed: {sorted(unknown)}")
    if not re.search(r"\blimit\b", s, re.I): s=f"{s} LIMIT {max_rows}"
    return s
