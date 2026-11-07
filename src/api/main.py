"""NEXUS FastAPI serving layer (Phase 7 scaffold, Phase 1 runnable)."""
from fastapi import FastAPI, HTTPException
from src.api.nl_analytics import validate_sql
app=FastAPI(title="NEXUS API", version="0.1.0")
@app.get("/health")
def health(): return {"status":"ok","service":"nexus"}
@app.get("/metrics/summary")
def summary(): return {"note":"Phase 1: aggregates wire to Gold via Trino in Phase 5","tables":["fact_sales","agg_revenue_daily_city"]}
@app.post("/analytics/validate")
def validate(payload: dict):
    try: return {"validated_sql": validate_sql(payload.get("sql",""))}
    except ValueError as e: raise HTTPException(status_code=400, detail=str(e))
