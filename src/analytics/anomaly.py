"""Anomaly detection (Phase 7) — pure-python z-score baseline + scikit-learn IsolationForest when available.
An anomaly is an alert for review, never proof of fraud."""
def zscore_flags(orders, threshold=4.0):
    amounts=[o["quantity"]*o["unit_price"] for o in orders]
    mean=sum(amounts)/max(len(amounts),1); var=sum((x-mean)**2 for x in amounts)/max(len(amounts),1); sd=var**0.5 or 1.0
    return [o["order_id"] for o in orders if abs((o["quantity"]*o["unit_price"]-mean)/sd) > threshold]
