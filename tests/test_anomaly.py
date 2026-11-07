from src.analytics.anomaly import zscore_flags
from src.data_generation.generate import gen
def test_flags_injected_outliers():
    orders=gen(seed=42, n_orders=1000)['orders']; flags=zscore_flags(orders)
    assert len(flags)>=1  # generator injects ~1% x20-price outliers
