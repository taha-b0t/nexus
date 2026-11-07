import pytest
from src.api.nl_analytics import validate_sql
def test_ok(): assert 'LIMIT' in validate_sql('SELECT city, SUM(revenue) FROM fact_sales GROUP BY city')
def test_blocks_write():
    with pytest.raises(ValueError): validate_sql('DELETE FROM fact_sales')
def test_blocks_unknown_table():
    with pytest.raises(ValueError): validate_sql('SELECT * FROM payments')
