from src.data_generation.generate import gen, CITIES
def test_gen_counts():
    d=gen(seed=1, n_customers=10, n_products=5, n_orders=50)
    assert len(d['customers'])==10 and len(d['orders'])==50
    assert all(o['city'] in CITIES for o in d['orders'])
def test_gen_deterministic():
    assert gen(seed=7, n_orders=20)['orders']==gen(seed=7, n_orders=20)['orders']
