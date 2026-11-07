"""NEXUS synthetic data generator — Moroccan e-commerce (Phase 1, stdlib only).

Deterministic with --seed so benchmarks and tests are reproducible.
Usage: python -m src.data_generation.generate --orders 1000 --out data/synthetic
"""
import argparse, csv, random
from pathlib import Path
from datetime import datetime, timedelta, timezone

CITIES = ["Casablanca","Rabat","Marrakech","Agadir","Tanger","Fes","Meknes","Oujda","Kenitra","Tetouan"]
FIRST = ["Yassine","Salma","Omar","Khadija","Mehdi","Sara","Hamza","Nour","Amine","Lina","Youssef","Meryem"]
LAST = ["Alaoui","Benali","El Fassi","Idrissi","Amrani","Bennis","Chraibi","Tazi","El Amrani","Rami"]
CATEGORIES = {"Electronics":(800,900,12000),"Fashion":(300,120,2500),"Home":(500,150,4000),"Beauty":(350,60,900),"Sports":(450,100,2200),"Grocery":(80,20,400)}

def gen(seed=42, n_customers=200, n_products=80, n_orders=1000):
    rng = random.Random(seed)
    customers=[{"customer_id":i+1,"full_name":f"{rng.choice(FIRST)} {rng.choice(LAST)}","city":rng.choice(CITIES),"segment":rng.choice(["standard","standard","premium","vip"])} for i in range(n_customers)]
    products=[]
    cats=list(CATEGORIES.items())
    for i in range(n_products):
        cat,(cost,lo,hi)=cats[i % len(cats)]
        price=round(rng.uniform(lo,hi),2)
        products.append({"product_id":i+1,"name":f"{cat} Item {i+1}","category":cat,"unit_price":price,"unit_cost":round(price*0.62,2)})
    orders=[]; payments=[]; base=datetime(2026,1,1,tzinfo=timezone.utc)
    for i in range(n_orders):
        c=rng.choice(customers); p=rng.choice(products); qty=rng.choice([1,1,1,2,2,3,5])
        # ~1% injected anomalies for the Phase 7 detector to find
        price=p["unit_price"]*20 if rng.random()<0.01 else p["unit_price"]
        ts=base+timedelta(minutes=rng.randint(0,60*24*90))
        orders.append({"order_id":i+1,"customer_id":c["customer_id"],"product_id":p["product_id"],"city":c["city"],"quantity":qty,"unit_price":round(price,2),"status":"paid","created_at":ts.isoformat()})
        payments.append({"payment_id":i+1,"order_id":i+1,"amount":round(qty*price,2),"method":rng.choice(["card","cash_on_delivery","transfer"]),"paid_at":ts.isoformat()})
    inventory=[{"product_id":p["product_id"],"city":city,"stock":rng.randint(0,120)} for p in products for city in CITIES[:5]]
    return {"customers":customers,"products":products,"orders":orders,"payments":payments,"inventory":inventory}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--seed",type=int,default=42); ap.add_argument("--customers",type=int,default=200); ap.add_argument("--products",type=int,default=80); ap.add_argument("--orders",type=int,default=1000); ap.add_argument("--out",default="data/synthetic")
    a=ap.parse_args(); data=gen(a.seed,a.customers,a.products,a.orders); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    for name,rows in data.items():
        with open(out/f"{name}.csv","w",newline="",encoding="utf-8") as f:
            wr=csv.DictWriter(f,fieldnames=list(rows[0].keys())); wr.writeheader(); wr.writerows(rows)
        print(f"{name}: {len(rows)} rows -> {out}/{name}.csv")
if __name__=="__main__": main()
