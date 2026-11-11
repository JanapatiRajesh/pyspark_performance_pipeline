import argparse, csv, random
from datetime import datetime, timedelta, timezone
from pathlib import Path

def generate_data(output_dir="data/input", rows=100000, customers=5000, seed=42):
    if rows < 1 or customers < 1: raise ValueError("rows and customers must be positive")
    out=Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    rng=random.Random(seed)
    with (out/"customers.csv").open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["customer_id","customer_name","city"])
        cities=["Hyderabad","Chennai","Bengaluru","Pune","Delhi"]
        for i in range(1,customers+1): w.writerow([f"C{i:06d}",f"Customer {i}",cities[(i-1)%5]])
    start=datetime(2025,1,1,tzinfo=timezone.utc)
    with (out/"transactions.csv").open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["transaction_id","customer_id","amount","currency","event_ts","status"])
        for i in range(1,rows+1):
            cid=f"C{rng.randint(1,customers):06d}"; amount=f"{rng.uniform(1,10000):.2f}"
            currency=rng.choice(["INR","USD","EUR"])
            ts=(start+timedelta(seconds=rng.randint(0,31536000))).isoformat().replace("+00:00","Z")
            if i%997==0: cid="C999999"
            elif i%991==0: amount="not-a-number"
            elif i%983==0: amount="-5.00"
            elif i%977==0: currency="XXX"
            elif i%971==0: cid=""
            row=[f"T{i:09d}",cid,amount,currency,ts,"COMPLETED"]; w.writerow(row)
            if i%5000==0: w.writerow(row)
    return str(out/"customers.csv"), str(out/"transactions.csv")

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--output-dir",default="data/input"); p.add_argument("--rows",type=int,default=100000); p.add_argument("--customers",type=int,default=5000); p.add_argument("--seed",type=int,default=42)
    a=p.parse_args(); print("Generated:", generate_data(a.output_dir,a.rows,a.customers,a.seed))
