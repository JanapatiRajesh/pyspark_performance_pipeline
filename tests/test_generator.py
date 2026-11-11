import csv
from src.data_generator import generate_data

def test_generator_creates_reproducible_shape(tmp_path):
    customers,transactions=generate_data(str(tmp_path),rows=30,customers=5,seed=7)
    with open(transactions,newline="",encoding="utf-8") as f: rows=list(csv.DictReader(f))
    assert len(rows)>=30
    assert rows[0]["transaction_id"]=="T000000001"
    assert customers.endswith("customers.csv")
