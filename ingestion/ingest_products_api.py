# ingestion/ingest_products_api.py
import requests, os, pandas as pd, time
from common.utils import ensure_dir, today_partition

API_URL = "http://localhost:5000/products"
DL_BASE = "data_lake/raw/products"

def ingest():
    try:
        resp = requests.get(API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)

        part = today_partition()
        out_dir = os.path.join(DL_BASE, part)
        ensure_dir(out_dir)

        out_path = os.path.join(out_dir, "data.csv")
        df.to_csv(out_path, index=False)
        print(f"Fetched {len(df)} products → {out_path}")
    except Exception as e:
        print(f"Product ingestion failed: {e}")

def run(interval_sec=60):
    while True:
        ingest()
        time.sleep(interval_sec)

if __name__ == "__main__":
    run()