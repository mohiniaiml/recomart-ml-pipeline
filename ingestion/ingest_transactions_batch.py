# ingestion/ingest_transactions_batch.py
import os, pandas as pd, time
from common.utils import ensure_dir, today_partition

SRC_FILE = "simulators/output/transactions_generated.csv"
DL_BASE = "data_lake/raw/transactions"

def ingest():
    if not os.path.exists(SRC_FILE):
        print("No source file yet.")
        return

    df = pd.read_csv(SRC_FILE)

    part = today_partition()
    out_dir = os.path.join(DL_BASE, part)
    ensure_dir(out_dir)

    out_path = os.path.join(out_dir, "data.csv")
    df.to_csv(out_path, index=False)
    print(f"Ingested {len(df)} rows → {out_path}")

def run(interval_sec=30):
    while True:
        ingest()
        time.sleep(interval_sec)

if __name__ == "__main__":
    run()
