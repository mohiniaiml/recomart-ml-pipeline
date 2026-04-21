# ingestion/ingest_transactions_batch.py
import os, pandas as pd, time
from src.common.utils import ensure_dir, today_partition
from src.config.config_loader import load_config

config = load_config()

simulator_output = config["paths"]["simulator_output"]
SRC_FILE = os.path.join(simulator_output, "transactions_generated.csv")

data_lake_path = config["paths"]["data_lake"]
DL_BASE = os.path.join(data_lake_path, "raw", "transactions")

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
