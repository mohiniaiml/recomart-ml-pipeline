# ingestion/ingest_transactions_batch.py
import os, pandas as pd, time
from src.common.utils import ensure_dir, today_partition
from src.lineage.lineage_logger import log_lineage
from src.config.config_loader import load_config

config = load_config()

simulator_output = config["paths"]["simulator_output"]
data_lake_path = config["paths"]["data_lake"]
VERSION = config["versions"]["transactions"]

SRC_FILE = os.path.join(simulator_output, "transactions_generated.csv")
DL_BASE = os.path.join(data_lake_path, "raw", "transactions", VERSION)

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

    log_lineage(
        dataset_name="transactions",
        version=VERSION,
        source="transaction_simulator",
        transformation="batch ingestion",
        output_path=out_path
    )

    print(f"Ingested {len(df)} rows -> {out_path}")

def run(interval_sec=30, loop=False):
    if loop:
        while True:
            ingest()
            time.sleep(interval_sec)
    else:
        ingest()

if __name__ == "__main__":
    run(loop=False)
