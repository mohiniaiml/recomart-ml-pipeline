# ingestion/ingest_products_api.py
import requests, os, pandas as pd, time
from src.common.utils import ensure_dir, today_partition
from src.lineage.lineage_logger import log_lineage
from src.config.config_loader import load_config
from src.common.logger import get_logger
logger = get_logger("ingestion")

config = load_config()
VERSION = config["versions"]["products"]
data_lake_path = config["paths"]["data_lake"]
DL_BASE = os.path.join(data_lake_path, "raw", "products", VERSION)

API_URL = "http://localhost:5000/products"

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

        log_lineage(
            dataset_name="products",
            version=VERSION,
            source="product_api",
            transformation="API ingestion",
            output_path=out_path
        )

        logger.info(f"Fetched {len(df)} products -> {out_path}")
    except Exception as e:
        logger.error(f"Product ingestion failed: {e}")

def run(interval_sec=60, loop=False):
    if loop:
        while True:
            ingest()
            time.sleep(interval_sec)
    else:
        ingest()

if __name__ == "__main__":
    run(loop=False)
