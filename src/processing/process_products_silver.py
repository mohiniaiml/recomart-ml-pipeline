import os
import pandas as pd
from datetime import datetime

from src.config.config_loader import load_config
from src.lineage.lineage_logger import log_lineage

config = load_config()

DATA_LAKE = config["paths"]["data_lake"]

# Versions
PR_VERSION = config["versions"]["products"]

# Paths
BRONZE_BASE = os.path.join(DATA_LAKE, "bronze")
SILVER_BASE = os.path.join(DATA_LAKE, "silver")

BRONZE_PRODUCTS_PATH = os.path.join(BRONZE_BASE, "source=products", f"version={PR_VERSION}", f"type=raw")
SILVER_PRODUCTS_BASE = os.path.join(SILVER_BASE, "products", PR_VERSION)


# -----------------------------
# Load bronze data
# -----------------------------
def load_bronze_products():
    dfs = []

    for root, _, files in os.walk(BRONZE_PRODUCTS_PATH):
        for f in files:
            if f.endswith(".csv") or f.endswith(".parquet"):
                path = os.path.join(root, f)

                try:
                    if f.endswith(".csv"):
                        df = pd.read_csv(path)
                    else:
                        df = pd.read_parquet(path)

                    dfs.append(df)

                except Exception as e:
                    print(f"Skipping {path}: {e}")

    if not dfs:
        print(f"No bronze product data found at {BRONZE_PRODUCTS_PATH}")
        return None

    return pd.concat(dfs, ignore_index=True)


# -----------------------------
# Clean data
# -----------------------------
def clean_products(df):
    print("Cleaning product data...")

    # Drop duplicates by product_id
    df = df.drop_duplicates(subset=["product_id"])

    # Drop invalid rows
    df = df.dropna(subset=["product_id", "category"])

    # Ensure correct dtypes
    df["product_id"] = df["product_id"].astype(int)

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df


# -----------------------------
# Write to silver (partitioned)
# -----------------------------
def write_silver(df):
    now = datetime.now()

    partition_path = os.path.join(
        SILVER_PRODUCTS_BASE,
        f"year={now.year}",
        f"month={now.month:02d}",
        f"day={now.day:02d}"
    )

    os.makedirs(partition_path, exist_ok=True)

    file_path = os.path.join(partition_path, "data.parquet")

    df.to_parquet(file_path, index=False)

    print(f"Saved silver products -> {file_path}")

    return file_path


# -----------------------------
# Pipeline
# -----------------------------
def run():
    df = load_bronze_products()
    if df is None:
        return

    df = clean_products(df)

    output_path = write_silver(df)

    # Lineage tracking
    log_lineage(
        dataset_name="products_silver",
        version=PR_VERSION,
        source="bronze/products",
        transformation="clean + deduplicate + type normalization",
        output_path=output_path
    )


if __name__ == "__main__":
    run()