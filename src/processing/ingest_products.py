import os
import pandas as pd
import sqlite3
from datetime import datetime

from src.config.config_loader import load_config
from src.lineage.lineage_logger import log_lineage

config = load_config()

DATA_LAKE = config["paths"]["data_lake"]
VERSION = config["versions"]["products"]

RAW_PATH = os.path.join(DATA_LAKE, "raw", "products")
SILVER_PATH = os.path.join(DATA_LAKE, "silver", "products", VERSION)

DB_PATH = os.path.join(
    config["paths"]["features_output"],
    config["versions"]["features"],
    "features.db"
)


def load_raw_products():
    all_files = []

    for root, _, files in os.walk(RAW_PATH):
        for f in files:
            if f.endswith(".csv"):
                all_files.append(os.path.join(root, f))

    dfs = [pd.read_csv(f) for f in all_files]

    return pd.concat(dfs, ignore_index=True)


def clean_products(df):
    df = df.drop_duplicates(subset=["product_id"])
    df = df.dropna(subset=["product_id", "category"])
    return df


def save_silver(df):
    os.makedirs(SILVER_PATH, exist_ok=True)
    out_file = os.path.join(SILVER_PATH, "products.csv")

    df.to_csv(out_file, index=False)
    print(f"Saved silver products -> {out_file}")


def store_in_db(df):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    df.to_sql("products", conn, if_exists="replace", index=False)

    conn.close()
    print("Stored products in DB")


def run():
    df = load_raw_products()
    df = clean_products(df)

    save_silver(df)
    store_in_db(df)

    log_lineage(
        dataset_name="products",
        version=VERSION,
        source="raw/products",
        transformation="clean + deduplicate",
        output_path=SILVER_PATH
    )


if __name__ == "__main__":
    run()