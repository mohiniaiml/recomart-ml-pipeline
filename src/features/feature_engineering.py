import pandas as pd
import sqlite3
import os
from datetime import datetime

from src.lineage.lineage_logger import log_lineage
from src.config.config_loader import load_config

config = load_config()

data_lake_path = config["paths"]["data_lake"]

# Per dataset versions
TX_VERSION = config["versions"]["transactions"]
CS_VERSION = config["versions"]["clickstream"]

# Feature version (independent)
FEATURE_VERSION = config["versions"]["features"]

# Use SILVER layer (not raw)
SILVER_BASE = os.path.join(data_lake_path, "silver")

DB_PATH = os.path.join(
    config["paths"]["data_lake"],
    "gold",
    "features",
    FEATURE_VERSION,
    "features.db"
)

# -----------------------------
# Get latest partition
# -----------------------------
def get_latest_partition(path):
    if not os.path.exists(path):
        return None

    partitions = sorted(os.listdir(path))
    return partitions[-1] if partitions else None


# -----------------------------
# Load data (version-aware)
# -----------------------------
def load_data():
    tx_path = os.path.join(SILVER_BASE, "transactions", TX_VERSION)
    cs_path = os.path.join(SILVER_BASE, "clickstream", CS_VERSION)

    t_part = get_latest_partition(tx_path)
    c_part = get_latest_partition(cs_path)

    if not t_part or not c_part:
        print("Data not ready yet")
        return None

    tx_file = os.path.join(tx_path, t_part, "data.parquet")
    cs_file = os.path.join(cs_path, c_part, "data.parquet")

    if not os.path.exists(tx_file) or not os.path.exists(cs_file):
        print(f"Missing files in latest partitions {tx_file} or {cs_file}")
        return None

    transactions = pd.read_parquet(tx_file)
    clicks = pd.read_parquet(cs_file)

    # unify schema
    # Ensure both dataframes have same columns
    required_cols = ["user_id", "product_id", "purchase_amount", "rating"]

    for col in required_cols:
        if col not in transactions.columns:
            transactions[col] = pd.NA
        if col not in clicks.columns:
            clicks[col] = pd.NA

    # Align column order
    transactions = transactions[required_cols]
    clicks = clicks[required_cols]

    transactions["event_type"] = "purchase"

    data = pd.concat([transactions, clicks], ignore_index=True)

    return data, tx_file, cs_file


# -----------------------------
# Feature generation
# -----------------------------
def generate_features(df):

    user_features = df.groupby("user_id").agg(
        activity_count=("product_id", "count"),
        avg_rating=("rating", "mean"),
        purchase_count=("purchase_amount", lambda x: x.notnull().sum())
    ).reset_index()

    item_features = df.groupby("product_id").agg(
        item_popularity=("user_id", "count"),
        avg_rating=("rating", "mean"),
        purchase_count=("purchase_amount", lambda x: x.notnull().sum())
    ).reset_index()

    interaction_features = (
        df.groupby(["user_id", "product_id"])
        .size()
        .reset_index(name="interaction_count")
    )

    return user_features, item_features, interaction_features


# -----------------------------
# Store features (version-aware)
# -----------------------------
def store_features(user_df, item_df, inter_df):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    user_df.to_sql("user_features", conn, if_exists="replace", index=False)
    item_df.to_sql("item_features", conn, if_exists="replace", index=False)
    inter_df.to_sql("interaction_features", conn, if_exists="replace", index=False)

    conn.close()


# -----------------------------
# Main pipeline
# -----------------------------
def run_pipeline():
    print(f"[{datetime.now()}] Running feature pipeline...")

    result = load_data()
    if result is None:
        return

    df, tx_source, cs_source = result

    user_df, item_df, inter_df = generate_features(df)
    store_features(user_df, item_df, inter_df)

    # 🔷 Lineage logging (multi-source)
    log_lineage(
        dataset_name="features",
        version=FEATURE_VERSION,
        source=f"{tx_source} + {cs_source}",
        transformation="aggregation: user/item/interaction features",
        output_path=DB_PATH
    )

    print(f"Stored features → {DB_PATH}")
    print(f"[{datetime.now()}] Feature pipeline completed")


if __name__ == "__main__":
    run_pipeline()