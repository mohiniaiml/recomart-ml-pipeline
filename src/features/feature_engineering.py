import pandas as pd
import sqlite3
import os
from datetime import datetime

from src.common.logger import get_logger
logger = get_logger("feature_engineering")

from src.lineage.lineage_logger import log_lineage
from src.config.config_loader import load_config

config = load_config()

data_lake_path = config["paths"]["data_lake"]

# Versions
TX_VERSION = config["versions"]["transactions"]
CS_VERSION = config["versions"]["clickstream"]
PR_VERSION = config["versions"]["products"]
FEATURE_VERSION = config["versions"]["features"]

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
# Load data (JOIN metadata)
# -----------------------------
def load_data():

    tx_path = os.path.join(SILVER_BASE, "transactions", TX_VERSION)
    cs_path = os.path.join(SILVER_BASE, "clickstream", CS_VERSION)
    pr_path = os.path.join(SILVER_BASE, "products", PR_VERSION)

    t_part = get_latest_partition(tx_path)
    c_part = get_latest_partition(cs_path)
    p_part = get_latest_partition(pr_path)

    if not t_part or not c_part or not p_part:
        logger.warning("Data not ready yet")
        return None

    tx_file = os.path.join(tx_path, t_part, "data.parquet")
    cs_file = os.path.join(cs_path, c_part, "data.parquet")
    pr_file = os.path.join(pr_path, p_part, "data.parquet")

    missing_files = []

    if not os.path.exists(tx_file):
        missing_files.append(("transactions", tx_file))

    if not os.path.exists(cs_file):
        missing_files.append(("clickstream", cs_file))

    if not os.path.exists(pr_file):
        missing_files.append(("products", pr_file))

    if missing_files:
        logger.warning("Missing files in latest partitions:")
        for name, path in missing_files:
            logger.warning(f" - {name}: {path}")
        return None
    
    # Load
    transactions = pd.read_parquet(tx_file)
    clicks = pd.read_parquet(cs_file)
    products = pd.read_parquet(pr_file)

    # -----------------------------
    # Normalize schema
    # -----------------------------
    required_cols = ["user_id", "product_id", "purchase_amount", "rating"]

    for col in required_cols:
        if col not in transactions.columns:
            transactions[col] = pd.NA
        if col not in clicks.columns:
            clicks[col] = pd.NA

    transactions = transactions[required_cols]
    clicks = clicks[required_cols]

    transactions["event_type"] = "purchase"
    clicks["event_type"] = "click"

    data = pd.concat([transactions, clicks], ignore_index=True)

    # -----------------------------
    # Deduplicate
    # -----------------------------
    data = data.drop_duplicates()

    # -----------------------------
    # JOIN product metadata
    # -----------------------------
    data = data.merge(
        products[["product_id", "category", "brand", "price"]],
        on="product_id",
        how="left"
    )

    return data, tx_file, cs_file, pr_file


# -----------------------------
# Feature Engineering
# -----------------------------
def generate_features(df):

    # -----------------------------
    # Derived features
    # -----------------------------
    df["is_purchase"] = df["purchase_amount"].notnull().astype(int)

    # Category popularity
    df["category_popularity"] = df.groupby("category")["product_id"].transform("count")

    # Price normalization
    if "price" in df.columns:
        df["price_norm"] = (df["price"] - df["price"].min()) / (
            df["price"].max() - df["price"].min() + 1e-9
        )

    # -----------------------------
    # USER FEATURES
    # -----------------------------
    user_features = df.groupby("user_id").agg(
        activity_count=("product_id", "count"),
        purchase_count=("is_purchase", "sum"),
        avg_rating=("rating", "mean"),
        avg_price=("price", "mean")
    ).reset_index()

    # -----------------------------
    # ITEM FEATURES
    # -----------------------------
    item_features = df.groupby("product_id").agg(
        item_popularity=("user_id", "count"),
        purchase_count=("is_purchase", "sum"),
        avg_rating=("rating", "mean"),
        category=("category", "first"),
        brand=("brand", "first"),
        avg_price=("price", "mean"),
        category_popularity=("category_popularity", "mean")
    ).reset_index()

    # -----------------------------
    # INTERACTION FEATURES (USED BY MODEL)
    # -----------------------------
    interaction_features = (
        df.groupby(["user_id", "product_id"])
        .size()
        .reset_index(name="interaction_count")
    )

    return user_features, item_features, interaction_features


# -----------------------------
# Store features
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
    logger.info(f"[{datetime.now()}] Running feature pipeline...")

    result = load_data()
    if result is None:
        return

    df, tx_source, cs_source, pr_source = result

    user_df, item_df, inter_df = generate_features(df)
    store_features(user_df, item_df, inter_df)

    # Lineage
    log_lineage(
        dataset_name="features",
        version=FEATURE_VERSION,
        source=f"{tx_source} + {cs_source} + {pr_source}",
        transformation="aggregation + metadata join + feature enrichment",
        output_path=DB_PATH
    )

    logger.info(f"Stored features -> {DB_PATH}")
    logger.info(f"[{datetime.now()}] Feature pipeline completed")


if __name__ == "__main__":
    run_pipeline()