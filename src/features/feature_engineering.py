import pandas as pd
import sqlite3
import os
from datetime import datetime
from src.config.config_loader import load_config

config = load_config()

data_lake_path = config["paths"]["data_lake"]
DATA_LAKE = os.path.join(data_lake_path, "raw")
DB_PATH = config["paths"]["features_db"]

def get_latest_partition(path):
    if not os.path.exists(path):
        return None
    partitions = sorted(os.listdir(path))
    return partitions[-1] if partitions else None

def load_data():
    # latest partitions
    t_part = get_latest_partition(f"{DATA_LAKE}/transactions")
    c_part = get_latest_partition(f"{DATA_LAKE}/clickstream")

    if not t_part or not c_part:
        print("Data not ready yet")
        return None

    transactions = pd.read_csv(f"{DATA_LAKE}/transactions/{t_part}/data.csv")
    clicks = pd.read_csv(f"{DATA_LAKE}/clickstream/{c_part}/events.csv")

    # unify schema
    clicks["purchase_amount"] = None
    clicks["rating"] = None

    transactions["event_type"] = "purchase"

    data = pd.concat([transactions, clicks], ignore_index=True)
    return data

def generate_features(df):
    # USER FEATURES
    user_features = df.groupby("user_id").agg(
        activity_count=("product_id", "count"),
        avg_rating=("rating", "mean"),
        purchase_count=("purchase_amount", lambda x: x.notnull().sum())
    ).reset_index()

    # ITEM FEATURES
    item_features = df.groupby("product_id").agg(
        item_popularity=("user_id", "count"),
        avg_rating=("rating", "mean"),
        purchase_count=("purchase_amount", lambda x: x.notnull().sum())
    ).reset_index()

    # INTERACTION FEATURES
    interaction_features = (
        df.groupby(["user_id", "product_id"])
        .size()
        .reset_index(name="interaction_count")
    )

    return user_features, item_features, interaction_features

def store_features(user_df, item_df, inter_df):
    os.makedirs("features", exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    user_df.to_sql("user_features", conn, if_exists="replace", index=False)
    item_df.to_sql("item_features", conn, if_exists="replace", index=False)
    inter_df.to_sql("interaction_features", conn, if_exists="replace", index=False)

    conn.close()

def run_pipeline():
    print(f"[{datetime.now()}] Running feature pipeline...")

    df = load_data()
    if df is None:
        return

    user_df, item_df, inter_df = generate_features(df)
    store_features(user_df, item_df, inter_df)

    print(f"[{datetime.now()}] Feature pipeline completed")

if __name__ == "__main__":
    run_pipeline()