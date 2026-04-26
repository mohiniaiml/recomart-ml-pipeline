import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

from src.config.config_loader import load_config
from src.lineage.lineage_logger import log_lineage


# -----------------------------
# Load Bronze Data (dataset + version aware)
# -----------------------------
def load_bronze_data(base_path):
    all_data = []

    for root, _, files in os.walk(base_path):
        for file in files:
            path = os.path.join(root, file)

            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(path)
                elif file.endswith(".json"):
                    df = pd.read_json(path)
                else:
                    continue

                all_data.append(df)

            except Exception as e:
                print(f"Skipping {path}: {e}")

    if not all_data:
        return None

    return pd.concat(all_data, ignore_index=True)


# -----------------------------
# Data Cleaning
# -----------------------------
def clean_data(df):
    df = df.drop_duplicates()

    if 'user_id' in df.columns and 'product_id' in df.columns:
        df = df.dropna(subset=['user_id', 'product_id'])

    if 'rating' in df.columns:
        df['rating'] = df['rating'].fillna(df['rating'].mean())

    return df


# -----------------------------
# Encoding
# -----------------------------
def encode_data(df):
    if 'category' in df.columns:
        df['category_encoded'] = df['category'].astype('category').cat.codes

    return df


# -----------------------------
# Normalization
# -----------------------------
def normalize_data(df):
    if 'price' in df.columns:
        df['price_norm'] = (df['price'] - df['price'].min()) / (
            df['price'].max() - df['price'].min()
        )

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['timestamp_norm'] = df['timestamp'].astype(np.int64) // 10**9

    return df


# -----------------------------
# Save Silver Data (version aware)
# -----------------------------
def save_silver(df, base_path, dataset, version):
    date = datetime.now().strftime("%Y-%m-%d")

    output_dir = os.path.join(base_path, dataset, version, date)
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, "data.parquet")

    try:
        df.to_parquet(file_path, index=False)
    except ImportError:
        file_path = file_path.replace(".parquet", ".csv")
        df.to_csv(file_path, index=False)

    return file_path


# -----------------------------
# EDA (optional per dataset)
# -----------------------------
def run_eda(df, dataset, output_dir="reports/eda"):
    os.makedirs(output_dir, exist_ok=True)

    if 'rating' in df.columns:
        plt.figure()
        df['rating'].hist()
        plt.title(f"{dataset} Rating Distribution")
        plt.savefig(f"{output_dir}/{dataset}_rating_distribution.png")

    if 'product_id' in df.columns:
        plt.figure()
        df['product_id'].value_counts().head(20).plot(kind='bar')
        plt.title(f"{dataset} Top Items")
        plt.savefig(f"{output_dir}/{dataset}_item_popularity.png")


# -----------------------------
# Main Pipeline (dataset-wise)
# -----------------------------
def main():
    config = load_config()

    data_lake = config["paths"]["data_lake"]
    bronze_base = os.path.join(data_lake, config["storage"]["base_path"])
    silver_base = os.path.join(data_lake, "silver")

    versions = config["versions"]

    datasets = ["transactions", "clickstream", "products"]

    for dataset in datasets:

        version = versions.get(dataset, "v1")

        bronze_path = os.path.join(
            bronze_base,
            f"source={dataset}",
            f"version={version}"
        )

        if not os.path.exists(bronze_path):
            print(f"Skipping {dataset}, no bronze data")
            continue

        print(f"Processing {dataset} version {version}")

        df = load_bronze_data(bronze_path)

        if df is None:
            continue

        # Transformations
        df = clean_data(df)
        df = encode_data(df)
        df = normalize_data(df)

        run_eda(df, dataset)

        # Save silver
        output_path = save_silver(df, silver_base, dataset, version)

        # 🔷 Lineage logging
        log_lineage(
            dataset_name=dataset,
            version=version,
            source=f"bronze/source={dataset}/version={version}",
            transformation="clean + encode + normalize",
            output_path=output_path
        )

        print(f"Silver created -> {output_path}")


if __name__ == "__main__":
    main()