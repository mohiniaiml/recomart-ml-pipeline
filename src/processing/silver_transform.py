import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.config.config_loader import load_config

# -----------------------------
# Load Bronze Data
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

    return pd.concat(all_data, ignore_index=True)


# -----------------------------
# Data Cleaning
# -----------------------------
def clean_data(df):
    print("Cleaning data...")

    # Drop duplicates
    df = df.drop_duplicates()

    # Handle missing values
    df = df.dropna(subset=['user_id', 'product_id'])

    # Fill optional fields
    if 'rating' in df.columns:
        df['rating'] = df['rating'].fillna(df['rating'].mean())

    return df


# -----------------------------
# Encoding
# -----------------------------
def encode_data(df):
    print("Encoding categorical variables...")

    if 'category' in df.columns:
        df['category_encoded'] = df['category'].astype('category').cat.codes

    return df


# -----------------------------
# Normalization
# -----------------------------
def normalize_data(df):
    print("Normalizing numerical features...")

    if 'price' in df.columns:
        df['price_norm'] = (df['price'] - df['price'].min()) / (
            df['price'].max() - df['price'].min()
        )

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['timestamp_norm'] = df['timestamp'].astype(np.int64) // 10**9

    return df


# -----------------------------
# EDA
# -----------------------------
def run_eda(df, output_dir="reports/eda"):
    os.makedirs(output_dir, exist_ok=True)

    print("Running EDA...")

    # Interaction distribution
    if 'rating' in df.columns:
        plt.figure()
        df['rating'].hist()
        plt.title("Rating Distribution")
        plt.savefig(f"{output_dir}/rating_distribution.png")

    # Item popularity
    if 'product_id' in df.columns:
        plt.figure()
        df['product_id'].value_counts().head(20).plot(kind='bar')
        plt.title("Top 20 Popular Items")
        plt.savefig(f"{output_dir}/item_popularity.png")

    # Sparsity
    if 'user_id' in df.columns and 'product_id' in df.columns:
        interaction_matrix = df.pivot_table(
            index='user_id',
            columns='product_id',
            aggfunc='size',
            fill_value=0
        )

        sparsity = 1.0 - (interaction_matrix.astype(bool).sum().sum() /
                         interaction_matrix.size)

        print(f"Sparsity: {sparsity:.4f}")

        plt.figure()
        plt.imshow(interaction_matrix.iloc[:50, :50])
        plt.title("User-Item Interaction Heatmap (sample)")
        plt.savefig(f"{output_dir}/interaction_heatmap.png")


# -----------------------------
# Save Silver Data
# -----------------------------
def save_silver(df, output_path):
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, "silver_dataset.parquet")

    try:
        df.to_parquet(file_path, index=False)
        print(f"Saved as Parquet: {file_path}")
    except ImportError:
        fallback = file_path.replace(".parquet", ".csv")
        df.to_csv(fallback, index=False)
        print(f"PyArrow missing → saved as CSV: {fallback}")


# -----------------------------
# Main Pipeline
# -----------------------------
def main():
    config = load_config()

    bronze_path = os.path.join(
        config["paths"]["data_lake"],
        config["storage"]["base_path"]
    )

    silver_path = os.path.join(
        config["paths"]["data_lake"],
        "silver"
    )

    df = load_bronze_data(bronze_path)

    df = clean_data(df)
    df = encode_data(df)
    df = normalize_data(df)

    run_eda(df)

    save_silver(df, silver_path)


if __name__ == "__main__":
    main()