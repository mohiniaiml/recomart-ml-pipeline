import sqlite3
import os
import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import mlflow
import mlflow.sklearn

from src.config.config_loader import load_config

config = load_config()
FEATURE_VERSION = config["versions"]["features"]

DB_PATH = os.path.join(
    config["paths"]["data_lake"],
    "gold",
    "features",
    FEATURE_VERSION,
    "features.db"
)


def load_data():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql("SELECT * FROM interaction_features", conn)
    conn.close()

    return df


def train_model(df):
    # Create user-item matrix
    matrix = df.pivot_table(
        index="user_id",
        columns="product_id",
        values="interaction_count",
        fill_value=0
    )

    train, test = train_test_split(matrix, test_size=0.2, random_state=42)

    # SVD
    svd = TruncatedSVD(n_components=10)
    svd.fit(train)

    # Reconstruction
    reconstructed = np.dot(svd.transform(test), svd.components_)

    rmse = np.sqrt(mean_squared_error(test.values.flatten(), reconstructed.flatten()))

    return svd, rmse


def main():
    mlflow.set_experiment("recommender-system")

    df = load_data()

    with mlflow.start_run():

        model, rmse = train_model(df)

        mlflow.log_param("model", "SVD")
        mlflow.log_param("n_components", 10)
        mlflow.log_metric("rmse", rmse)

        mlflow.sklearn.log_model(model, "svd_model")

        print(f"RMSE: {rmse:.4f}")


if __name__ == "__main__":
    main()