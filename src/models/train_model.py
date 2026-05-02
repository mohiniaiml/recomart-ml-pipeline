import os
import sqlite3
import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD

import mlflow
import mlflow.sklearn

from src.config.config_loader import load_config
from src.models.evaluate import precision_at_k, recall_at_k, ndcg_at_k

# -----------------------------
# Config
# -----------------------------
config = load_config()

FEATURE_VERSION = config["versions"]["features"]
RECS_VERSION = config["versions"]["recommendations"]

DB_PATH = os.path.join(
    config["paths"]["data_lake"],
    "gold",
    "features",
    FEATURE_VERSION,
    "features.db"
)

OUTPUT_DIR = os.path.join(
    config["paths"]["recommendations_output"],
    RECS_VERSION
)

OUTPUT_FILE = os.path.join(OUTPUT_DIR, "recommendations.csv")


# -----------------------------
# Load Data
# -----------------------------
def load_data():
    conn = sqlite3.connect(DB_PATH)

    interactions = pd.read_sql("SELECT * FROM interaction_features", conn)
    items = pd.read_sql("SELECT product_id, category FROM item_features", conn)

    conn.close()

    return interactions, items


# -----------------------------
# Train/Test Split
# -----------------------------
def train_test_split(df, test_ratio=0.2):
    train, test = [], []

    for user, group in df.groupby("user_id"):
        group = group.sample(frac=1, random_state=42)

        split = int(len(group) * (1 - test_ratio))
        train.append(group.iloc[:split])
        test.append(group.iloc[split:])

    return pd.concat(train), pd.concat(test)


# -----------------------------
# Matrix
# -----------------------------
def build_matrix(df):
    return df.pivot_table(
        index="user_id",
        columns="product_id",
        values="interaction_count",
        fill_value=0
    )


# -----------------------------
# Train SVD
# -----------------------------
def train_svd(matrix, n_components=10):
    svd = TruncatedSVD(n_components=n_components, random_state=42)

    user_factors = svd.fit_transform(matrix)
    item_factors = svd.components_

    reconstructed = np.dot(user_factors, item_factors)

    pred_df = pd.DataFrame(
        reconstructed,
        index=matrix.index,
        columns=matrix.columns
    )

    return svd, pred_df


# -----------------------------
# Category Preference
# -----------------------------
def get_user_preferred_category(user_id, train_df, item_df):
    user_data = train_df[train_df["user_id"] == user_id]

    merged = user_data.merge(item_df, on="product_id", how="left")

    # Drop missing categories
    categories = merged["category"].dropna()

    if categories.empty:
        return None

    return categories.mode().iloc[0]

# -----------------------------
# Hybrid Scoring
# -----------------------------
def get_top_k(pred_df, train_matrix, train_df, item_df, user_id, k=5, alpha=0.8):

    if user_id not in pred_df.index:
        return []

    # Remove seen items
    seen_items = train_matrix.loc[user_id]
    seen_items = seen_items[seen_items > 0].index

    scores = pred_df.loc[user_id].drop(seen_items)

    # Category preference
    preferred_category = get_user_preferred_category(user_id, train_df, item_df)

    if preferred_category:
        item_df_indexed = item_df.set_index("product_id")

        category_boost = scores.index.map(
            lambda x: 1 if item_df_indexed.loc[x]["category"] == preferred_category else 0
        )

        scores = alpha * scores + (1 - alpha) * category_boost

    return list(scores.sort_values(ascending=False).head(k).index)


# -----------------------------
# Evaluation
# -----------------------------
def evaluate_model(pred_df, train_matrix, train_df, test_df, item_df, k=5):

    precision_scores, recall_scores, ndcg_scores = [], [], []

    for user in test_df["user_id"].unique():

        actual_items = test_df[test_df["user_id"] == user]["product_id"].tolist()

        if not actual_items:
            continue

        predicted_items = get_top_k(
            pred_df, train_matrix, train_df, item_df, user, k
        )

        precision_scores.append(precision_at_k(actual_items, predicted_items, k))
        recall_scores.append(recall_at_k(actual_items, predicted_items, k))
        ndcg_scores.append(ndcg_at_k(actual_items, predicted_items, k))

    return {
        "precision_at_5": float(np.mean(precision_scores)),
        "recall_at_5": float(np.mean(recall_scores)),
        "ndcg_at_5": float(np.mean(ndcg_scores))
    }


# -----------------------------
# Save Recommendations
# -----------------------------
def save_recommendations(pred_df, train_matrix, train_df, item_df, k=5):

    rows = []

    for user in train_matrix.index:

        recs = get_top_k(pred_df, train_matrix, train_df, item_df, user, k)

        preferred_category = get_user_preferred_category(user, train_df, item_df)

        for rank, item in enumerate(recs, start=1):

            category = item_df[item_df["product_id"] == item]["category"].values[0]

            if preferred_category and category == preferred_category:
                explanation = f"Matches your preference for {preferred_category}"
            else:
                explanation = f"Popular in {category}"

            rows.append({
                "user_id": int(user),
                "product_id": int(item),
                "rank": rank,
                "category": category,
                "explanation": explanation
            })

    df = pd.DataFrame(rows)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved recommendations -> {OUTPUT_FILE}")


# -----------------------------
# MAIN
# -----------------------------
def main():

    mlflow.set_experiment("recommender-system")

    print("Loading data...")
    interactions, items = load_data()

    print("Splitting data...")
    train_df, test_df = train_test_split(interactions)

    print("Building matrix...")
    train_matrix = build_matrix(train_df)

    print("Training model...")
    model, pred_df = train_svd(train_matrix)

    print("Evaluating model...")
    metrics = evaluate_model(pred_df, train_matrix, train_df, test_df, items)

    for k, v in metrics.items():
        print(f"{k}: {v:.4f}")

    print("Saving recommendations...")
    save_recommendations(pred_df, train_matrix, train_df, items)

    with mlflow.start_run():
        mlflow.log_param("model", "SVD + category hybrid")
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "svd_model")

    print("Training completed.")


if __name__ == "__main__":
    main()