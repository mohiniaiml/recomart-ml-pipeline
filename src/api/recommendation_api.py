import os
from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD

from src.config.config_loader import load_config

app = Flask(__name__)

config = load_config()

FEATURE_VERSION = config["versions"]["features"]

DB_PATH = os.path.join(
    config["paths"]["data_lake"],
    "gold",
    "features",
    FEATURE_VERSION,
    "features.db"
)

# -----------------------------
# Load data + train model once
# -----------------------------
def load_model():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql("SELECT * FROM interaction_features", conn)
    conn.close()

    matrix = df.pivot_table(
        index="user_id",
        columns="product_id",
        values="interaction_count",
        fill_value=0
    )

    svd = TruncatedSVD(n_components=10)
    svd.fit(matrix)

    reconstructed = np.dot(svd.transform(matrix), svd.components_)

    pred_df = pd.DataFrame(
        reconstructed,
        index=matrix.index,
        columns=matrix.columns
    )

    return matrix, pred_df


matrix, pred_df = load_model()


# -----------------------------
# Recommendation Logic
# -----------------------------
def get_recommendations(user_id, top_n=5):

    if user_id not in matrix.index:
        return []

    user_interactions = matrix.loc[user_id]
    seen_items = user_interactions[user_interactions > 0].index

    scores = pred_df.loc[user_id]
    scores = scores.drop(seen_items)

    top_items = scores.sort_values(ascending=False).head(top_n)

    return list(map(int, top_items.index))


# -----------------------------
# API Endpoint
# -----------------------------
@app.route("/recommend", methods=["GET"])
def recommend():

    user_id = request.args.get("user_id", type=int)
    top_n = request.args.get("top_n", default=5, type=int)

    if user_id is None:
        return jsonify({"error": "user_id is required"}), 400

    recommendations = get_recommendations(user_id, top_n)

    return jsonify({
        "user_id": user_id,
        "recommendations": recommendations
    })


# -----------------------------
# Run Server
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5001)