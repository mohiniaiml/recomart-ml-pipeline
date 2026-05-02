import streamlit as st
import pandas as pd
import sqlite3
import os

from src.config.config_loader import load_config
from src.simulators.clickstream_simulator import generate_clickstream

# -----------------------------
# Load Config
# -----------------------------
config = load_config()

FEATURE_VERSION = config["versions"]["features"]
RECS_VERSION = config["versions"]["recommendations"]

FEATURE_DB_PATH = os.path.join(
    config["paths"]["data_lake"],
    "gold",
    "features",
    FEATURE_VERSION,
    "features.db"
)

RECS_FILE = os.path.join(
    config["paths"]["recommendations_output"],
    RECS_VERSION,
    "recommendations.csv"
)

SIMULATOR_OUTPUT = os.path.join(
    config["paths"]["simulator_output"],
    "clickstream"
)

# -----------------------------
# Load Data
# -----------------------------
@st.cache_data
def load_recommendations():
    if not os.path.exists(RECS_FILE):
        return None
    return pd.read_csv(RECS_FILE)


@st.cache_data
def load_user_history():
    if not os.path.exists(FEATURE_DB_PATH):
        return None

    conn = sqlite3.connect(FEATURE_DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM interaction_features", conn)
    except:
        df = None
    conn.close()
    return df


@st.cache_data
def load_item_features():
    if not os.path.exists(FEATURE_DB_PATH):
        return None

    conn = sqlite3.connect(FEATURE_DB_PATH)
    try:
        df = pd.read_sql("SELECT product_id, category FROM item_features", conn)
    except:
        df = None
    conn.close()
    return df


# -----------------------------
# Simulator Handler
# -----------------------------
def handle_simulation(category, user_id):
    file_path = generate_clickstream(
        num_events=120,
        user_id=user_id,
        category=category
    )

    if file_path:
        st.success(f"Generated clickstream for {category}")
        st.write(f"File: {file_path}")
        st.info("👉 Run ingestion + feature + training pipeline to update recommendations")


# -----------------------------
# UI Setup
# -----------------------------
st.set_page_config(page_title="Recommender Demo", layout="wide")
st.title("🛍️ Recommender System Demo")

col_refresh, col_info = st.columns([1, 4])

with col_refresh:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.success("Data refreshed. Reloading...")
        st.rerun()

with col_info:
    st.caption("Run pipeline manually, then click refresh to load latest data.")

recs_df = load_recommendations()
history_df = load_user_history()
items_df = load_item_features()

# -----------------------------
# Validation
# -----------------------------
if recs_df is None:
    st.error("❌ Recommendations not found. Run training pipeline.")
    st.stop()

# -----------------------------
# User Selection
# -----------------------------
user_ids = sorted(recs_df["user_id"].unique())
selected_user = st.selectbox("Select User", user_ids)

# -----------------------------
# Layout
# -----------------------------
col1, col2 = st.columns(2)

# -----------------------------
# USER HISTORY
# -----------------------------
with col1:
    st.subheader("📊 User Interaction History")

    if history_df is not None:
        user_hist = history_df[history_df["user_id"] == selected_user]

        if user_hist.empty:
            st.write("No history available")
        else:
            if items_df is not None:
                user_hist = user_hist.merge(
                    items_df,
                    on="product_id",
                    how="left"
                )

            st.dataframe(user_hist.head(10))

            if "category" in user_hist.columns:
                preferred = user_hist["category"].dropna()
                if not preferred.empty:
                    st.info(f"Preferred Category: {preferred.mode().iloc[0]}")
    else:
        st.write("History not available")

# -----------------------------
# RECOMMENDATIONS
# -----------------------------
with col2:
    st.subheader("🎯 Recommendations")

    user_recs = recs_df[recs_df["user_id"] == selected_user]
    user_recs = user_recs.sort_values("rank")

    if user_recs.empty:
        st.write("No recommendations available")
    else:
        for _, row in user_recs.iterrows():
            st.markdown(f"""
            **Product {row['product_id']}**
            - Category: {row.get('category', 'unknown')}
            - Rank: {row['rank']}
            - 💡 {row.get('explanation', 'No explanation')}
            """)

# -----------------------------
# SIMULATION SECTION
# -----------------------------
st.subheader("🔄 Simulate User Interaction")

categories = ["electronics", "fashion", "home", "sports"]

cols = st.columns(len(categories))

for i, category in enumerate(categories):
    with cols[i]:
        if st.button(f"{category.capitalize()} Clicks", key=category):
            handle_simulation(category, selected_user)

# -----------------------------
# SYSTEM INFO
# -----------------------------
st.sidebar.title("⚙️ System Info")
st.sidebar.write("Feature DB:", FEATURE_DB_PATH)
st.sidebar.write("Recommendations File:", RECS_FILE)

st.sidebar.markdown("---")
st.sidebar.write("Model: SVD + Category Hybrid")
st.sidebar.write("Serving: Precomputed Recommendations")

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("End-to-End ML Pipeline Demo: Data → Features → Model → Recommendations → UI")