import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.common.logger import get_logger
logger = get_logger("processing")

# -----------------------------
# Utility: Save Plotly Figure
# -----------------------------
def fig_to_html(fig):
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


# -----------------------------
# Dataset Section Generator
# -----------------------------
def generate_dataset_section(df, dataset_name):
    sections = []

    # -----------------------------
    # Title
    # -----------------------------
    sections.append(f"<h2>{dataset_name.upper()} DATASET</h2>")

    # -----------------------------
    # Summary
    # -----------------------------
    sections.append(f"""
    <p>
    Rows: {len(df)} <br>
    Columns: {len(df.columns)}
    </p>
    """)

    # -----------------------------
    # Missing Values
    # -----------------------------
    missing = (
        df.isnull()
        .mean()
        .mul(100)
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )

    missing.columns = ["column", "missing_percent"]

    fig = px.bar(
        missing,
        x="column",
        y="missing_percent",
        title="Missing Values (%)",
        text="missing_percent"
    )

    fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    fig.update_layout(xaxis_title="Column", yaxis_title="Missing %")
    sections.append(fig_to_html(fig))

    # -----------------------------
    # Correlation Heatmap
    # -----------------------------
    numeric_df = df.select_dtypes(include=["int64", "float64"])

    if len(numeric_df.columns) > 1:
        corr = numeric_df.corr()

        fig = px.imshow(
            corr,
            text_auto=True,
            title="Correlation Heatmap"
        )
        sections.append(fig_to_html(fig))

    # -----------------------------
    # Category-wise Analysis
    # -----------------------------
    if "category" in df.columns:
        cat_counts = df["category"].value_counts().head(10)

        fig = px.bar(
            x=cat_counts.index,
            y=cat_counts.values,
            title="Top Categories"
        )
        sections.append(fig_to_html(fig))

    # -----------------------------
    # Rating Distribution
    # -----------------------------
    if "rating" in df.columns:
        fig = px.histogram(
            df,
            x="rating",
            nbins=10,
            title="Rating Distribution"
        )
        sections.append(fig_to_html(fig))

    # -----------------------------
    # Top Products
    # -----------------------------
    if "product_id" in df.columns:
        top_products = df["product_id"].value_counts().head(20)

        fig = px.bar(
            x=top_products.index.astype(str),
            y=top_products.values,
            title="Top Products"
        )
        sections.append(fig_to_html(fig))

    return "\n".join(sections)


# -----------------------------
# Full HTML Report
# -----------------------------
def generate_eda_report(datasets, output_path="reports/eda/eda_report.html"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    html_content = [
        "<html><head><title>EDA Report</title></head><body>",
        "<h1>📊 Exploratory Data Analysis Report</h1>"
    ]

    for name, df in datasets.items():
        section = generate_dataset_section(df, name)
        html_content.append(section)
        html_content.append("<hr>")

    html_content.append("</body></html>")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_content))

    logger.info(f"EDA report generated: {output_path}")

    return output_path