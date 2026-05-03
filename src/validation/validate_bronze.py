import os
import pandas as pd
from datetime import datetime

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

from src.config.config_loader import load_config


# -----------------------------
# Schema Definitions
# -----------------------------
SCHEMAS = {
    "clickstream": ["event_id", "user_id", "product_id", "timestamp", "event_type"],
    "transactions": ["user_id", "product_id", "purchase_amount", "rating"],
    "products": ["product_id", "category", "brand", "price"]
}


# -----------------------------
# Load Bronze Data (per dataset)
# -----------------------------
def load_bronze_datasets(base_path):
    datasets = {}

    for root, _, files in os.walk(base_path):
        for file in files:
            path = os.path.join(root, file)

            if "clickstream" in path:
                key = "clickstream"
            elif "transactions" in path:
                key = "transactions"
            elif "products" in path:
                key = "products"
            else:
                continue

            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(path)
                elif file.endswith(".json"):
                    df = pd.read_json(path, lines=True)
                else:
                    continue

                datasets.setdefault(key, []).append(df)

            except Exception as e:
                print(f"Skipping {path}: {e}")

    # merge each dataset
    for k in datasets:
        datasets[k] = pd.concat(datasets[k], ignore_index=True)

    if not datasets:
        raise ValueError("No valid Bronze datasets found")

    return datasets


# -----------------------------
# Profiling
# -----------------------------
def generate_profile(df):
    profile = []

    for col in df.columns:
        profile.append([
            col,
            str(df[col].dtype),
            round(df[col].isna().mean() * 100, 2),
            round(df[col].nunique() / len(df) * 100, 2)
        ])

    return profile


# -----------------------------
# Validation per dataset
# -----------------------------
def validate_dataset(df, dataset_name):
    results = []

    required_cols = SCHEMAS.get(dataset_name, [])

    # -----------------------------
    # Required columns
    # -----------------------------
    for col in required_cols:
        if col not in df.columns:
            results.append((f"{col} exists", False))
        else:
            results.append((f"{col} not null", df[col].notna().all()))

    # -----------------------------
    # Additional checks
    # -----------------------------
    if dataset_name == "transactions":
        if "rating" in df.columns:
            valid = df["rating"].between(1, 5).all()
            results.append(("rating between 1-5", valid))

    if dataset_name == "clickstream":
        if "timestamp" in df.columns:
            valid_ts = pd.to_datetime(df["timestamp"], errors="coerce")
            results.append(("timestamp valid format", valid_ts.notna().all()))

    if dataset_name == "products":
        if "price" in df.columns:
            results.append(("price > 0", (df["price"] > 0).all()))

    # -----------------------------
    # Score
    # -----------------------------
    pass_count = sum([1 for _, s in results if s])
    score = pass_count / len(results) if results else 0

    return results, score


# -----------------------------
# PDF Report
# -----------------------------
def generate_pdf_report(all_results, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    doc = SimpleDocTemplate(output_path)
    styles = getSampleStyleSheet()
    content = []

    # Title
    content.append(Paragraph("📊 Bronze Data Validation Report", styles['Title']))
    content.append(Spacer(1, 10))

    for dataset, data in all_results.items():
        df = data["df"]
        profile = data["profile"]
        results = data["results"]
        score = data["score"]

        content.append(Paragraph(f"{dataset.upper()} DATASET", styles['Heading2']))
        content.append(Spacer(1, 6))

        # Summary
        content.append(Paragraph(
            f"Rows: {len(df)} | Columns: {len(df.columns)} | Score: {round(score*100,2)}%",
            styles['Normal']
        ))
        content.append(Spacer(1, 10))

        # Profile table
        profile_table = [["Column", "Dtype", "Missing %", "Unique %"]] + profile
        table = Table(profile_table)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.black)
        ]))
        content.append(table)
        content.append(Spacer(1, 10))

        # Validation results
        result_table = [["Check", "Status"]]
        for check, status in results:
            result_table.append([check, "PASS" if status else "FAIL"])

        table = Table(result_table)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.black)
        ]))

        content.append(table)
        content.append(Spacer(1, 20))

    doc.build(content)

    print(f"Report saved at {output_path}")


# -----------------------------
# Main
# -----------------------------
def main():
    config = load_config()

    bronze_path = os.path.join(
        config["paths"]["data_lake"],
        config["storage"]["base_path"]
    )

    print(f"Reading Bronze data from: {bronze_path}")

    datasets = load_bronze_datasets(bronze_path)

    all_results = {}

    for name, df in datasets.items():
        print(f"\nValidating {name}")

        profile = generate_profile(df)
        results, score = validate_dataset(df, name)

        print(f"{name} score: {round(score*100,2)}%")

        all_results[name] = {
            "df": df,
            "profile": profile,
            "results": results,
            "score": score
        }

    generate_pdf_report(
        all_results,
        output_path="reports/data_quality_report.pdf"
    )


if __name__ == "__main__":
    main()