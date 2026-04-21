import os
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from src.config.config_loader import load_config
import great_expectations as ge


def load_bronze_data(base_path):
    """
    Reads all files from bronze layer into a single dataframe
    (simplified for assignment)
    """
    all_data = []

    for root, dirs, files in os.walk(base_path):
        for file in files:
            file_path = os.path.join(root, file)

            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(file_path)
                elif file.endswith(".json"):
                    df = pd.read_json(file_path)
                else:
                    continue

                all_data.append(df)
            except Exception as e:
                print(f"Skipping {file_path}: {e}")

    if not all_data:
        raise ValueError("No valid data found in Bronze layer")

    return pd.concat(all_data, ignore_index=True)


def run_validation(df):
    df_ge = ge.from_pandas(df)

    results = []

    # -----------------------------
    # Missing values
    # -----------------------------
    for col in df.columns:
        res = df_ge.expect_column_values_to_not_be_null(col)
        results.append((f"{col} not null", res.success))

    # -----------------------------
    # Duplicate check (example: id)
    # -----------------------------
    if "id" in df.columns:
        res = df_ge.expect_column_values_to_be_unique("id")
        results.append(("id uniqueness", res.success))

    # -----------------------------
    # Range check (rating 1–5)
    # -----------------------------
    if "rating" in df.columns:
        res = df_ge.expect_column_values_to_be_between("rating", 1, 5)
        results.append(("rating between 1-5", res.success))

    # -----------------------------
    # Format check (timestamp)
    # -----------------------------
    if "timestamp" in df.columns:
        res = df_ge.expect_column_values_to_not_be_null("timestamp")
        results.append(("timestamp valid", res.success))

    return results


def generate_pdf_report(results, output_path="reports/data_quality_report.pdf"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    doc = SimpleDocTemplate(output_path)
    styles = getSampleStyleSheet()
    content = []

    content.append(Paragraph("Data Quality Report", styles['Title']))
    content.append(Spacer(1, 10))

    for check, status in results:
        status_text = "PASS" if status else "FAIL"
        content.append(Paragraph(f"{check}: {status_text}", styles['Normal']))
        content.append(Spacer(1, 8))

    doc.build(content)
    print(f"Report saved at {output_path}")


def main():
    config = load_config()

    bronze_path = os.path.join(
        config["paths"]["data_lake"],
        config["storage"]["base_path"]
    )

    print(f"Reading Bronze data from: {bronze_path}")

    df = load_bronze_data(bronze_path)

    results = run_validation(df)

    generate_pdf_report(results)


if __name__ == "__main__":
    main()