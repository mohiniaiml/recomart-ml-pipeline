import os
from prefect import task, get_run_logger

from src.config.config_loader import load_config
from src.validation.validate_bronze import (
    load_bronze_data,
    generate_profile,
    run_validation,
    generate_pdf_report
)


@task(name="data_validation", retries=1, retry_delay_seconds=5)
def run_validation_task():
    """
    Prefect task for data validation.

    Steps:
    - Load bronze data
    - Generate profiling metrics
    - Run validation checks
    - Generate PDF report
    - Enforce threshold-based decision
    """

    logger = get_run_logger()

    # -----------------------------
    # Load config
    # -----------------------------
    config = load_config()

    bronze_path = os.path.join(
        config["paths"]["data_lake"],
        config["storage"]["base_path"]
    )

    report_path = os.path.join(
        config["paths"].get("reports", "reports"),
        "data_quality_report.pdf"
    )

    validation_threshold = config.get("validation", {}).get("threshold", 0.8)
    fail_on_error = config.get("validation", {}).get("fail_on_error", False)

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Validation threshold: {validation_threshold}")

    # -----------------------------
    # Load data
    # -----------------------------
    try:
        df = load_bronze_data(bronze_path)
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    except Exception as e:
        raise Exception(f"Failed to load Bronze data: {e}")

    # -----------------------------
    # Profiling
    # -----------------------------
    profile_df = generate_profile(df)
    logger.info("Generated column profiling")

    # -----------------------------
    # Validation
    # -----------------------------
    results, _, _ = run_validation(df)

    pass_count = sum([1 for _, status in results if status])
    total_checks = len(results)
    score = pass_count / total_checks if total_checks > 0 else 0

    logger.info(f"Validation score: {score:.2f}")
    logger.info(f"Checks passed: {pass_count}/{total_checks}")

    # -----------------------------
    # Generate report
    # -----------------------------
    generate_pdf_report(
        df,
        profile_df,
        results,
        output_path=report_path
    )

    logger.info(f"Validation report generated at: {report_path}")

    # -----------------------------
    # Decision Logic
    # -----------------------------
    if score < validation_threshold:
        message = f"Data quality below threshold ({score:.2f} < {validation_threshold})"

        if fail_on_error:
            logger.error(message)
            raise Exception(message)
        else:
            logger.warning(message)
    else:
        logger.info("Data quality checks passed")

    # -----------------------------
    # Return structured result
    # -----------------------------
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "validation_score": round(score, 4),
        "report_path": report_path
    }