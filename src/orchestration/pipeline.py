from prefect import flow, task, get_run_logger
from src.orchestration.tasks_validation import run_validation_task
import subprocess


# -----------------------------
# Helper to run scripts
# -----------------------------
def run_command(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


# -----------------------------
# Tasks
# -----------------------------
@task
def run_ingestion():
    logger = get_run_logger()
    logger.info("Running ingestion...")

    commands = [
        "python -m src.ingestion.ingest_transactions_batch",
        "python -m src.ingestion.process_clickstream_stream",
        "python -m src.ingestion.ingest_products_api"   # or ingestion if you kept it there
    ]

    for cmd in commands:
        logger.info(f"Running: {cmd}")

        code, out, err = run_command(cmd)

        if code != 0:
            raise Exception(f"{cmd} failed:\n{err}")

        logger.info(out)

    logger.info("All ingestion steps completed")


@task
def run_bronze():
    logger = get_run_logger()
    logger.info("Running bronze processing...")

    code, out, err = run_command("python -m src.ingestion.run_bronze")

    if code != 0:
        raise Exception(f"Bronze failed: {err}")

    logger.info("Bronze completed")


@task
def run_silver():
    logger = get_run_logger()
    logger.info("Running silver transformation...")

    code, out, err = run_command("python -m src.processing.silver_transform")

    if code != 0:
        raise Exception(f"Silver failed: {err}")

    logger.info("Silver completed")


@task
def run_features():
    logger = get_run_logger()
    logger.info("Running feature pipeline...")

    code, out, err = run_command("python -m src.features.feature_engineering")

    if code != 0:
        raise Exception(f"Feature pipeline failed: {err}")

    logger.info("Features completed")


@task
def run_training():
    logger = get_run_logger()
    logger.info("Running model training...")

    code, out, err = run_command("python -m src.models.train_model")

    if code != 0:
        raise Exception(f"Training failed: {err}")

    logger.info("Training completed")


# -----------------------------
# Flow (DAG)
# -----------------------------
@flow(name="recommender-pipeline")
def main_pipeline():

    logger = get_run_logger()
    logger.info("Starting full pipeline...")

    run_ingestion()
    run_bronze()
    run_validation_task()
    run_silver()
    run_features()
    run_training()

    logger.info("Pipeline completed successfully!")


if __name__ == "__main__":
    main_pipeline()