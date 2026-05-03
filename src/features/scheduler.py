import time
from src.features.feature_engineering import run_pipeline
from src.common.logger import get_logger
logger = get_logger("feature_engineering")

INTERVAL = 300  # seconds (5 minutes)

def start():
    logger.info("Starting feature pipeline scheduler...")

    while True:
        try:
            run_pipeline()
        except Exception as e:
            logger.error(f"Error: {e}")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    start()