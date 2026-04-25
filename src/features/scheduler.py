import time
from src.features.feature_engineering import run_pipeline

INTERVAL = 300  # seconds (5 minutes)

def start():
    print("Starting feature pipeline scheduler...")

    while True:
        try:
            run_pipeline()
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    start()