# simulators/transaction_simulator.py
import csv, random, time, os
from common.utils import ensure_dir, now_ts

OUT_DIR = "simulators/output"
OUT_FILE = os.path.join(OUT_DIR, "transactions_generated.csv")

def init_file():
    ensure_dir(OUT_DIR)
    if not os.path.exists(OUT_FILE):
        with open(OUT_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id","product_id","purchase_amount","rating","transaction_timestamp"])

def generate_row():
    return [
        random.randint(1, 1000),
        random.randint(1, 200),
        round(random.uniform(50, 5000), 2),
        random.choice([None, 3, 4, 5]),
        now_ts()
    ]

def run(interval_sec=5):
    init_file()
    print(f"Writing to {OUT_FILE}")
    while True:
        with open(OUT_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(generate_row())
        print("Generated 1 transaction")
        time.sleep(interval_sec)

if __name__ == "__main__":
    run()
