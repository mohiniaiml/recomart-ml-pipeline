# simulators/clickstream_simulator.py
import json, random, time, os
from src.common.utils import ensure_dir
from datetime import datetime

from src.config.config_loader import load_config

config = load_config()
simulator_output = config["paths"]["simulator_output"]
OUT_FILE = os.path.join(simulator_output, "clickstream_events.json")

EVENTS = ["view", "click", "add_to_cart"]

def now_iso():
    return datetime.now().isoformat()

def generate_event():
    return {
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 200),
        "event_type": random.choice(EVENTS),
        "timestamp": now_iso()
    }

def run(interval_sec=1):
    ensure_dir(simulator_output)
    print(f"Writing to {OUT_FILE}")
    while True:
        with open(OUT_FILE, "a") as f:
            f.write(json.dumps(generate_event()) + "\n")
        print("Generated 1 click event")
        time.sleep(interval_sec)

if __name__ == "__main__":
    run()
