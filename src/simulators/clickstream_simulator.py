# simulators/clickstream_simulator.py
import os
import json
import time
import random
from datetime import datetime

from src.simulators.common_simulators import (
    NUM_USERS,
    get_preferred_product,
    get_random_product,
    generate_event_id
)
from src.config.config_loader import load_config

config = load_config()
simulator_output = config["paths"]["simulator_output"]
OUTPUT_FILE = os.path.join(simulator_output, "clickstream_events.json")

EVENT_TYPES = ["view", "click", "add_to_cart"]


def generate_click_event():
    user_id = random.randint(1, NUM_USERS)

    # 70% preference-based behavior
    if random.random() < 0.7:
        product_id = get_preferred_product(user_id)
    else:
        product_id = get_random_product()

    return {
        "event_id": generate_event_id(),
        "user_id": user_id,
        "product_id": product_id,
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.utcnow().isoformat()
    }


def run(batch_size=100, interval=5):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    while True:
        with open(OUTPUT_FILE, "a") as f:
            for _ in range(batch_size):
                event = generate_click_event()
                f.write(json.dumps(event) + "\n")

        print(f"Generated {batch_size} click events -> {OUTPUT_FILE}")
        time.sleep(interval)


if __name__ == "__main__":
    run()