# simulators/clickstream_simulator.py
import os
import json
import time
import random
from datetime import datetime
import pandas as pd
import numpy as np

from src.simulators.common_simulators import (
    NUM_USERS,
    get_preferred_product,
    get_random_product,
    generate_event_id,
    get_products_by_category
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

def generate_clickstream(num_events, user_id, category):
    """
    Generate clickstream events using category mapping and write as JSONL
    """

    output_file = OUTPUT_FILE
    # -----------------------------
    # Validate inputs
    # -----------------------------
    if not category:
        raise ValueError("Category is required")

    product_ids = get_products_by_category(category)

    if not product_ids:
        print(f"No products found for category: {category}")
        return None

    # -----------------------------
    # Generate events
    # -----------------------------
    events = []

    for _ in range(num_events):
        events.append({
            "event_id": generate_event_id(),
            "user_id": int(user_id),
            "product_id": int(np.random.choice(product_ids)),
            "timestamp": datetime.now().isoformat(),
            "event_type": "click"
        })

    # -----------------------------
    # Write as JSONL (append mode)
    # -----------------------------
    with open(output_file, "a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"Generated {num_events} events → {output_file}")

    return output_file

if __name__ == "__main__":
    run()