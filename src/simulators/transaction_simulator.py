import os
import pandas as pd
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
OUTPUT_FILE = os.path.join(simulator_output, "transactions_generated.csv")

def generate_transaction():
    user_id = random.randint(1, NUM_USERS)

    # 80% preference-based, 20% random
    if random.random() < 0.8:
        product_id = get_preferred_product(user_id)
    else:
        product_id = get_random_product()

    return {
        "event_id": generate_event_id(),
        "user_id": user_id,
        "product_id": product_id,
        "purchase_amount": round(random.uniform(10, 500), 2),
        "rating": random.choice([3, 4, 5]),
        "timestamp": datetime.utcnow().isoformat()
    }


def run(batch_size=50, interval=10):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    while True:
        data = [generate_transaction() for _ in range(batch_size)]
        df = pd.DataFrame(data)

        if os.path.exists(OUTPUT_FILE):
            df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)
        else:
            df.to_csv(OUTPUT_FILE, index=False)

        print(f"Generated {batch_size} transactions -> {OUTPUT_FILE}")
        time.sleep(interval)


if __name__ == "__main__":
    run()