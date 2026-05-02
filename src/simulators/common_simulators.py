import random
from datetime import datetime

NUM_USERS = 100
NUM_PRODUCTS = 200

# Define product categories
CATEGORIES = {
    "electronics": list(range(1, 51)),
    "fashion": list(range(51, 101)),
    "home": list(range(101, 151)),
    "sports": list(range(151, 201)),
}

# Assign each user a preferred category
USER_PREFERENCES = {
    user_id: random.choice(list(CATEGORIES.keys()))
    for user_id in range(1, NUM_USERS + 1)
}


def get_preferred_product(user_id):
    category = USER_PREFERENCES[user_id]
    return random.choice(CATEGORIES[category])


def get_random_product():
    return random.randint(1, NUM_PRODUCTS)


def generate_event_id():
    return f"{datetime.utcnow().timestamp()}_{random.randint(1000,9999)}"