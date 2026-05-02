from flask import Flask, jsonify
import random

# Import shared mapping
from src.simulators.common_simulators import CATEGORIES

app = Flask(__name__)

# -----------------------------
# Reverse Mapping: product → category
# -----------------------------
PRODUCT_TO_CATEGORY = {}

for category, product_ids in CATEGORIES.items():
    for pid in product_ids:
        PRODUCT_TO_CATEGORY[pid] = category


# -----------------------------
# Generate Products (Deterministic)
# -----------------------------
def generate_products():
    products = []

    for product_id, category in PRODUCT_TO_CATEGORY.items():
        products.append({
            "product_id": product_id,
            "category": category,
            "price": round(random.uniform(100, 10000), 2),
            "brand": f"Brand_{random.randint(1, 20)}",
            "description": f"{category} product {product_id}"
        })

    return products


# -----------------------------
# API Endpoint
# -----------------------------
@app.route("/products", methods=["GET"])
def get_products():
    return jsonify(generate_products())


# -----------------------------
# Run Server
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)