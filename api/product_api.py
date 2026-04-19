# api/product_api.py
from flask import Flask, jsonify
import random

app = Flask(__name__)

CATEGORIES = ["Electronics", "Clothing", "Home", "Books", "Sports"]

def generate_products(n=50):
    products = []
    for i in range(1, n+1):
        products.append({
            "product_id": i,
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(100, 10000), 2),
            "brand": f"Brand_{random.randint(1,20)}",
            "description": f"Product {i} description"
        })
    return products

@app.route("/products", methods=["GET"])
def get_products():
    return jsonify(generate_products())

if __name__ == "__main__":
    # run: python api/product_api.py
    app.run(host="0.0.0.0", port=5000)