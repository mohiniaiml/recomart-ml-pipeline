import sqlite3
import json
import os
from src.config.config_loader import load_config

config = load_config()

data_lake_path = config["paths"]["data_lake"]
DATA_LAKE = os.path.join(data_lake_path, "raw")
DB_PATH = config["paths"]["features_db"]

METADATA_PATH = os.path.join(os.path.dirname(__file__), "metadata.json")


class FeatureStore:

    def __init__(self, version="v1"):
        self.version = version
        self.conn = sqlite3.connect(DB_PATH)

        with open(METADATA_PATH, "r") as f:
            self.metadata = json.load(f)

    def get_user_features(self, user_id):
        query = f"""
        SELECT * FROM user_features
        WHERE user_id = {user_id}
        """
        return self._fetch_one(query)

    def get_item_features(self, product_id):
        query = f"""
        SELECT * FROM item_features
        WHERE product_id = {product_id}
        """
        return self._fetch_one(query)

    def get_interaction_features(self, user_id, product_id):
        query = f"""
        SELECT * FROM interaction_features
        WHERE user_id = {user_id} AND product_id = {product_id}
        """
        return self._fetch_one(query)

    def get_feature_metadata(self):
        return self.metadata

    def _fetch_one(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        columns = [col[0] for col in cursor.description]

        if row:
            return dict(zip(columns, row))
        return None

    def close(self):
        self.conn.close()