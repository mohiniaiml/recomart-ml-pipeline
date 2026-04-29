# common/utils.py
import os
from datetime import datetime
from src.feature_store.registry import register_feature
from src.feature_store.lineage import log_lineage

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def today_partition():
    return datetime.now().strftime("%Y-%m-%d")

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

register_feature("activity_count")
log_lineage("feature_engineering")