# common/utils.py
import os
from datetime import datetime

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def today_partition():
    return datetime.now().strftime("%Y-%m-%d")

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
