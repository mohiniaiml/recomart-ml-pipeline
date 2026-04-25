import json
import os
from datetime import datetime
from src.config.config_loader import load_config

config = load_config()

lineage_log_folder = config["paths"]["lineage_log_folder"]
LINEAGE_FILE = os.path.join(lineage_log_folder, "lineage_log.json")

def log_lineage(
    dataset_name,
    version,
    source,
    transformation,
    output_path
):
    os.makedirs(os.path.dirname(LINEAGE_FILE), exist_ok=True)

    record = {
        "dataset": dataset_name,
        "version": version,
        "source": source,
        "transformation": transformation,
        "output_path": output_path,
        "timestamp": datetime.now().isoformat()
    }

    # load existing logs
    if os.path.exists(LINEAGE_FILE):
        with open(LINEAGE_FILE, "r") as f:
            data = json.load(f)
    else:
        data = []

    data.append(record)

    with open(LINEAGE_FILE, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Lineage logged for {dataset_name} ({version})")