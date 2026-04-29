import json
from datetime import datetime
import os

def log_lineage(stage, source, output, transformations):
    os.makedirs("metadata", exist_ok=True)

    file = "metadata/lineage.json"

    try:
        with open(file, "r") as f:
            data = json.load(f)
    except:
        data = []

    data.append({
        "stage": stage,
        "source": source,
        "output": output,
        "transformations": transformations,
        "timestamp": datetime.now().isoformat()
    })

    with open(file, "w") as f:
        json.dump(data, f, indent=4)

    print("Lineage saved:", stage)