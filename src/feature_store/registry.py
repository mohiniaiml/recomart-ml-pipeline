import json
from datetime import datetime
import os

def register_feature(name, source, transformation):
    os.makedirs("metadata", exist_ok=True)

    file = "metadata/feature_registry.json"

    try:
        with open(file, "r") as f:
            data = json.load(f)
    except:
        data = {}

    data[name] = {
        "source": source,
        "transformation": transformation,
        "version": datetime.now().isoformat()
    }

    with open(file, "w") as f:
        json.dump(data, f, indent=4)

    print("Feature saved:", name)