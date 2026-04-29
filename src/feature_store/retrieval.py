def get_feature_metadata():
    with open("metadata/feature_registry.json", "r") as f:
        return json.load(f)