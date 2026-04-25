from src.features.store.feature_store import FeatureStore

fs = FeatureStore()

# sample retrieval
user = fs.get_user_features(1)
item = fs.get_item_features(10)
interaction = fs.get_interaction_features(1, 10)

print("User Features:", user)
print("Item Features:", item)
print("Interaction Features:", interaction)

# metadata
print("\nMetadata:")
print(fs.get_feature_metadata())

fs.close()