from src.ingestion.store_to_bronze import move_to_bronze_configurable

if __name__ == "__main__":
    move_to_bronze_configurable("config/config.json")