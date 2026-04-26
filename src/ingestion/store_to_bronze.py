import os
import shutil
from datetime import datetime
from src.lineage.lineage_logger import log_lineage
from src.config.config_loader import load_config

# Optional cloud import
try:
    import boto3
except:
    boto3 = None


def parse_datetime_folder(folder_name):
    """
    Try parsing datetime folder like:
    2026-04-21 or 20260421 or 2026_04_21
    Fallback to current time if parsing fails
    """
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y_%m_%d"):
        try:
            return datetime.strptime(folder_name, fmt)
        except:
            continue
    return datetime.now()


def move_to_bronze_configurable(config_path="config/config.json"):
    
    config = load_config(config_path)
    storage_type = config["storage"]["type"]
    input_base = config["paths"]["data_lake"]
    versions = config["versions"]

    sources = ["clickstream", "products", "transactions"]

    # Setup storage
    if storage_type == "s3":
        s3 = boto3.client("s3")
        bucket = config["storage"]["bucket"]
        base_path = config["storage"]["base_path"]
    else:
        base_path = config["storage"]["base_path"]

    # -----------------------------
    # Traverse sources
    # -----------------------------
    for source in sources:
        version = versions.get(source, "v1")
        source_path = os.path.join(input_base, "raw", source, version)

        if not os.path.exists(source_path):
            print(f"Skipping {source}, path not found: {source_path}")
            continue
        
        print(f"Processing {source} version: {version}")
        
        # Iterate datetime folders
        for dt_folder in os.listdir(source_path):
            dt_path = os.path.join(source_path, dt_folder)

            if not os.path.isdir(dt_path):
                continue

            dt = parse_datetime_folder(dt_folder)

            # Iterate files inside datetime folder
            for file_name in os.listdir(dt_path):
                file_path = os.path.join(dt_path, file_name)

                if not os.path.isfile(file_path):
                    continue

                # Partition based on datetime folder
                partition_path = (
                    f"source={source}/version={version}/type=raw/"
                    f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
                )

                # -----------------------------
                # LOCAL STORAGE
                # -----------------------------
                if storage_type == "local":
                    target_dir = os.path.join(input_base, base_path, partition_path)
                    os.makedirs(target_dir, exist_ok=True)

                    target_file = os.path.join(target_dir, file_name)
                    shutil.copy(file_path, target_file)

                    print(f"[LOCAL] {file_name} -> {target_dir}")

                    # Lineage logging
                    log_lineage(
                        dataset_name=source,
                        version=version,
                        source=f"raw/{source}/{version}",
                        transformation="bronze partitioning",
                        output_path=target_file
                    )

                # -----------------------------
                # S3 STORAGE
                # -----------------------------
                elif storage_type == "s3":
                    if boto3 is None:
                        raise ImportError("boto3 not installed")

                    s3_key = f"{base_path}/{partition_path}/{file_name}"
                    s3.upload_file(file_path, bucket, s3_key)

                    print(f"[S3] {file_name} -> s3://{bucket}/{s3_key}")

                    # Lineage logging
                    log_lineage(
                        dataset_name=source,
                        version=version,
                        source=f"raw/{source}/{version}",
                        transformation="bronze partitioning",
                        output_path=target_file
                    )

                else:
                    raise ValueError("Unsupported storage type")