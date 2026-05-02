# ingestion/process_clickstream_stream.py
import os, json, time
import pandas as pd
from src.common.utils import ensure_dir, today_partition
from src.lineage.lineage_logger import log_lineage
from src.config.config_loader import load_config

config = load_config()

simulator_output = config["paths"]["simulator_output"]
VERSION = config["versions"]["clickstream"]
SRC_FILE = os.path.join(simulator_output, "clickstream_events.json")

data_lake_path = config["paths"]["data_lake"]
DL_BASE = os.path.join(data_lake_path, "raw", "clickstream", VERSION)

# simple offset tracking
OFFSET_FILE = "ingestion/.clickstream_offset"

def read_offset():
    if not os.path.exists(OFFSET_FILE):
        return 0
    with open(OFFSET_FILE, "r") as f:
        return int(f.read().strip() or 0)

def write_offset(off):
    os.makedirs(os.path.dirname(OFFSET_FILE), exist_ok=True)
    with open(OFFSET_FILE, "w") as f:
        f.write(str(off))

def process_batch(batch_lines):
    if not batch_lines:
        return
    records = []

    for line in batch_lines:
        line = line.strip()

        if not line:
            continue  # skip empty lines

        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON line: {line}")
    df = pd.DataFrame(records)

    part = today_partition()
    out_dir = os.path.join(DL_BASE, part)
    ensure_dir(out_dir)

    out_path = os.path.join(out_dir, "events.csv")
    # append mode
    if os.path.exists(out_path):
        df.to_csv(out_path, mode="a", header=False, index=False)
    else:
        df.to_csv(out_path, index=False)

    log_lineage(
        dataset_name="clickstream",
        version=VERSION,
        source="clickstream_simulator",
        transformation="stream processing",
        output_path=out_path
    )
    print(f"Wrote {len(df)} events -> {out_path}")

def run(poll_sec=5, batch_size=100, loop=False):
    last_off = read_offset()
    print(f"Starting from offset {last_off}")

    while True:
        print(f"In loop {loop}")
        if not os.path.exists(SRC_FILE):
            print(f"Source file doesn't exist {SRC_FILE}")
            time.sleep(poll_sec)
            continue

        with open(SRC_FILE, "r") as f:
            print(f"seeking {last_off}")
            f.seek(last_off)
            lines = []
            for _ in range(batch_size):
                line = f.readline()
                if not line:
                    break
                lines.append(line.strip())

            new_off = f.tell()

        if lines:
            print(f"Processing lines {new_off}")
            process_batch(lines)
            write_offset(new_off)
            last_off = new_off
        else:
            if loop:
                print(f"In loop {loop} sleep")
                time.sleep(poll_sec)
            else:
                break

if __name__ == "__main__":
    write_offset(0)
    run(loop=False)