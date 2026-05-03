import logging
import os
from datetime import datetime


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(name: str):
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # prevent duplicate handlers

    logger.setLevel(logging.INFO)

    # -----------------------------
    # Formatter
    # -----------------------------
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # -----------------------------
    # Console Handler
    # -----------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # -----------------------------
    # File Handler
    # -----------------------------
    log_file = os.path.join(
        LOG_DIR,
        f"{datetime.now().strftime('%Y-%m-%d')}.log"
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger