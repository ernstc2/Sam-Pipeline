import configparser
import logging
import os
import time
from logging.handlers import RotatingFileHandler


def load_config(config_path="config.ini"):
    """Load pipeline configuration from INI file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Copy config.ini.example to config.ini and fill in credentials."
        )
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")
    return config


def setup_logger(config):
    """Configure rotating file logger and console output."""
    log_dir = config["logging"]["log_dir"]
    max_bytes = int(config["logging"]["max_bytes"])
    backup_count = int(config["logging"]["backup_count"])

    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "sam_importer.log")

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger = logging.getLogger("sam_importer")
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def main():
    """Run the SAM data import pipeline."""
    start_time = time.time()
    config = load_config()
    logger = setup_logger(config)
    logger.info("SAM Importer started")

    try:
        # Phase 2 wires extract + transform + load here
        pass
    except Exception:
        elapsed = time.time() - start_time
        logger.error("Pipeline failed after %.1f seconds", elapsed, exc_info=True)
        raise

    elapsed = time.time() - start_time
    logger.info("Completed in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
