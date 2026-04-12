import configparser
import logging
import os
import time
from logging.handlers import RotatingFileHandler

import download, extract, transform, db


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
        # Download: fetch ZIP from SAM API if key is configured
        if config.has_section("sam_api") and config.get("sam_api", "api_key", fallback=""):
            download.fetch_extract(config, logger)
        else:
            logger.info("No API key configured — expecting manual ZIP in input folder")

        # Extract: find ZIP in input folder, extract .dat
        dat_path = extract.run(config, logger)
        logger.info("Extracted .dat: %s", dat_path)

        # Transform: parse date and build table name
        dat_filename = os.path.basename(dat_path)
        date_str = transform.extract_date(dat_filename)
        tbl = transform.table_name(date_str)
        logger.info("Target table: %s", tbl)

        # Load: connect, stream rows, bulk insert
        conn = db.connect(config)
        try:
            batch_size = int(config["pipeline"]["batch_size"])
            rows = [row for _, row in transform.stream_dat(dat_path)]
            logger.info("Parsed %d rows from %s", len(rows), dat_filename)
            db.load_table(conn, tbl, rows, logger, batch_size=batch_size)
            db.enrich_contact_info(conn, tbl, logger)
        finally:
            conn.close()

        # Cleanup extracted .dat temp file on success
        if dat_path and os.path.exists(dat_path):
            try:
                os.remove(dat_path)
                logger.info("Removed temp file: %s", dat_path)
            except OSError as exc:
                logger.warning("Could not remove temp file %s: %s", dat_path, exc)

    except Exception:
        elapsed = time.time() - start_time
        logger.error("Pipeline failed after %.1f seconds", elapsed, exc_info=True)
        raise

    elapsed = time.time() - start_time
    logger.info("Completed in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
