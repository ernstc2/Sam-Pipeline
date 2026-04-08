import configparser
import logging
import os
import time
from logging.handlers import RotatingFileHandler

from sam_pipeline import extract, transform, db


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
        # Extract: download zip from SAM.gov, extract .dat
        dat_path = extract.run(config, logger)
        zip_path = os.path.join(
            config["download"]["temp_dir"],
            os.path.basename(dat_path).replace(".dat", ".ZIP").replace(".DAT", ".ZIP"),
        )
        logger.info("Extracted .dat: %s", dat_path)

        # Transform: parse date and build table name
        dat_filename = os.path.basename(dat_path)
        date_str = transform.extract_date(dat_filename)
        tbl = transform.table_name(date_str)
        logger.info("Target table: %s", tbl)

        # Load: connect, discover schema, stream rows, bulk insert
        conn = db.connect(config)
        try:
            schema = db.get_template_schema(conn)
            batch_size = int(config["pipeline"]["batch_size"])
            rows = [row for _, row in transform.stream_dat(dat_path)]
            logger.info("Parsed %d rows from %s", len(rows), dat_filename)
            db.load_table(conn, tbl, schema, rows, logger, batch_size=batch_size)
        finally:
            conn.close()

        # Cleanup temp files on success
        for path in (dat_path, zip_path):
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                    logger.info("Removed temp file: %s", path)
                except OSError as exc:
                    logger.warning("Could not remove temp file %s: %s", path, exc)

    except Exception:
        elapsed = time.time() - start_time
        logger.error("Pipeline failed after %.1f seconds", elapsed, exc_info=True)
        try:
            temp_dir = config["download"]["temp_dir"]
            if os.path.exists(temp_dir):
                logger.info("Temp files left for inspection in: %s", os.path.abspath(temp_dir))
        except (KeyError, TypeError):
            pass
        raise

    elapsed = time.time() - start_time
    logger.info("Completed in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
