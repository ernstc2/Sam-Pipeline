import configparser
import logging
import os
import time
from logging.handlers import RotatingFileHandler

import download, extract, transform, db, cleanup


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
        dat_path, zip_path = extract.run(config, logger)
        logger.info("Extracted .dat: %s", dat_path)

        # Transform: parse date and build table name
        dat_filename = os.path.basename(dat_path)
        date_str = transform.extract_date(dat_filename)
        tbl = transform.table_name(date_str)
        logger.info("Target table: %s", tbl)

        # Load: connect, stream rows, bulk insert
        conn = db.connect(config)
        try:
            if db.table_exists(conn, tbl):
                # Already loaded — not an error. The safety guard forbids
                # overwriting existing data, so there is simply nothing to do.
                existing = db.count_rows(conn, tbl)
                logger.warning(
                    "%s is already loaded (%d rows) — nothing to do. "
                    "To load a new month, put that month's ZIP in the input folder.",
                    tbl, existing,
                )
                already_loaded = True
            else:
                already_loaded = False
                batch_size = int(config["pipeline"]["batch_size"])
                rows = [row for _, row in transform.stream_dat(dat_path)]
                logger.info("Parsed %d rows from %s", len(rows), dat_filename)
                db.load_table(conn, tbl, rows, logger, batch_size=batch_size)
                db.enrich_contact_info(conn, tbl, logger)
                view_name = config.get("pipeline", "current_view", fallback="").strip()
                db.update_current_view(conn, tbl, view_name, logger)
        finally:
            conn.close()

        if not already_loaded:
            # Archive the source ZIP out of input/ so it is not reprocessed
            # next run. Failure here is non-fatal — the load already succeeded.
            try:
                extract.archive_zip(zip_path, config, logger, date_str)
            except OSError as exc:
                logger.warning("Could not archive source ZIP %s: %s", zip_path, exc)

        # Cleanup extracted .dat temp file on success
        if dat_path and os.path.exists(dat_path):
            try:
                os.remove(dat_path)
                logger.info("Removed temp file: %s", dat_path)
            except OSError as exc:
                logger.warning("Could not remove temp file %s: %s", dat_path, exc)

        # Prune old CSV exports that would otherwise pile up (config-driven,
        # opt-in). Never fatal — housekeeping must not fail a good load.
        try:
            cleanup.run(config, logger)
        except Exception as exc:
            logger.warning("CSV cleanup step failed: %s", exc)

    except Exception:
        elapsed = time.time() - start_time
        logger.error("Pipeline failed after %.1f seconds", elapsed, exc_info=True)
        raise

    elapsed = time.time() - start_time
    logger.info("Completed in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
