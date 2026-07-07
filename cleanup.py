import calendar
import glob
import os
from datetime import datetime


def run(config, logger):
    """Delete old CSV exports that accumulate from the monthly workflow.

    Opt-in and config-driven: does nothing unless [cleanup] csv_dir is set, so
    it is safe to leave wired in on machines that have no CSVs to prune. Only
    files in csv_dir that match csv_pattern AND are older than retention_months
    are removed, and every deletion is logged. A missing directory is a warning,
    not an error.

    Returns the list of deleted file paths.
    """
    if not config.has_section("cleanup"):
        return []

    section = config["cleanup"]
    csv_dir = section.get("csv_dir", "").strip()
    if not csv_dir:
        return []

    pattern = section.get("csv_pattern", "*.csv").strip() or "*.csv"
    retention_months = int(section.get("retention_months", "3"))

    if not os.path.isdir(csv_dir):
        logger.warning("Cleanup: CSV directory not found: %s — skipping", csv_dir)
        return []

    cutoff_ts = _months_ago(datetime.now(), retention_months).timestamp()

    deleted = []
    for path in glob.glob(os.path.join(csv_dir, pattern)):
        if not os.path.isfile(path):
            continue
        if os.path.getmtime(path) >= cutoff_ts:
            continue
        try:
            os.remove(path)
            deleted.append(path)
            logger.info("Cleanup: deleted old CSV %s", os.path.basename(path))
        except OSError as exc:
            logger.warning("Cleanup: could not delete %s: %s", path, exc)

    if deleted:
        logger.info(
            "Cleanup: removed %d CSV file(s) older than %d months from %s",
            len(deleted), retention_months, csv_dir,
        )
    else:
        logger.info(
            "Cleanup: no CSV files older than %d months in %s",
            retention_months, csv_dir,
        )
    return deleted


def _months_ago(dt, months):
    """Return dt shifted back by a whole number of calendar months."""
    index = dt.month - 1 - months
    year = dt.year + index // 12
    month = index % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)
