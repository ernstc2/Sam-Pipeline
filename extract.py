import glob
import os
import shutil
import zipfile


def run(config, logger):
    """Find a SAM ZIP in the input folder and extract the .dat.

    Returns a ``(dat_path, zip_path)`` tuple: the extracted .dat in the temp
    directory and the source ZIP it came from, so the caller can archive the
    ZIP after a successful load and avoid reprocessing it next run.
    """
    input_dir = config["pipeline"]["input_dir"]
    temp_dir = config["pipeline"].get("temp_dir", "temp")
    os.makedirs(temp_dir, exist_ok=True)

    zip_path = _find_zip(input_dir, logger)
    dat_path = _extract_dat(zip_path, temp_dir, logger)
    return dat_path, zip_path


def archive_zip(zip_path, config, logger, date_str):
    """Move a loaded source ZIP out of input/ into the archive folder.

    The ZIP is renamed with its load date so there is a record of what was
    loaded and, more importantly, so it is no longer picked up on the next
    run. Never overwrites an existing archived file.
    """
    processed_dir = config["pipeline"].get("processed_dir", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    base, ext = os.path.splitext(os.path.basename(zip_path))
    dest = os.path.join(processed_dir, f"{base}_{date_str}{ext}")
    counter = 1
    while os.path.exists(dest):
        dest = os.path.join(processed_dir, f"{base}_{date_str}_{counter}{ext}")
        counter += 1

    shutil.move(zip_path, dest)
    logger.info("Archived source ZIP to %s", dest)
    return dest


def _find_zip(input_dir, logger):
    """Find the most recent SAM ZIP file in the input directory."""
    if not os.path.isdir(input_dir):
        raise FileNotFoundError(
            f"Input directory not found: {input_dir}\n"
            f"Create the folder and drop the SAM monthly ZIP file into it."
        )

    zips = glob.glob(os.path.join(input_dir, "*.zip"))
    zips += glob.glob(os.path.join(input_dir, "*.ZIP"))
    # Deduplicate (Windows is case-insensitive)
    seen = set()
    unique = []
    for z in zips:
        norm = os.path.normcase(z)
        if norm not in seen:
            seen.add(norm)
            unique.append(z)
    zips = unique

    if not zips:
        raise FileNotFoundError(
            f"No ZIP files found in {input_dir}\n"
            f"Download the monthly SAM extract from sam.gov and place it here."
        )

    newest = max(zips, key=os.path.getmtime)
    if len(zips) > 1:
        logger.warning(
            "Multiple ZIPs in %s — using newest: %s",
            input_dir, os.path.basename(newest),
        )
    logger.info("Found ZIP: %s", os.path.basename(newest))
    return newest


def _extract_dat(zip_path, work_dir, logger):
    """Extract the .dat file from a zip archive. Returns path to .dat file."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            if member.upper().endswith(".DAT"):
                safe_name = os.path.basename(member)
                dest = os.path.join(work_dir, safe_name)
                with zf.open(member) as src, open(dest, "wb") as dst:
                    dst.write(src.read())
                logger.info("Extracted %s", safe_name)
                return dest

    raise RuntimeError(f"No .dat file found in {zip_path}")
