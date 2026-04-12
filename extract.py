import glob
import os
import zipfile


def run(config, logger):
    """Find a SAM ZIP in the input folder, extract the .dat, return its path."""
    input_dir = config["pipeline"]["input_dir"]
    temp_dir = config["pipeline"].get("temp_dir", "temp")
    os.makedirs(temp_dir, exist_ok=True)

    zip_path = _find_zip(input_dir, logger)
    dat_path = _extract_dat(zip_path, temp_dir, logger)
    return dat_path


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
