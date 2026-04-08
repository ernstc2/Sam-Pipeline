import os
import time
import zipfile

import requests


PROGRESS_INTERVAL = 10 * 1024 * 1024  # 10MB


def run(config, logger):
    """Resolve download URL, download zip, extract .dat, return .dat path."""
    temp_dir = config["download"]["temp_dir"]
    os.makedirs(temp_dir, exist_ok=True)

    override = config["portal"].get("download_url", "").strip()
    if override:
        logger.info("Using manual override URL from config")
        url = override
    else:
        url = _resolve_api_url(config, logger)

    zip_filename = _zip_name_from_url(url)
    zip_path = os.path.join(temp_dir, zip_filename)

    max_retries = int(config["download"]["max_retries"])
    connect_timeout = int(config["download"]["connect_timeout"])
    read_timeout = int(config["download"]["read_timeout"])

    _download_with_retry(url, zip_path, logger, max_retries,
                         connect_timeout, read_timeout)

    dat_path = _extract_dat(zip_path, temp_dir, logger)
    return dat_path


def _resolve_api_url(config, logger):
    """Build SAM.gov API URL with query parameters. Masks api_key in logs."""
    base_url = config["portal"]["url"]
    api_key = config["portal"]["api_key"]

    params = (
        f"?api_key={api_key}"
        f"&fileType=ENTITY"
        f"&sensitivity=PUBLIC"
        f"&frequency=MONTHLY"
        f"&charset=UTF8"
    )
    full_url = base_url + params

    masked = full_url.replace(api_key, "***")
    logger.info("Requesting extract list from %s", masked)

    return full_url


def _download_zip(url, dest_path, logger, connect_timeout, read_timeout):
    """Stream-download a zip file, logging progress every 10MB."""
    logger.info("Downloading %s", dest_path)
    start = time.time()

    with requests.get(url, stream=True, timeout=(connect_timeout, read_timeout)) as resp:
        resp.raise_for_status()
        total = resp.headers.get("Content-Length")
        if total:
            logger.info("File size: %.1f MB", int(total) / (1024 * 1024))

        downloaded = 0
        last_logged = 0
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded - last_logged >= PROGRESS_INTERVAL:
                    logger.info("%.0f MB downloaded", downloaded / (1024 * 1024))
                    last_logged = downloaded

    elapsed = time.time() - start
    logger.info("Download complete: %.1f MB in %.1f seconds",
                downloaded / (1024 * 1024), elapsed)


def _download_with_retry(url, dest_path, logger, max_retries,
                         connect_timeout, read_timeout):
    """Retry download with exponential backoff. Save error page on failure."""
    for attempt in range(1, max_retries + 1):
        try:
            _download_zip(url, dest_path, logger, connect_timeout, read_timeout)
            return
        except requests.exceptions.RequestException as exc:
            if attempt < max_retries:
                wait = 2 ** attempt
                logger.warning("Download attempt %d/%d failed: %s -- retrying in %ds",
                               attempt, max_retries, exc, wait)
                time.sleep(wait)
            else:
                _save_error_page(exc, dest_path, logger)
                raise


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


def _zip_name_from_url(url):
    """Derive zip filename from URL, fallback to sam_extract.zip."""
    path = url.split("?")[0].rstrip("/")
    name = path.split("/")[-1]
    if name.lower().endswith(".zip"):
        return name
    return "sam_extract.zip"


def _save_error_page(exc, dest_path, logger):
    """Save the response body (if available) for debugging."""
    try:
        resp = getattr(exc, "response", None)
        if resp is not None and resp.content:
            error_dir = os.path.dirname(dest_path)
            os.makedirs(error_dir, exist_ok=True)
            error_path = os.path.join(error_dir, "last_portal_page.html")
            with open(error_path, "wb") as f:
                f.write(resp.content)
            logger.info("Saved error response to %s", error_path)
    except Exception:
        logger.debug("Could not save error page", exc_info=True)
