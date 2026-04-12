import os

import requests


def fetch_extract(config, logger):
    """Download the latest SAM monthly extract ZIP from the API into input_dir.

    Returns the path to the downloaded ZIP file. Skips download if
    the file already exists (based on Content-Length match).
    """
    api_key = config["sam_api"]["api_key"]
    input_dir = config["pipeline"]["input_dir"]
    os.makedirs(input_dir, exist_ok=True)

    url = "https://api.sam.gov/data-services/v1/extracts"
    params = {
        "api_key": api_key,
        "fileType": "ENTITY",
        "sensitivity": "PUBLIC",
        "frequency": "MONTHLY",
        "charset": "UTF8",
    }

    logger.info("Requesting SAM extract from API...")
    resp = requests.get(url, params=params, stream=True, timeout=60)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    if "zip" not in content_type and "octet-stream" not in content_type:
        resp.close()
        raise RuntimeError(
            f"Unexpected Content-Type from SAM API: {content_type}. "
            f"Check API key validity (keys expire every 90 days)."
        )

    remote_size = int(resp.headers.get("Content-Length", 0))
    logger.info("Remote file size: %.1f MB", remote_size / 1024 / 1024)

    # Use a fixed name — extract.py picks the newest zip anyway
    zip_path = os.path.join(input_dir, "SAM_PUBLIC_MONTHLY_LATEST.ZIP")

    # Skip if we already have a file of the same size
    if os.path.exists(zip_path) and os.path.getsize(zip_path) == remote_size:
        logger.info("ZIP already downloaded (size matches): %s", zip_path)
        resp.close()
        return zip_path

    # Stream download in 1MB chunks
    chunk_size = 1024 * 1024
    downloaded = 0
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded % (10 * chunk_size) == 0 or downloaded == remote_size:
                logger.info(
                    "Downloaded %.1f / %.1f MB",
                    downloaded / 1024 / 1024,
                    remote_size / 1024 / 1024,
                )

    # Verify size
    local_size = os.path.getsize(zip_path)
    if remote_size and local_size != remote_size:
        os.remove(zip_path)
        raise RuntimeError(
            f"Download size mismatch: got {local_size}, expected {remote_size}"
        )

    logger.info("Downloaded %s (%.1f MB)", zip_path, local_size / 1024 / 1024)
    return zip_path
