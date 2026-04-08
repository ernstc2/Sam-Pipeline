import io
import logging
import os
import zipfile
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import requests

from sam_pipeline.extract import (
    run,
    _resolve_api_url,
    _download_zip,
    _download_with_retry,
    _extract_dat,
)


@pytest.fixture
def logger():
    log = logging.getLogger("test_extract")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    log.addHandler(logging.StreamHandler())
    return log


@pytest.fixture
def sample_zip(tmp_path):
    """Create a small zip containing a .dat file."""
    dat_content = "HEADER1|HEADER2\nval1|val2\n"
    zip_path = tmp_path / "test_extract.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("SAM_PUBLIC_UTF-8_MONTHLY_20260401.DAT", dat_content)
    return str(zip_path)


@pytest.fixture
def sample_zip_no_dat(tmp_path):
    """Create a zip with no .dat file."""
    zip_path = tmp_path / "no_dat.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("readme.txt", "no dat here")
    return str(zip_path)


@pytest.fixture
def sample_zip_traversal(tmp_path):
    """Create a zip with a path-traversal member name."""
    zip_path = tmp_path / "traversal.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("../../etc/evil.DAT", "malicious content")
    return str(zip_path)


class TestResolveApiUrl:
    def test_builds_url_with_params(self, tmp_config_with_portal, logger):
        config = tmp_config_with_portal
        url = _resolve_api_url(config, logger)
        assert "api.sam.gov/data-services/v1/extracts" in url
        assert "api_key=test_key_123" in url
        assert "fileType=ENTITY" in url
        assert "sensitivity=PUBLIC" in url
        assert "frequency=MONTHLY" in url
        assert "charset=UTF8" in url

    def test_api_key_masked_in_logs(self, tmp_config_with_portal, caplog):
        config = tmp_config_with_portal
        log = logging.getLogger("test_mask")
        log.setLevel(logging.DEBUG)
        with caplog.at_level(logging.DEBUG, logger="test_mask"):
            _resolve_api_url(config, log)
        # The raw key should never appear in logs
        for record in caplog.records:
            assert "test_key_123" not in record.message
        # The mask should appear
        assert any("***" in r.message for r in caplog.records)


class TestDownloadZip:
    @patch("sam_pipeline.extract.requests.get")
    def test_writes_chunks_to_disk(self, mock_get, tmp_path, logger):
        chunk_data = b"x" * 1024
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Length": str(len(chunk_data))}
        mock_resp.iter_content.return_value = [chunk_data]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value.__enter__ = MagicMock(return_value=mock_resp)
        mock_get.return_value.__exit__ = MagicMock(return_value=False)

        dest = str(tmp_path / "out.zip")
        _download_zip("http://example.com/file.zip", dest, logger, 5, 10)
        assert os.path.exists(dest)
        assert os.path.getsize(dest) == 1024

    @patch("sam_pipeline.extract.requests.get")
    def test_logs_progress_every_10mb(self, mock_get, tmp_path, caplog):
        # Create chunks totaling > 10MB to trigger progress logging
        chunk_size = 1024 * 1024  # 1MB
        chunks = [b"x" * chunk_size] * 12  # 12MB total
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Length": str(chunk_size * 12)}
        mock_resp.iter_content.return_value = chunks
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value.__enter__ = MagicMock(return_value=mock_resp)
        mock_get.return_value.__exit__ = MagicMock(return_value=False)

        log = logging.getLogger("test_progress")
        log.setLevel(logging.INFO)
        dest = str(tmp_path / "big.zip")
        with caplog.at_level(logging.INFO, logger="test_progress"):
            _download_zip("http://example.com/big.zip", dest, log, 5, 10)
        progress_msgs = [r for r in caplog.records if "MB" in r.message and "downloaded" in r.message.lower()]
        assert len(progress_msgs) >= 1


class TestDownloadWithRetry:
    @patch("sam_pipeline.extract.time.sleep")
    @patch("sam_pipeline.extract._download_zip")
    def test_succeeds_on_first_try(self, mock_dl, mock_sleep, tmp_path, logger):
        dest = str(tmp_path / "out.zip")
        _download_with_retry("http://example.com/f.zip", dest, logger, 3, 5, 10)
        mock_dl.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("sam_pipeline.extract.time.sleep")
    @patch("sam_pipeline.extract._download_zip")
    def test_retries_on_failure(self, mock_dl, mock_sleep, tmp_path, logger):
        mock_dl.side_effect = [
            requests.exceptions.RequestException("fail 1"),
            None,  # succeeds on second try
        ]
        dest = str(tmp_path / "out.zip")
        _download_with_retry("http://example.com/f.zip", dest, logger, 3, 5, 10)
        assert mock_dl.call_count == 2
        mock_sleep.assert_called_once()

    @patch("sam_pipeline.extract.time.sleep")
    @patch("sam_pipeline.extract._download_zip")
    def test_raises_after_max_retries(self, mock_dl, mock_sleep, tmp_path, logger):
        mock_dl.side_effect = requests.exceptions.RequestException("always fails")
        dest = str(tmp_path / "out.zip")
        with pytest.raises(requests.exceptions.RequestException):
            _download_with_retry("http://example.com/f.zip", dest, logger, 3, 5, 10)
        assert mock_dl.call_count == 3

    @patch("sam_pipeline.extract.time.sleep")
    @patch("sam_pipeline.extract._download_zip")
    def test_exponential_backoff(self, mock_dl, mock_sleep, tmp_path, logger):
        mock_dl.side_effect = [
            requests.exceptions.RequestException("fail 1"),
            requests.exceptions.RequestException("fail 2"),
            None,
        ]
        dest = str(tmp_path / "out.zip")
        _download_with_retry("http://example.com/f.zip", dest, logger, 3, 5, 10)
        # Backoff: 2**1=2, 2**2=4
        assert mock_sleep.call_args_list[0][0][0] == 2
        assert mock_sleep.call_args_list[1][0][0] == 4


class TestExtractDat:
    def test_extracts_dat_file(self, sample_zip, tmp_path, logger):
        dat_path = _extract_dat(sample_zip, str(tmp_path), logger)
        assert os.path.exists(dat_path)
        assert dat_path.endswith(".DAT")
        with open(dat_path) as f:
            content = f.read()
        assert "HEADER1" in content

    def test_raises_if_no_dat(self, sample_zip_no_dat, tmp_path, logger):
        with pytest.raises(RuntimeError, match="No .dat file"):
            _extract_dat(sample_zip_no_dat, str(tmp_path), logger)

    def test_basename_protection(self, sample_zip_traversal, tmp_path, logger):
        """Zip member with path traversal should be extracted safely."""
        dat_path = _extract_dat(sample_zip_traversal, str(tmp_path), logger)
        # File should be in tmp_path, not in ../../etc/
        assert os.path.dirname(os.path.abspath(dat_path)) == str(tmp_path)
        assert os.path.basename(dat_path) == "evil.DAT"


class TestRun:
    @patch("sam_pipeline.extract._download_with_retry")
    @patch("sam_pipeline.extract._extract_dat")
    @patch("sam_pipeline.extract._resolve_api_url")
    def test_run_with_api(self, mock_resolve, mock_extract, mock_dl,
                          tmp_config_with_portal, logger):
        config = tmp_config_with_portal
        mock_resolve.return_value = "http://example.com/sam_extract.zip"
        mock_extract.return_value = "/tmp/extract.DAT"

        result = run(config, logger)
        mock_resolve.assert_called_once()
        mock_dl.assert_called_once()
        mock_extract.assert_called_once()
        assert result == "/tmp/extract.DAT"

    @patch("sam_pipeline.extract._download_with_retry")
    @patch("sam_pipeline.extract._extract_dat")
    def test_run_with_override_url(self, mock_extract, mock_dl,
                                   tmp_config_with_portal, logger):
        config = tmp_config_with_portal
        config["portal"]["download_url"] = "http://direct.example.com/file.zip"
        mock_extract.return_value = "/tmp/extract.DAT"

        result = run(config, logger)
        mock_dl.assert_called_once()
        # The URL passed to download should be the override
        call_args = mock_dl.call_args
        assert call_args[0][0] == "http://direct.example.com/file.zip"
        assert result == "/tmp/extract.DAT"

    def test_run_creates_temp_dir(self, tmp_config_with_portal, logger):
        config = tmp_config_with_portal
        temp_dir = config["download"]["temp_dir"]
        assert not os.path.exists(temp_dir)
        with patch("sam_pipeline.extract._resolve_api_url", return_value="http://example.com/f.zip"), \
             patch("sam_pipeline.extract._download_with_retry"), \
             patch("sam_pipeline.extract._extract_dat", return_value="/tmp/f.DAT"):
            run(config, logger)
        assert os.path.exists(temp_dir)
