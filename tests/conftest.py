import os
import configparser
import pytest

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def sample_dat_path():
    return os.path.join(FIXTURE_DIR, "sample_extract.dat")


@pytest.fixture
def tmp_config(tmp_path):
    """Create a temporary config.ini for testing."""
    config = configparser.ConfigParser()
    config["database"] = {
        "server": "127.0.0.1",
        "database": "TestDB",
        "username": "test_user",
        "password": "test_pass",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "yes",
        "trust_server_certificate": "yes",
    }
    config["logging"] = {
        "log_dir": str(tmp_path / "logs"),
        "max_bytes": "1048576",
        "backup_count": "2",
    }
    config["pipeline"] = {
        "batch_size": "100",
        "template_table": "SAM_PUBLIC_MONTHLY_Empty",
    }
    path = tmp_path / "config.ini"
    with open(path, "w") as f:
        config.write(f)
    return str(path)


@pytest.fixture
def tmp_config_with_portal(tmp_path):
    """Create a temporary config.ini with portal and download sections."""
    config = configparser.ConfigParser()
    config["database"] = {
        "server": "127.0.0.1",
        "database": "TestDB",
        "username": "test_user",
        "password": "test_pass",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "yes",
        "trust_server_certificate": "yes",
    }
    config["logging"] = {
        "log_dir": str(tmp_path / "logs"),
        "max_bytes": "1048576",
        "backup_count": "2",
    }
    config["pipeline"] = {
        "batch_size": "100",
        "template_table": "SAM_PUBLIC_MONTHLY_Empty",
    }
    config["portal"] = {
        "url": "https://api.sam.gov/data-services/v1/extracts",
        "api_key": "test_key_123",
    }
    config["download"] = {
        "temp_dir": str(tmp_path / "temp"),
        "connect_timeout": "5",
        "read_timeout": "10",
        "max_retries": "3",
    }
    path = tmp_path / "config.ini"
    with open(path, "w") as f:
        config.write(f)
    return config
