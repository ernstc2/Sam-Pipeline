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
