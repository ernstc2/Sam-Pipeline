import pytest
from unittest.mock import MagicMock, patch, call

from sam_pipeline.db import (
    connect,
    get_template_schema,
    table_exists,
    build_create_ddl,
    build_insert_sql,
    load_table,
)


# ---------------------------------------------------------------------------
# Sample schema rows used across multiple tests
# ---------------------------------------------------------------------------

SAMPLE_SCHEMA = [
    ("DUNS", "varchar", 9),
    ("Legal_Business_Name", "varchar", 120),
    ("Notes", "varchar", -1),
]


# ---------------------------------------------------------------------------
# build_create_ddl tests
# ---------------------------------------------------------------------------

class TestBuildCreateDdl:

    def test_build_create_ddl_basic(self):
        """DDL contains CREATE TABLE with correct table name and column defs."""
        ddl = build_create_ddl("SAM_PUBLIC_MONTHLY_20260401", SAMPLE_SCHEMA)
        assert "CREATE TABLE" in ddl
        assert "SAM_PUBLIC_MONTHLY_20260401" in ddl
        assert "DUNS" in ddl
        assert "Legal_Business_Name" in ddl
        assert "Notes" in ddl

    def test_build_create_ddl_varchar_max(self):
        """Schema row with max_length=-1 produces varchar(max) NULL."""
        schema = [("Notes", "varchar", -1)]
        ddl = build_create_ddl("TestTable", schema)
        assert "varchar(max) NULL" in ddl

    def test_build_create_ddl_varchar_sized(self):
        """Schema row with positive max_length produces varchar(N) NULL."""
        schema = [("DUNS", "varchar", 9)]
        ddl = build_create_ddl("TestTable", schema)
        assert "varchar(9) NULL" in ddl

    def test_build_create_ddl_non_varchar(self):
        """Schema row with no max_length uses data_type directly."""
        schema = [("SomeInt", "int", None)]
        ddl = build_create_ddl("TestTable", schema)
        assert "int NULL" in ddl

    def test_build_create_ddl_brackets(self):
        """Table name and column names are wrapped in square brackets."""
        ddl = build_create_ddl("SAM_PUBLIC_MONTHLY_20260401", SAMPLE_SCHEMA)
        assert "[SAM_PUBLIC_MONTHLY_20260401]" in ddl
        assert "[DUNS]" in ddl
        assert "[Legal_Business_Name]" in ddl
        assert "[Notes]" in ddl


# ---------------------------------------------------------------------------
# build_insert_sql tests
# ---------------------------------------------------------------------------

class TestBuildInsertSql:

    def test_build_insert_sql(self):
        """INSERT statement has correct column list and placeholder count."""
        sql = build_insert_sql("SAM_PUBLIC_MONTHLY_20260401", ["DUNS", "Name", "Zip"])
        assert "INSERT INTO [SAM_PUBLIC_MONTHLY_20260401]" in sql
        assert "[DUNS]" in sql
        assert "[Name]" in sql
        assert "[Zip]" in sql
        assert sql.count("?") == 3


# ---------------------------------------------------------------------------
# connect tests
# ---------------------------------------------------------------------------

class TestConnect:

    @patch("sam_pipeline.db.pyodbc")
    def test_connect_string(self, mock_pyodbc):
        """connect() builds connection string with all config values."""
        config = {
            "database": {
                "server": "10.0.0.1",
                "database": "Sam",
                "username": "tom",
                "password": "secret",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "yes",
                "trust_server_certificate": "yes",
            }
        }
        connect(config)
        mock_pyodbc.connect.assert_called_once()
        conn_str = mock_pyodbc.connect.call_args[0][0]
        assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str
        assert "SERVER=10.0.0.1" in conn_str
        assert "DATABASE=Sam" in conn_str
        assert "UID=tom" in conn_str
        assert "PWD=secret" in conn_str
        assert "Encrypt=yes" in conn_str
        assert "TrustServerCertificate=yes" in conn_str


# ---------------------------------------------------------------------------
# table_exists tests
# ---------------------------------------------------------------------------

class TestTableExists:

    def test_table_exists_true(self):
        """Returns True when INFORMATION_SCHEMA query returns a row."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)

        result = table_exists(mock_conn, "SAM_PUBLIC_MONTHLY_20260401")
        assert result is True
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INFORMATION_SCHEMA.TABLES" in sql

    def test_table_exists_false(self):
        """Returns False when INFORMATION_SCHEMA query returns None."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        result = table_exists(mock_conn, "SAM_PUBLIC_MONTHLY_20260401")
        assert result is False


# ---------------------------------------------------------------------------
# load_table tests
# ---------------------------------------------------------------------------

class TestLoadTable:

    def _make_mock_conn(self, table_exists_result=False):
        """Create a mock connection with cursor that simulates table_exists."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # table_exists calls cursor().fetchone() — first call for the check
        if table_exists_result:
            mock_cursor.fetchone.return_value = (1,)
        else:
            mock_cursor.fetchone.side_effect = [None, None]
        return mock_conn, mock_cursor

    def test_load_table_aborts_if_exists(self):
        """Raises RuntimeError if table already exists (LD-03)."""
        mock_conn, mock_cursor = self._make_mock_conn(table_exists_result=True)
        logger = MagicMock()
        rows = [("val1", "val2", "val3")]

        with pytest.raises(RuntimeError, match="already exists"):
            load_table(mock_conn, "SAM_PUBLIC_MONTHLY_20260401",
                       SAMPLE_SCHEMA, rows, logger)

    def test_load_table_drops_on_failure(self):
        """Table is dropped when executemany raises after creation (LD-06)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # table_exists check returns None (table doesn't exist)
        mock_cursor.fetchone.return_value = None

        # Make executemany fail to simulate mid-load error
        mock_cursor.executemany.side_effect = Exception("insert failure")
        logger = MagicMock()
        rows = [("val1", "val2", "val3")]

        with pytest.raises(Exception, match="insert failure"):
            load_table(mock_conn, "SAM_PUBLIC_MONTHLY_20260401",
                       SAMPLE_SCHEMA, rows, logger)

        # Verify DROP TABLE was called
        all_execute_calls = [str(c) for c in mock_cursor.execute.call_args_list]
        drop_calls = [c for c in all_execute_calls if "DROP TABLE" in c]
        assert len(drop_calls) > 0, "DROP TABLE should be called on failure after creation"

    def test_load_table_no_drop_if_not_created(self):
        """No DROP TABLE if exception occurs before CREATE TABLE succeeds."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # table_exists returns None (doesn't exist)
        mock_cursor.fetchone.return_value = None

        # Make the CREATE TABLE execute itself fail
        call_count = [0]
        original_side_effect = None

        def execute_side_effect(*args, **kwargs):
            call_count[0] += 1
            # First call is table_exists check, second is CREATE TABLE
            if call_count[0] == 2:
                raise Exception("create failed")

        mock_cursor.execute.side_effect = execute_side_effect
        logger = MagicMock()
        rows = [("val1", "val2", "val3")]

        with pytest.raises(Exception, match="create failed"):
            load_table(mock_conn, "SAM_PUBLIC_MONTHLY_20260401",
                       SAMPLE_SCHEMA, rows, logger)

        # Verify DROP TABLE was NOT called (only 2 execute calls: exists check + failed create)
        all_execute_calls = [str(c) for c in mock_cursor.execute.call_args_list]
        drop_calls = [c for c in all_execute_calls if "DROP TABLE" in c]
        assert len(drop_calls) == 0, "DROP TABLE should not be called if table was never created"

    def test_load_table_commits_per_batch(self):
        """With batch_size=10 and 25 rows, exactly 4 commits (1 CREATE + 3 batches)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # table_exists returns None
        mock_cursor.fetchone.side_effect = [None, (25,)]

        logger = MagicMock()
        rows = [("v1", "v2", "v3") for _ in range(25)]

        load_table(mock_conn, "SAM_PUBLIC_MONTHLY_20260401",
                   SAMPLE_SCHEMA, rows, logger, batch_size=10)

        # 1 commit for CREATE TABLE + 3 commits for batches (10, 10, 5) = 4
        assert mock_conn.commit.call_count == 4


# ---------------------------------------------------------------------------
# Integration tests (require live SQL Server)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestIntegration:

    def test_get_template_schema(self):
        """Queries SAM_PUBLIC_MONTHLY_Empty and returns column definitions."""
        # Requires live SQL Server connection
        pass

    def test_create_and_drop(self):
        """Creates a test table, verifies it exists, drops it."""
        # Requires live SQL Server connection
        pass
