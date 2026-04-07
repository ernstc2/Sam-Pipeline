import logging

import pyodbc


logger = logging.getLogger("sam_importer")


def connect(config):
    """Open a pyodbc connection using settings from the [database] config section."""
    db = config["database"]
    driver = db["driver"]
    server = db["server"]
    database = db["database"]
    username = db["username"]
    password = db["password"]
    encrypt = db["encrypt"]
    trust_cert = db["trust_server_certificate"]

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert}"
    )
    return pyodbc.connect(conn_str)


def get_template_schema(conn, template_table="SAM_PUBLIC_MONTHLY_Empty"):
    """Query INFORMATION_SCHEMA.COLUMNS to discover column definitions from the template table."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_NAME = ? "
        "ORDER BY ORDINAL_POSITION",
        (template_table,),
    )
    return cursor.fetchall()


def table_exists(conn, table_name):
    """Check whether a table already exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def build_create_ddl(table_name, schema_rows):
    """Build a CREATE TABLE statement from schema rows returned by get_template_schema."""
    col_defs = []
    for col_name, data_type, max_length in schema_rows:
        if max_length == -1:
            col_defs.append(f"[{col_name}] varchar(max) NULL")
        elif max_length is not None and max_length > 0:
            col_defs.append(f"[{col_name}] varchar({max_length}) NULL")
        else:
            col_defs.append(f"[{col_name}] {data_type} NULL")
    return f"CREATE TABLE [{table_name}] (\n    " + ",\n    ".join(col_defs) + "\n)"


def build_insert_sql(table_name, column_names):
    """Build a parameterized INSERT INTO statement for the given columns."""
    col_list = ", ".join(f"[{c}]" for c in column_names)
    placeholders = ", ".join("?" for _ in column_names)
    return f"INSERT INTO [{table_name}] ({col_list}) VALUES ({placeholders})"


def load_table(conn, table_name, schema_rows, rows, logger, batch_size=10_000):
    """Create a new dated table and bulk insert all rows.

    Safety guarantees:
    - Aborts immediately if the target table already exists (LD-03)
    - Drops the newly created table on any failure (LD-06)
    - Only issues CREATE TABLE and INSERT; never modifies existing objects (LD-07)
    - Validates row count after load (LD-05)
    """
    if table_exists(conn, table_name):
        raise RuntimeError(
            f"Table {table_name} already exists — aborting to protect existing data (LD-03)"
        )

    cursor = conn.cursor()
    table_created = False

    try:
        ddl = build_create_ddl(table_name, schema_rows)
        cursor.execute(ddl)
        conn.commit()
        table_created = True
        logger.info("Created table %s", table_name)

        column_names = [row[0] for row in schema_rows]
        insert_sql = build_insert_sql(table_name, column_names)
        cursor.fast_executemany = True

        total_rows = len(rows)
        for i in range(0, total_rows, batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted = min(i + batch_size, total_rows)
            logger.info("Inserted %d / %d rows", inserted, total_rows)

        # Validate row count (LD-05)
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        db_count = cursor.fetchone()[0]
        if db_count != total_rows:
            raise RuntimeError(
                f"Row count mismatch: loaded {db_count}, expected {total_rows}"
            )
        logger.info("Row count verified: %d rows", db_count)

    except Exception:
        conn.rollback()
        if table_created:
            try:
                cursor.execute(f"DROP TABLE [{table_name}]")
                conn.commit()
                logger.info("Dropped incomplete table %s after failure", table_name)
            except Exception:
                logger.error("Failed to drop table %s during cleanup", table_name)
        raise
