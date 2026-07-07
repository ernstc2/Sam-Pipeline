"""One-time utility: copy the SAM_CONTACT_INFO lookup table into the live
database so contact enrichment can run there.

The lookup table lives on the source database (the one that already has it);
this copies its structure and rows into the destination (live) database. Run
it once on a machine that can reach both, then it is no longer needed.

Reads two sections from config.ini:
  [database]        destination -- where the table will be created (your live DB)
  [contact_source]  source      -- the DB that already has SAM_CONTACT_INFO

Safe by design: it only CREATEs the new table and INSERTs rows. If
SAM_CONTACT_INFO already exists on the destination it stops without touching
it, and it never modifies the source.
"""
import configparser
import logging
import sys

import pyodbc

import db  # reuse ODBC driver detection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sam_importer")

TABLE = "SAM_CONTACT_INFO"
BATCH_SIZE = 10_000


def _connect(section):
    """Open a pyodbc connection from a config section."""
    driver = db._detect_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={section['server']};"
        f"DATABASE={section['database']};"
        f"UID={section['username']};"
        f"PWD={section['password']};"
        f"Encrypt={section.get('encrypt', 'yes')};"
        f"TrustServerCertificate={section.get('trust_server_certificate', 'yes')}"
    )
    return pyodbc.connect(conn_str)


def _table_exists(conn, name):
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (name,)
    )
    return cur.fetchone() is not None


def _column_defs(conn, name):
    """Return [(column, type_sql), ...] for a table in ordinal order."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? "
        "ORDER BY ORDINAL_POSITION",
        (name,),
    )
    defs = []
    for col, dtype, maxlen in cur.fetchall():
        if dtype in ("varchar", "nvarchar", "char", "nchar") and maxlen is not None:
            length = "max" if maxlen == -1 else str(maxlen)
            defs.append((col, f"{dtype}({length})"))
        else:
            defs.append((col, dtype))
    return defs


def _build_ddl(table, col_defs):
    cols = ",\n    ".join(f"[{c}] {t} NULL" for c, t in col_defs)
    return f"CREATE TABLE [{table}] (\n    {cols}\n)"


def _input_sizes(col_defs):
    sizes = []
    for _, t in col_defs:
        base = t.split("(")[0]
        if base in ("nvarchar", "varchar", "nchar", "char"):
            if "(max)" in t:
                sizes.append((pyodbc.SQL_WVARCHAR, 0, 0))
            else:
                sizes.append((pyodbc.SQL_WVARCHAR, int(t.split("(")[1].rstrip(")")), 0))
        elif base == "int":
            sizes.append((pyodbc.SQL_INTEGER, 0, 0))
        else:
            sizes.append((pyodbc.SQL_WVARCHAR, 0, 0))
    return sizes


def main():
    config = configparser.ConfigParser()
    config.read("config.ini", encoding="utf-8")
    if not config.has_section("contact_source"):
        sys.exit(
            "Missing [contact_source] section in config.ini — add the source DB "
            f"(server/database/username/password) that has {TABLE}."
        )

    src = _connect(config["contact_source"])
    dst = _connect(config["database"])
    try:
        if not _table_exists(src, TABLE):
            sys.exit(f"Source database has no {TABLE} to copy.")
        if _table_exists(dst, TABLE):
            logger.info("%s already exists on the destination — nothing to do.", TABLE)
            return

        col_defs = _column_defs(src, TABLE)
        cols = [c for c, _ in col_defs]
        col_list = ", ".join(f"[{c}]" for c in cols)

        dcur = dst.cursor()
        dcur.execute(_build_ddl(TABLE, col_defs))
        dst.commit()
        logger.info("Created %s on destination (%d columns)", TABLE, len(cols))

        insert_sql = (
            f"INSERT INTO [{TABLE}] ({col_list}) "
            f"VALUES ({', '.join('?' for _ in cols)})"
        )
        dcur.fast_executemany = True
        dcur.setinputsizes(_input_sizes(col_defs))

        scur = src.cursor()
        scur.execute(f"SELECT {col_list} FROM [{TABLE}]")
        total = 0
        while True:
            batch = scur.fetchmany(BATCH_SIZE)
            if not batch:
                break
            dcur.executemany(insert_sql, [list(r) for r in batch])
            dst.commit()
            total += len(batch)
            logger.info("Copied %d rows", total)

        logger.info("Done — %s provisioned with %d rows.", TABLE, total)
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
