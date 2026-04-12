import logging

import pyodbc


logger = logging.getLogger("sam_importer")


# Table schema matching prod table column order exactly.
# Int columns match prod (dates as YYYYMMDD, counters, some zips).
# nvarchar(max) where data exceeds 255.
TABLE_SCHEMA = [
    ("ID",                                 "int IDENTITY(1,1) NOT"),
    ("DUNS",                               "nvarchar(255)"),
    ("Blank",                              "nvarchar(255)"),
    ("DUNS4",                              "nvarchar(255)"),
    ("CageCode",                           "nvarchar(255)"),
    ("DODAAC",                             "nvarchar(255)"),
    ("SAM_EXTRACT_CODE",                   "nvarchar(255)"),
    ("Purpose_Reg",                        "nvarchar(255)"),
    ("Registration_Date",                  "int"),
    ("Expiration_Date",                    "int"),
    ("LAST_UPDATE_DATE",                   "int"),
    ("ACTIVATION_DATE",                    "int"),
    ("Legal_Business_Name",               "nvarchar(max)"),
    ("DBA_Name",                           "nvarchar(255)"),
    ("Address1",                           "nvarchar(255)"),
    ("Company_Division",                  "nvarchar(max)"),
    ("Division_Number",                    "nvarchar(255)"),
    ("Address2",                           "nvarchar(255)"),
    ("City",                               "nvarchar(255)"),
    ("County",                             "nvarchar(255)"),
    ("State",                              "nvarchar(255)"),
    ("Country",                            "nvarchar(255)"),
    ("Zip",                                "nvarchar(255)"),
    ("Zip_4",                              "nvarchar(255)"),
    ("Business_Start_Date",                "int"),
    ("Cong_District",                      "int"),
    ("Other",                              "nvarchar(255)"),
    ("Fiscal_Year_End_Close_Date",         "int"),
    ("Corporate_URL",                      "nvarchar(255)"),
    ("Country_Of_Incorp",                  "nvarchar(255)"),
    ("Entity_Structure",                   "nvarchar(255)"),
    ("State_Of_Incorp",                    "nvarchar(255)"),
    ("Business_Type_Counter",              "int"),
    ("Business_Type",                      "nvarchar(255)"),
    ("Primary_NAICS",                      "nvarchar(255)"),
    ("NAICS_Code_Counter",                 "int"),
    ("NAICS_Code_String",                 "nvarchar(max)"),
    ("Credit_Card_Use",                    "nvarchar(255)"),
    ("PSC_Code_Counter",                   "int"),
    ("PSC_Code_String",                   "nvarchar(max)"),
    ("CORRESPONDENCE_FLAG",                "nvarchar(255)"),
    ("MAILING_ADDRESS_CITY",               "nvarchar(255)"),
    ("MAILING_ADDRESS_LINE_1",             "nvarchar(255)"),
    ("MAILING_ADDRESS_LINE_2",             "nvarchar(255)"),
    ("MAILING_ADDRESS_COUNTRY",            "nvarchar(255)"),
    ("MAILING_ADDRESS_ZIP",                "int"),
    ("MAILING_ADDRESS_ZIP_CODE_4",         "nvarchar(255)"),
    ("MAILING_ADDRESS_STATE_OR_PROVINCE",  "nvarchar(255)"),
    ("Govt_Bus_Poc_First_Name",            "nvarchar(255)"),
    ("Govt_Bus_Poc_Middle_Initial",       "nvarchar(max)"),
    ("Govt_Bus_Poc_Last_Name",             "nvarchar(255)"),
    ("Govt_Bus_Poc_Title",                "nvarchar(max)"),
    ("Govt_Bus_Poc_St_Add_1",              "nvarchar(255)"),
    ("Govt_Bus_Poc_St_Add_2",              "nvarchar(255)"),
    ("Govt_Bus_Poc_City",                  "nvarchar(255)"),
    ("Govt_Bus_Poc_Country_Code",          "nvarchar(255)"),
    ("Govt_Bus_Poc_Zip/Postal_Code",       "int"),
    ("Govt_Bus_Poc_Zip_Code_4",            "nvarchar(255)"),
    ("Govt_Bus_Poc_State",                 "nvarchar(255)"),
    ("Alt_Govt_Bus_Poc_First_Name",        "nvarchar(255)"),
    ("Alt_Govt_Bus_Poc_Middle_Initial",    "nvarchar(255)"),
    ("Alt_Govt_Bus_Poc_Last_Name",         "nvarchar(255)"),
    ("Alt_Govt_Bus_Poc_Title",             "nvarchar(255)"),
    ("Govt_Bus_Poc_US_Phone",              "nvarchar(255)"),
    ("Govt_Bus_Poc_US_Phone_Ext",          "nvarchar(255)"),
    ("Govt_Bus_Poc_Non_US_Phone",          "nvarchar(255)"),
    ("Govt_Bus_Poc_Fax",                   "nvarchar(255)"),
    ("Govt_Bus_Poc_Email",                 "nvarchar(255)"),
]

# Column names in .dat file order (positions 0-60).
# Used for INSERT — maps each ? placeholder to the correct column by name.
# ID and County are NOT in this list (ID is auto-increment, County is always NULL).
INSERT_COLUMNS = [
    "DUNS", "Blank", "DUNS4", "CageCode", "DODAAC",
    "SAM_EXTRACT_CODE", "Purpose_Reg", "Registration_Date",
    "Expiration_Date", "LAST_UPDATE_DATE", "ACTIVATION_DATE",
    "Legal_Business_Name", "DBA_Name", "Company_Division",
    "Division_Number", "Address1", "Address2", "City", "State",
    "Zip", "Zip_4", "Country", "Cong_District", "Other",
    "Business_Start_Date", "Fiscal_Year_End_Close_Date",
    "Corporate_URL", "Entity_Structure", "State_Of_Incorp",
    "Country_Of_Incorp", "Business_Type_Counter", "Business_Type",
    "Primary_NAICS", "NAICS_Code_Counter", "NAICS_Code_String",
    "PSC_Code_Counter", "PSC_Code_String", "Credit_Card_Use",
    "CORRESPONDENCE_FLAG", "MAILING_ADDRESS_LINE_1",
    "MAILING_ADDRESS_LINE_2", "MAILING_ADDRESS_CITY",
    "MAILING_ADDRESS_ZIP", "MAILING_ADDRESS_ZIP_CODE_4",
    "MAILING_ADDRESS_COUNTRY", "MAILING_ADDRESS_STATE_OR_PROVINCE",
    "Govt_Bus_Poc_First_Name", "Govt_Bus_Poc_Middle_Initial",
    "Govt_Bus_Poc_Last_Name", "Govt_Bus_Poc_Title",
    "Govt_Bus_Poc_St_Add_1", "Govt_Bus_Poc_St_Add_2",
    "Govt_Bus_Poc_City", "Govt_Bus_Poc_Zip/Postal_Code",
    "Govt_Bus_Poc_Zip_Code_4", "Govt_Bus_Poc_Country_Code",
    "Govt_Bus_Poc_State", "Alt_Govt_Bus_Poc_First_Name",
    "Alt_Govt_Bus_Poc_Middle_Initial", "Alt_Govt_Bus_Poc_Last_Name",
    "Alt_Govt_Bus_Poc_Title",
]

# Lookup for column type by name (for setinputsizes in INSERT_COLUMNS order)
_TYPE_BY_NAME = {name: typedef for name, typedef in TABLE_SCHEMA}


def _detect_driver():
    """Find the best available SQL Server ODBC driver."""
    available = pyodbc.drivers()
    for candidate in ("ODBC Driver 18 for SQL Server",
                      "ODBC Driver 17 for SQL Server"):
        if candidate in available:
            return candidate
    raise RuntimeError(
        f"No SQL Server ODBC driver found. Available: {available}"
    )


def connect(config):
    """Open a pyodbc connection using settings from the [database] config section."""
    db = config["database"]
    driver = _detect_driver()
    server = db["server"]
    database = db["database"]
    username = db["username"]
    password = db["password"]
    encrypt = db.get("encrypt", "yes")
    trust_cert = db.get("trust_server_certificate", "yes")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert}"
    )
    logger.info("Using driver: %s", driver)
    return pyodbc.connect(conn_str)


def table_exists(conn, table_name):
    """Check whether a table already exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _build_create_ddl(table_name):
    """Build a CREATE TABLE statement matching prod table structure."""
    col_defs = []
    for name, typedef in TABLE_SCHEMA:
        if typedef.startswith("int IDENTITY"):
            col_defs.append(f"[{name}] {typedef} NULL")
        else:
            col_defs.append(f"[{name}] {typedef} NULL")
    return f"CREATE TABLE [{table_name}] (\n    " + ",\n    ".join(col_defs) + "\n)"


def _build_insert_sql(table_name):
    """Build INSERT using .dat column order so row data maps correctly."""
    col_list = ", ".join(f"[{name}]" for name in INSERT_COLUMNS)
    placeholders = ", ".join("?" for _ in INSERT_COLUMNS)
    return f"INSERT INTO [{table_name}] ({col_list}) VALUES ({placeholders})"


def _input_sizes():
    """Build setinputsizes in INSERT_COLUMNS order for fast_executemany.

    Int columns use SQL_INTEGER since transform.clean_row converts them
    to Python int before insert. Text columns use SQL_WVARCHAR.
    """
    sizes = []
    for name in INSERT_COLUMNS:
        typedef = _TYPE_BY_NAME[name]
        if "max" in typedef:
            sizes.append((pyodbc.SQL_WVARCHAR, 0, 0))
        elif typedef == "int":
            sizes.append((pyodbc.SQL_INTEGER, 0, 0))
        else:
            length = int(typedef.split("(")[1].rstrip(")"))
            sizes.append((pyodbc.SQL_WVARCHAR, length, 0))
    return sizes


def load_table(conn, table_name, rows, logger, batch_size=10_000):
    """Create a new dated table and bulk insert all rows.

    Safety guarantees:
    - Aborts immediately if the target table already exists
    - Drops the newly created table on any failure
    - Only issues CREATE TABLE and INSERT; never modifies existing objects
    - Validates row count after load
    """
    if table_exists(conn, table_name):
        raise RuntimeError(
            f"Table {table_name} already exists — aborting to protect existing data"
        )

    cursor = conn.cursor()
    table_created = False

    try:
        ddl = _build_create_ddl(table_name)
        cursor.execute(ddl)
        conn.commit()
        table_created = True
        logger.info("Created table %s", table_name)

        insert_sql = _build_insert_sql(table_name)
        cursor.fast_executemany = True
        cursor.setinputsizes(_input_sizes())

        total_rows = len(rows)
        for i in range(0, total_rows, batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted = min(i + batch_size, total_rows)
            logger.info("Inserted %d / %d rows", inserted, total_rows)

        # Validate row count
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        db_count = cursor.fetchone()[0]
        if db_count != total_rows:
            raise RuntimeError(
                f"Row count mismatch: loaded {db_count}, expected {total_rows}"
            )
        logger.info("Row count verified: %d rows", db_count)

        # Nonclustered indexes (matching prod)
        for col in ("ID", "CageCode"):
            idx = f"IX_{table_name}_{col}"
            cursor.execute(
                f"CREATE NONCLUSTERED INDEX [{idx}] ON [{table_name}] ([{col}])"
            )
            logger.info("Created index %s", idx)
        conn.commit()

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


CONTACT_COLUMNS = [
    "Govt_Bus_Poc_US_Phone",
    "Govt_Bus_Poc_US_Phone_Ext",
    "Govt_Bus_Poc_Non_US_Phone",
    "Govt_Bus_Poc_Fax",
    "Govt_Bus_Poc_Email",
]


def enrich_contact_info(conn, table_name, logger):
    """Populate phone/email columns from the SAM_CONTACT_INFO lookup table.

    Joins on CageCode. Rows without a match keep NULL values.
    """
    cursor = conn.cursor()

    # Verify lookup table exists
    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
        ("SAM_CONTACT_INFO",),
    )
    if cursor.fetchone() is None:
        logger.warning("SAM_CONTACT_INFO table not found — skipping contact enrichment")
        return 0

    set_clause = ", ".join(
        f"t.[{col}] = c.[{col}]" for col in CONTACT_COLUMNS
    )
    sql = (
        f"UPDATE t SET {set_clause} "
        f"FROM [{table_name}] t "
        f"INNER JOIN [SAM_CONTACT_INFO] c ON t.[CageCode] = c.[CageCode]"
    )
    cursor.execute(sql)
    updated = cursor.rowcount
    conn.commit()
    logger.info("Enriched %d rows with contact info from SAM_CONTACT_INFO", updated)
    return updated
