import csv
import re
from datetime import datetime


FIELD_COUNT = 61  # V1 fields preserved in V2 format (positions 0-60)

# .dat positions that are int in the prod table.
# Dates are YYYYMMDD integers; counters and some zips are also int.
INT_POSITIONS = frozenset({
    7, 8, 9, 10,   # Registration_Date, Expiration_Date, LAST_UPDATE_DATE, ACTIVATION_DATE
    22,             # Cong_District
    24,             # Business_Start_Date
    25,             # Fiscal_Year_End_Close_Date
    30, 33, 35,     # Business_Type_Counter, NAICS_Code_Counter, PSC_Code_Counter
    42,             # MAILING_ADDRESS_ZIP
    53,             # Govt_Bus_Poc_Zip/Postal_Code
})

csv.field_size_limit(10 * 1024 * 1024)  # 10MB — SAM fields can be very large


def stream_dat(path):
    """Yield (row_num, row) tuples from a pipe-delimited V2 .dat file.

    Opens with utf-8-sig to strip BOM, skips the BOF metadata line,
    and takes only the first 61 fields from each data row (V2 appends
    extra fields after position 60 that we don't need).
    """
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter="|")
        first_line = next(reader)  # skip BOF metadata line
        if not first_line or not first_line[0].startswith("BOF"):
            raise ValueError(
                f"Expected BOF metadata line, got: {first_line[0][:50]!r}"
            )
        for row_num, row in enumerate(reader, start=2):
            # Skip EOF metadata line at end of file
            if row and row[0].startswith("EOF"):
                break
            if len(row) < FIELD_COUNT:
                raise ValueError(
                    f"Row {row_num}: expected at least {FIELD_COUNT} columns, got {len(row)}"
                )
            cleaned = [v if v else None for v in row[:FIELD_COUNT]]
            clean_row(cleaned)
            yield (row_num, cleaned)


def clean_row(row):
    """Convert int-typed positions to Python int or None.

    Non-numeric values and values outside SQL Server int range
    become None → SQL NULL on insert.
    """
    for pos in INT_POSITIONS:
        val = row[pos]
        if val is not None:
            try:
                n = int(val)
                row[pos] = n if -2_147_483_648 <= n <= 2_147_483_647 else None
            except (ValueError, TypeError):
                row[pos] = None


def extract_date(filename):
    """Extract and validate an 8-digit date from a filename.

    Returns the date string (e.g. '20260401') or raises ValueError
    if no 8-digit sequence is found or the date is not a valid calendar date.
    """
    match = re.search(r"(\d{8})", filename)
    if not match:
        raise ValueError(f"No 8-digit date found in filename: {filename!r}")
    date_str = match.group(1)
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def table_name(date_str):
    """Return the target table name for a given date string."""
    return f"SAM_PUBLIC_MONTHLY_{date_str}"
