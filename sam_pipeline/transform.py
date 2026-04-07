import csv
import re
from datetime import datetime


HEADERS = [
    "DUNS", "Blank", "DUNS4", "CageCode", "DODAAC", "SAM_EXTRACT_CODE",
    "Purpose_Reg", "Registration_Date", "Expiration_Date", "LAST_UPDATE_DATE",
    "ACTIVATION_DATE", "Legal_Business_Name", "DBA_Name", "Company_Division",
    "Division_Number", "Address1", "Address2", "City", "State", "Zip", "Zip_4",
    "Country", "Cong_District", "Other", "Business_Start_Date",
    "Fiscal_Year_End_Close_Date", "Corporate_URL", "Entity_Structure",
    "State_Of_Incorp", "Country_Of_Incorp", "Business_Type_Counter",
    "Business_Type", "Primary_NAICS", "NAICS_Code_Counter", "NAICS_Code_String",
    "PSC_Code_Counter", "PSC_Code_String", "Credit_Card_Use",
    "CORRESPONDENCE_FLAG", "MAILING_ADDRESS_LINE_1", "MAILING_ADDRESS_LINE_2",
    "MAILING_ADDRESS_CITY", "MAILING_ADDRESS_ZIP", "MAILING_ADDRESS_ZIP_CODE_4",
    "MAILING_ADDRESS_COUNTRY", "MAILING_ADDRESS_STATE_OR_PROVINCE",
    "Govt_Bus_Poc_First_Name", "Govt_Bus_Poc_Middle_Initial",
    "Govt_Bus_Poc_Last_Name", "Govt_Bus_Poc_Title", "Govt_Bus_Poc_St_Add_1",
    "Govt_Bus_Poc_St_Add_2", "Govt_Bus_Poc_City", "Govt_Bus_Poc_Zip/Postal_Code",
    "Govt_Bus_Poc_Zip_Code_4", "Govt_Bus_Poc_Country_Code", "Govt_Bus_Poc_State",
    "Alt_Govt_Bus_Poc_First_Name", "Alt_Govt_Bus_Poc_Middle_Initial",
    "Alt_Govt_Bus_Poc_Last_Name", "Alt_Govt_Bus_Poc_Title",
]


def stream_dat(path):
    """Yield (row_num, row) tuples from a pipe-delimited .dat file.

    Opens with utf-8-sig to strip BOM, discards the first row (original
    SAM headers), and validates that every data row has exactly 61 fields.
    Raises ValueError on column count mismatch.
    """
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter="|")
        next(reader)  # discard original SAM header row
        for row_num, row in enumerate(reader, start=2):
            if len(row) != len(HEADERS):
                raise ValueError(
                    f"Row {row_num}: expected {len(HEADERS)} columns, got {len(row)}"
                )
            yield (row_num, row)


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
