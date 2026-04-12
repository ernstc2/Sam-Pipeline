# SAM Pipeline

Automates the monthly SAM.gov entity registration data import. Downloads the UTF-8 monthly extract, parses the pipe-delimited `.dat` file, and loads all rows into a new dated table in the `Sam` database on SQL Server.

Replaces steps 1-3 of the manual SAM data update process.

## Requirements

- Python 3.12
- ODBC Driver 18 for SQL Server (already installed on the server)
- Python packages: `pyodbc`, `requests`, `beautifulsoup4`, `lxml`

Install dependencies:

```
pip install pyodbc requests beautifulsoup4 lxml
```

## Setup

1. Copy `config.ini.example` to `config.ini`
2. Fill in your SQL Server credentials in the `[database]` section
3. If using the SAM API for automatic download, add your API key under `[sam_api]`

### Config Sections

| Section      | Purpose                                               |
|--------------|-------------------------------------------------------|
| `[database]` | SQL Server connection (server, database, credentials) |
| `[logging]`  | Log file location, rotation size, backup count        |
| `[pipeline]` | Batch size, input/temp directories, template table    |
| `[sam_api]`  | (Optional) SAM.gov API key for automatic download     |

## Usage

### Option A: Automatic Download (API Key)

If `config.ini` has a valid `[sam_api]` section with an API key, the pipeline will download the latest monthly extract automatically.

```
python importer.py
```

Or double-click `run.bat`.

> **Note:** SAM.gov API keys expire every 90 days. Renew at [sam.gov](https://sam.gov).

### Option B: Manual Download

1. Download the monthly SAM public extract ZIP from [sam.gov](https://sam.gov)
   - File type: ENTITY, Sensitivity: PUBLIC, Frequency: MONTHLY, Charset: UTF-8
2. Place the ZIP file in the `input/` folder
3. Run the pipeline:

```
python importer.py
```

Or double-click `run.bat`.

## What It Does

1. **Download** (optional) - Fetches the latest monthly ZIP from the SAM API
2. **Extract** - Finds the ZIP in `input/`, extracts the `.dat` file to `temp/`
3. **Transform** - Parses the date from the filename, streams and cleans all rows
4. **Load** - Creates `SAM_PUBLIC_MONTHLY_YYYYMMDD` table, bulk inserts all rows
5. **Enrich** - Joins against `SAM_CONTACT_INFO` to populate phone/email columns
6. **Cleanup** - Removes the extracted `.dat` temp file

### Safety

- Only creates new tables (CREATE TABLE + INSERT). Never modifies existing data.
- If the target table already exists, the pipeline aborts immediately.
- If anything fails mid-load, the incomplete table is dropped automatically.
- Row count is verified after insert.

## Scheduling

Use Windows Task Scheduler to run `run.bat` monthly. This matches the TomProject setup.

## Files

| File                 | Purpose                              |
|----------------------|--------------------------------------|
| `importer.py`        | Main entry point, orchestrates steps |
| `download.py`        | SAM API download                     |
| `extract.py`         | ZIP extraction                       |
| `transform.py`       | .dat parsing and data cleaning       |
| `db.py`              | SQL Server connection and bulk load  |
| `config.ini.example` | Configuration template               |
| `run.bat`            | Task Scheduler entry point           |

## Logs

Logs are written to `logs/sam_importer.log` with rotation (default 10MB, 5 backups). Console output mirrors the log.
