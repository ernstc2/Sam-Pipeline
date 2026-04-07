<!-- GSD:project-start source:PROJECT.md -->
## Project

**SAM Pipeline**

A Python ETL pipeline that automates the monthly SAM.gov entity registration data import. It scrapes the SAM.gov File Extracts portal, downloads the UTF-8 monthly zip (~124MB), parses the pipe-delimited .dat file, and loads all rows into a new dated table in the `Sam` database on SQL Server. Replaces steps 1-3 of a 40+ step manual process that currently uses EmEditor, Access, and SSMS.

**Core Value:** Safely load each month's SAM.gov extract into a new dated table without touching any existing data in the database.

### Constraints

- **Safety:** Only CREATE TABLE + INSERT operations. Never DROP, ALTER, UPDATE, or DELETE existing objects
- **Data integrity:** DUNS and Zip must be varchar (leading zeros). BOM must be stripped
- **Tech stack:** Python + pyodbc (matches TomProject, team familiarity)
- **Server:** Must use existing SQL Server at YOUR_SERVER with SQL auth
- **Naming:** Table must follow `SAM_PUBLIC_MONTHLY_YYYYMMDD` convention (matches existing tables)
<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->
## Technology Stack

## Recommended Stack
### Core Runtime
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Python | 3.12.x | Runtime | Latest stable 3.12 branch. Avoid 3.13 — too new, pyodbc and some C-extension packages lag on Windows. 3.12 is the production-safe choice as of mid-2025. |
### HTTP / Scraping
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| requests | >=2.31.0 | Download zip, fetch portal HTML | De-facto standard. Handles streaming large binary downloads (`stream=True`), connection retries, and session management. No async needed — this is a nightly batch job, not a server. |
| beautifulsoup4 | >=4.12.0 | Parse SAM.gov portal HTML to find download link | SAM.gov's extract page renders server-side HTML. BS4 with `html.parser` (stdlib, no extra install) reliably extracts anchor tags. No JavaScript rendering needed — the download link is in static HTML. |
| lxml | >=5.1.0 | Optional faster HTML parser backend for BS4 | Drop-in `features="lxml"` argument to BeautifulSoup. Faster than `html.parser` on large pages, more lenient on malformed markup. Worth installing given no cost. |
### File Handling
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| stdlib `zipfile` | (stdlib) | Extract `.dat` from the downloaded zip | Built in, no dependency. The zip is a simple archive — no need for third-party zip libraries. |
| stdlib `csv` | (stdlib) | Parse pipe-delimited `.dat` file | Built in. `csv.reader(f, delimiter='|')` handles quoted fields, embedded pipes, and line endings correctly. Avoid pandas for parsing — adds 30MB+ dependency with no benefit when rows are streamed directly to SQL. |
| stdlib `codecs` / open with `encoding='utf-8-sig'` | (stdlib) | Strip UTF-8 BOM from first line | `open(path, encoding='utf-8-sig')` transparently strips the BOM on read. One-line fix, no third-party library needed. |
### Database
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| pyodbc | >=5.1.0 | SQL Server connection and bulk insert | Proven in TomProject on this exact server (YOUR_SERVER, SQL Server 13.0, ODBC Driver 18). `fast_executemany=True` on the cursor enables bulk INSERT performance matching BCP for 10K-row batches. Direct match to team's existing knowledge. |
| ODBC Driver 18 for SQL Server | (system install) | ODBC driver on Windows host | Already installed (confirmed by TomProject running). `DRIVER={ODBC Driver 18 for SQL Server}` in connection string. Do not switch to Driver 17 — TomProject uses 18, keep consistent. |
### Configuration and Logging
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| stdlib `configparser` | (stdlib) | `config.ini` for connection string, batch size, portal URL | Matches TomProject pattern. INI format is readable by non-developers who may need to update connection settings. No need for .env files or Pydantic — overkill for a batch script. |
| stdlib `logging` + `RotatingFileHandler` | (stdlib) | Rotating log files | Built in. `RotatingFileHandler(maxBytes=5MB, backupCount=3)` is sufficient. Matches TomProject. No need for structlog or loguru in a single-process batch job. |
### Scheduling
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| `run.bat` | (Windows batch) | Windows Task Scheduler entry point | Matches TomProject. Calls `python importer.py`. Task Scheduler triggers monthly. No Airflow, Prefect, or cron — the orchestration need is a single monthly trigger, not a DAG. |
## What NOT to Use
| Category | Avoid | Why |
|----------|-------|-----|
| DataFrame library | pandas | Adds 30MB+ dependency, encourages loading entire 124MB file into memory. The pipeline streams rows directly from csv.reader to pyodbc batches — pandas adds zero value and real risk of OOM. |
| ORM | SQLAlchemy | Abstracts away `fast_executemany`, complicates the raw DDL needed for dynamic `CREATE TABLE SAM_PUBLIC_MONTHLY_YYYYMMDD`. pyodbc raw cursors are the right tool here. |
| Async HTTP | aiohttp / httpx async | The job runs once a month, downloads one file. Async adds complexity with no throughput benefit for a sequential batch process. |
| Headless browser | Playwright / Selenium | SAM.gov's extract page serves static HTML — the download link is in the page source. No JavaScript execution needed. Adds large system dependency for zero gain. |
| Python 3.13 | cpython 3.13 | Released Oct 2024, pyodbc C extensions and Windows build toolchain compatibility lag. 3.12 is the safe choice. |
| Airflow / Prefect | workflow orchestrators | One job, one schedule, zero dependencies between tasks. Task Scheduler + run.bat is the correct scope. |
| python-dotenv | .env management | config.ini already handles all settings. Two config systems in one project creates confusion. |
| tqdm | progress bars | This runs unattended via Task Scheduler. Progress output goes to log file, not a terminal. Progress bars are meaningless in batch context. |
## Alternatives Considered
| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| HTML parsing | beautifulsoup4 | scrapy | Scrapy is a crawling framework. This pipeline fetches one URL. BS4 is the right scope. |
| DB driver | pyodbc | pymssql | pymssql is unmaintained (archived 2023). pyodbc with ODBC Driver 18 is the Microsoft-supported path. |
| DB driver | pyodbc | sqlalchemy + pyodbc | SQLAlchemy adds an ORM layer not needed here. Raw pyodbc cursor with fast_executemany is faster and simpler for bulk load. |
| Config | configparser (INI) | Pydantic Settings / .env | INI is sufficient and matches existing TomProject convention. Pydantic adds a compile dependency for no gain. |
| File parsing | stdlib csv | pandas read_csv | pandas loads the whole file into memory. Streaming with csv.reader uses ~constant memory regardless of file size. |
## Installation
# Minimal dependencies — most are stdlib
## Confidence Assessment
| Component | Confidence | Notes |
|-----------|------------|-------|
| pyodbc | HIGH | In production on this exact server via TomProject. Known to work with ODBC Driver 18, SQL Server 13.0, fast_executemany. |
| requests | HIGH | Industry standard for HTTP in Python batch scripts. No credible alternative for this use case. |
| beautifulsoup4 | HIGH | Standard for static HTML parsing. SAM.gov extract page is server-rendered HTML — confirmed by the portal URL pattern (.jsf = JavaServer Faces, server-side). |
| stdlib csv / zipfile / logging | HIGH | Stdlib, no version ambiguity. |
| Python 3.12 | MEDIUM | 3.12 is the safe production choice as of mid-2025. Verify 3.13 pyodbc compatibility before upgrading. |
| lxml | MEDIUM | Widely used, but html.parser fallback works fine if lxml install fails on target machine. |
| Version pins (specific numbers) | LOW | Version numbers drawn from training data (Aug 2025 cutoff). Run `pip install requests beautifulsoup4 lxml pyodbc` without pins to get latest, then pin from `pip freeze` after smoke test. |
## Sources
- PROJECT.md: stack constraints (Python + pyodbc, ODBC Driver 18, TomProject mirror)
- TomProject memory: confirmed pyodbc + fast_executemany pattern, rotating logs, run.bat
- Training data (August 2025 cutoff): library version estimates — LOW confidence, verify before pinning
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, or `.github/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
