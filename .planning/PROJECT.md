# SAM Pipeline

## What This Is

A Python ETL pipeline that automates the monthly SAM.gov entity registration data import. It scrapes the SAM.gov File Extracts portal, downloads the UTF-8 monthly zip (~124MB), parses the pipe-delimited .dat file, and loads all rows into a new dated table in the `Sam` database on SQL Server. Replaces steps 1-3 of a 40+ step manual process that currently uses EmEditor, Access, and SSMS.

## Core Value

Safely load each month's SAM.gov extract into a new dated table without touching any existing data in the database.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Scrape SAM.gov File Extracts page to find current UTF-8 monthly zip link
- [ ] Download the zip file automatically (no login required, public access)
- [ ] Extract and parse the pipe-delimited .dat file from the zip
- [ ] Strip UTF-8 BOM from first line
- [ ] Apply correct column headers (~60+ fields)
- [ ] Store DUNS and Zip fields as text (varchar) to preserve leading zeros
- [ ] Create new table named `SAM_PUBLIC_MONTHLY_YYYYMMDD` (date extracted from filename)
- [ ] Bulk insert all rows using chunked batches with fast_executemany
- [ ] If target table already exists, abort immediately — never overwrite
- [ ] On failure mid-load, drop the newly-created table so it can be retried cleanly
- [ ] Never modify, drop, or alter any existing tables, views, or database objects
- [ ] Rotating log files with errors, warnings, and success messages
- [ ] config.ini for connection settings and pipeline configuration
- [ ] run.bat for Windows Task Scheduler (monthly execution)

### Out of Scope

- County enrichment (`Add_Counties_From_ZipCode`) — future phase
- Updating the `SAM_Current` view — future phase
- Awards Processor frontend updates — downstream application
- SAM Mailing List backend updates — downstream application
- Cage code machining data refresh — downstream application
- Email notifications — logging is sufficient for Phase 1

## Context

### Current Manual Process

The existing workflow (documented in `Update New Sam Data.docx`) involves 40+ manual steps across multiple tools:

1. Navigate to https://www.sam.gov/SAM/pages/public/extracts/samPublicAccessData.jsf
2. Go to Data Access Services > Entity Registration > Public
3. Download the UTF-8 monthly zip (`SAM_PUBLIC_UTF-8_MONTHLY_YYYYMMDD.ZIP`)
4. Extract .dat file, open in EmEditor, strip BOM, add headers, save as text
5. Import into Access .mdb (configure DUNS/Zip as text, skip blank columns)
6. Compact and repair Access DB (hits 2GB limit)
7. Import Access table into SQL Server via SSMS
8. Update SAM_Current view, Awards Processor, SAM Mailing List, etc.

This project automates steps 1-3. Steps 4+ are future phases.

### SQL Server Environment

- **Server:** YOUR_SERVER (SQL Server 13.0.1742.0)
- **Database:** `Sam` (sibling to `DN_Live` used by TomProject)
- **Authentication:** SQL Server auth (user: `tom`)
- **Driver:** ODBC Driver 18 for SQL Server

### Existing Tables in Sam Database

Historical monthly tables are kept, never overwritten:

- `SAM_PUBLIC_MONTHLY_20181202` through `SAM_PUBLIC_MONTHLY_20250601`
- `SAM_PUBLIC_MONTHLY_Empty` (empty template table)
- `Business_Type_Codes`, `Companies_SAM_AllStates`, and related tables

### Data Source

- **Portal:** https://www.sam.gov/SAM/pages/public/extracts/samPublicAccessData.jsf
- **Path:** Data Access Services > Entity Registration > Public
- **File:** `SAM_PUBLIC_UTF-8_MONTHLY_YYYYMMDD.ZIP` (~124MB)
- **Contents:** Pipe-delimited `.dat` file, UTF-8 with BOM
- **Key fields:** DUNS, CageCode, DODAAC, Legal_Business_Name, Address fields, NAICS codes, PSC codes, Points of Contact, registration dates
- **Authentication:** None required (public access)

### Reference Codebase — TomProject

TomProject (`C:\Users\Chris\Desktop\TomProject`) is a sibling ETL pipeline on the same server targeting `DN_Live`. Its proven patterns will be mirrored:

- **Architecture:** `importer.py` (orchestrator) > `extract.py` > `transform.py` > `db.py` + `config.ini`
- **Connection:** pyodbc with ODBC Driver 18, SQL auth (user: tom)
- **Loading:** Streams large files in chunks, bulk inserts in 10K batches with `fast_executemany`
- **Logging:** Rotating log files
- **Scheduling:** `run.bat` for Windows Task Scheduler

### What Differs from TomProject

- **Data source:** SAM.gov portal (requires page scraping) instead of direct URL
- **File format:** Pipe-delimited `.dat` instead of CSV
- **Load strategy:** Create new dated table only (no swap/upsert) — simpler and safer
- **Database:** `Sam` instead of `DN_Live`

## Constraints

- **Safety:** Only CREATE TABLE + INSERT operations. Never DROP, ALTER, UPDATE, or DELETE existing objects
- **Data integrity:** DUNS and Zip must be varchar (leading zeros). BOM must be stripped
- **Tech stack:** Python + pyodbc (matches TomProject, team familiarity)
- **Server:** Must use existing SQL Server at YOUR_SERVER with SQL auth
- **Naming:** Table must follow `SAM_PUBLIC_MONTHLY_YYYYMMDD` convention (matches existing tables)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Mirror TomProject architecture | Same server, same team, proven patterns | — Pending |
| Scrape portal for download link | No stable direct URL; link changes monthly | — Pending |
| Date from filename, not run date | Matches source data identity, more accurate | — Pending |
| Drop new table on failure | Allows clean retry without manual cleanup | — Pending |
| Abort if table exists | Prevents accidental overwrites of historical data | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-06 after initialization*
