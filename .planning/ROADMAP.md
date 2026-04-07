# Roadmap: SAM Pipeline

## Overview

Two phases build a complete ETL pipeline from the ground up. Phase 1 establishes the foundation — the transform layer, database layer, and operational scaffolding — validated against sample data before any web requests are made. Phase 2 adds the extract layer and wires everything into a working end-to-end pipeline. Both phases together deliver a production-ready monthly batch job that safely loads each SAM.gov extract into a new dated SQL Server table.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Foundation** - Config, transform layer, and database layer — validated with sample data
- [ ] **Phase 2: Extract and Assembly** - Portal scraping, download, and full pipeline wired end-to-end

## Phase Details

### Phase 1: Foundation
**Goal**: The pipeline's core data-handling and database-safety logic works correctly against sample data
**Depends on**: Nothing (first phase)
**Requirements**: TF-01, TF-02, TF-03, TF-04, TF-05, TF-06, LD-01, LD-02, LD-03, LD-04, LD-05, LD-06, LD-07, OPS-01, OPS-02, OPS-03
**Success Criteria** (what must be TRUE):
  1. A sample pipe-delimited .dat file (with UTF-8 BOM) is parsed correctly: BOM stripped, original header row discarded, 61 column headers injected, rows split on `|`, and column count validated against the expected 61
  2. The date is parsed from a filename like `SAM_PUBLIC_UTF-8_MONTHLY_20260401.ZIP` and validated as a real calendar date
  3. Running against the Sam database with a sample data file creates a new `SAM_PUBLIC_MONTHLY_YYYYMMDD` table with all columns defined as VARCHAR and all rows inserted with leading zeros preserved on DUNS and Zip fields
  4. If the target table already exists, the run aborts immediately without modifying any data
  5. If the run fails mid-insert, the newly created table is dropped so a clean retry is possible; no existing tables are modified under any circumstance
**Plans:** 3 plans
Plans:
- [x] 01-01-PLAN.md — Project scaffold, config, logging, test fixture
- [x] 01-02-PLAN.md — Transform layer (BOM strip, header injection, column validation, date parsing)
- [ ] 01-03-PLAN.md — Database layer (schema discovery, table creation, bulk insert, cleanup)

### Phase 2: Extract and Assembly
**Goal**: The complete pipeline runs end-to-end: scrapes SAM.gov for the current download link, downloads and extracts the zip, and loads the data using the Phase 1 foundation
**Depends on**: Phase 1
**Requirements**: DL-01, DL-02, DL-03, DL-04, OPS-04
**Success Criteria** (what must be TRUE):
  1. Running `run.bat` (or `python importer.py`) against the live SAM.gov portal discovers the current monthly zip URL, downloads the ~124MB file with progress logged at intervals, and extracts the .dat file
  2. The full pipeline completes without error and a correctly populated `SAM_PUBLIC_MONTHLY_YYYYMMDD` table exists in the Sam database with the expected row count matching the source file
  3. The rotating log file contains timestamps, row counts, and a final success message with total elapsed time
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation | 2/3 | Executing | - |
| 2. Extract and Assembly | 0/TBD | Not started | - |
