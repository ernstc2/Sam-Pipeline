# Requirements: SAM Pipeline

**Defined:** 2026-04-06
**Core Value:** Safely load each month's SAM.gov extract into a new dated table without touching any existing data in the database.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Download

- [x] **DL-01**: Pipeline scrapes SAM.gov File Extracts portal to find the current UTF-8 monthly zip link
- [x] **DL-02**: Pipeline downloads the ~124MB zip file over HTTP (no auth required, streaming download)
- [x] **DL-03**: Pipeline logs download progress at intervals during the download
- [x] **DL-04**: Pipeline extracts the .dat file from the downloaded zip

### Transform

- [x] **TF-01**: Pipeline strips the UTF-8 BOM from the .dat file using `utf-8-sig` encoding
- [x] **TF-02**: Pipeline discards the first row of the .dat file (unusable original headers)
- [x] **TF-03**: Pipeline applies the 61 column headers from `SAM Public_Headers.txt` in order
- [x] **TF-04**: Pipeline parses the file as pipe-delimited (`|` separator)
- [x] **TF-05**: Pipeline validates that parsed row width matches the expected 61-column header list (column count guard)
- [x] **TF-06**: Pipeline parses the date from the zip/dat filename (e.g., `SAM_PUBLIC_UTF-8_MONTHLY_20260401.ZIP` → `20260401`) and validates it is a real calendar date

### Load

- [ ] **LD-01**: Pipeline creates a new table named `SAM_PUBLIC_MONTHLY_YYYYMMDD` using the filename-derived date
- [ ] **LD-02**: All columns are created as VARCHAR (no numeric coercion) — DUNS and Zip leading zeros preserved
- [ ] **LD-03**: Pipeline checks if the target table already exists before creating — if it exists, abort immediately
- [ ] **LD-04**: Pipeline bulk inserts all rows using chunked batches with `fast_executemany`
- [ ] **LD-05**: Pipeline validates post-load row count matches rows parsed from the source file
- [ ] **LD-06**: On failure mid-load, pipeline drops the newly-created table so the run can be retried cleanly
- [ ] **LD-07**: Pipeline never modifies, drops, or alters any existing tables, views, or database objects

### Operations

- [x] **OPS-01**: Pipeline uses `config.ini` for server IP, database name, credentials, driver, and pipeline settings
- [x] **OPS-02**: Pipeline writes rotating log files with timestamps, row counts, errors, warnings, and success/failure status
- [x] **OPS-03**: Pipeline logs total elapsed time in the final success message
- [ ] **OPS-04**: `run.bat` provided for Windows Task Scheduler monthly execution

## v2 Requirements

Deferred to future releases. Tracked but not in current roadmap.

### Data Enrichment

- **ENR-01**: County enrichment via `Add_Counties_From_ZipCode` logic
- **ENR-02**: Update `SAM_Current` view to point to the latest table

### Downstream Updates

- **DS-01**: Awards Processor frontend SAM table swap and version bump
- **DS-02**: SAM Mailing List backend update
- **DS-03**: Cage code machining data refresh

### Operational

- **OPS-05**: Dry-run mode (`--dry-run` flag) for pre-flight validation without SQL writes
- **OPS-06**: Email notifications on success/failure

## Out of Scope

| Feature | Reason |
|---------|--------|
| Upsert / merge logic | Safety model is append-only — one new table per run, never touch existing |
| Schema migration of existing tables | ALTER TABLE on historical `SAM_PUBLIC_MONTHLY_*` is dangerous and unnecessary |
| Incremental / delta loads | SAM.gov provides full monthly snapshots, not deltas |
| Data type casting beyond VARCHAR | Downstream views or queries can cast as needed — raw load stays safe |
| Parallel / multi-threaded processing | Single ~124MB file, single table — parallelism adds complexity with no benefit |
| Automatic retry on download failure | Fail fast, log clearly, let operator re-run — retries mask problems |
| GUI or web interface | Scheduled backend process — operators interact via logs and SSMS |
| ORM or migration framework | Raw pyodbc with hand-written DDL matches TomProject and this scope |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| DL-01 | Phase 2 | Complete |
| DL-02 | Phase 2 | Complete |
| DL-03 | Phase 2 | Complete |
| DL-04 | Phase 2 | Complete |
| TF-01 | Phase 1 | Complete |
| TF-02 | Phase 1 | Complete |
| TF-03 | Phase 1 | Complete |
| TF-04 | Phase 1 | Complete |
| TF-05 | Phase 1 | Complete |
| TF-06 | Phase 1 | Complete |
| LD-01 | Phase 1 | Pending |
| LD-02 | Phase 1 | Pending |
| LD-03 | Phase 1 | Pending |
| LD-04 | Phase 1 | Pending |
| LD-05 | Phase 1 | Pending |
| LD-06 | Phase 1 | Pending |
| LD-07 | Phase 1 | Pending |
| OPS-01 | Phase 1 | Complete |
| OPS-02 | Phase 1 | Complete |
| OPS-03 | Phase 1 | Complete |
| OPS-04 | Phase 2 | Pending |

**Coverage:**
- v1 requirements: 21 total
- Mapped to phases: 21
- Unmapped: 0

---
*Requirements defined: 2026-04-06*
*Last updated: 2026-04-07 after Plan 01-02 execution*
