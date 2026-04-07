---
phase: 01-foundation
plan: 03
subsystem: database
tags: [pyodbc, sql-server, bulk-insert, schema-discovery, tdd]
dependency_graph:
  requires: [01-01]
  provides: [connect, get_template_schema, table_exists, build_create_ddl, build_insert_sql, load_table]
  affects: [sam_pipeline/db.py, tests/test_db.py]
tech_stack:
  added: [pyodbc]
  patterns: [fast_executemany, INFORMATION_SCHEMA discovery, parameterized queries, batch commit]
key_files:
  created: [sam_pipeline/db.py, tests/test_db.py]
  modified: []
decisions:
  - "All column definitions mirror template table via INFORMATION_SCHEMA — no hardcoded schema"
  - "Batch commits (not single transaction) for bulk insert resilience"
  - "DROP TABLE only targets table created in current run — never existing tables"
metrics:
  duration: 2 min
  completed: "2026-04-07"
  tasks_completed: 1
  tasks_total: 1
  files_created: 2
  files_modified: 0
---

# Phase 1 Plan 03: Database Layer Summary

Runtime schema discovery from SAM_PUBLIC_MONTHLY_Empty template, dated table creation with fast_executemany bulk insert, abort-on-exists guard, and DROP-on-failure cleanup.

## What Was Built

### sam_pipeline/db.py (129 lines)

Six functions providing the complete database layer:

- `connect(config)` — Builds pyodbc connection string from `config["database"]` section with all seven parameters (server, database, username, password, driver, encrypt, trust_server_certificate).
- `get_template_schema(conn, template_table)` — Parameterized INFORMATION_SCHEMA.COLUMNS query ordered by ORDINAL_POSITION. Returns list of (name, type, max_length) tuples.
- `table_exists(conn, table_name)` — Parameterized INFORMATION_SCHEMA.TABLES check. Returns boolean.
- `build_create_ddl(table_name, schema_rows)` — Generates CREATE TABLE with bracketed names. Handles varchar(N), varchar(max) for -1, and raw data_type for non-varchar columns.
- `build_insert_sql(table_name, column_names)` — Generates parameterized INSERT INTO with ? placeholders.
- `load_table(conn, table_name, schema_rows, rows, logger, batch_size)` — Orchestrates the full load with safety guarantees: LD-03 abort if exists, LD-04 fast_executemany, LD-05 row count validation, LD-06 DROP on failure, LD-07 never touches existing tables.

### tests/test_db.py (262 lines)

13 passing offline tests + 2 integration stubs:

- 5 DDL builder tests (basic, varchar_max, varchar_sized, non_varchar, brackets)
- 1 INSERT SQL builder test (column list + placeholder count)
- 1 connect string test (mocked pyodbc.connect, all 7 config values verified)
- 2 table_exists tests (true/false with mocked cursor)
- 4 load_table tests (abort if exists, drop on failure, no drop if not created, commits per batch)
- 2 integration stubs marked @pytest.mark.integration

## Commits

| Commit | Type | Description |
|--------|------|-------------|
| 6e3bd7d | test | Add failing tests for database layer (TDD red) |
| 920e891 | feat | Implement database layer for table creation and bulk insert (TDD green) |

## Requirements Coverage

| Req ID | Status | How |
|--------|--------|-----|
| LD-01 | Covered | build_create_ddl generates CREATE TABLE SAM_PUBLIC_MONTHLY_YYYYMMDD |
| LD-02 | Covered | Schema mirrors template VARCHAR definitions exactly |
| LD-03 | Covered | table_exists check + RuntimeError abort |
| LD-04 | Covered | cursor.fast_executemany = True with batch executemany |
| LD-05 | Covered | SELECT COUNT(*) validation after load |
| LD-06 | Covered | DROP TABLE in except block when table_created is True |
| LD-07 | Covered | Only CREATE TABLE + INSERT; never references other tables |

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. All functions are fully implemented with real logic.

## Verification

```
tests/test_db.py::TestBuildCreateDdl::test_build_create_ddl_basic PASSED
tests/test_db.py::TestBuildCreateDdl::test_build_create_ddl_varchar_max PASSED
tests/test_db.py::TestBuildCreateDdl::test_build_create_ddl_varchar_sized PASSED
tests/test_db.py::TestBuildCreateDdl::test_build_create_ddl_non_varchar PASSED
tests/test_db.py::TestBuildCreateDdl::test_build_create_ddl_brackets PASSED
tests/test_db.py::TestBuildInsertSql::test_build_insert_sql PASSED
tests/test_db.py::TestConnect::test_connect_string PASSED
tests/test_db.py::TestTableExists::test_table_exists_true PASSED
tests/test_db.py::TestTableExists::test_table_exists_false PASSED
tests/test_db.py::TestLoadTable::test_load_table_aborts_if_exists PASSED
tests/test_db.py::TestLoadTable::test_load_table_drops_on_failure PASSED
tests/test_db.py::TestLoadTable::test_load_table_no_drop_if_not_created PASSED
tests/test_db.py::TestLoadTable::test_load_table_commits_per_batch PASSED

13 passed, 2 deselected in 0.13s
```

## Self-Check: PASSED

- FOUND: sam_pipeline/db.py
- FOUND: tests/test_db.py
- FOUND: 6e3bd7d (test commit)
- FOUND: 920e891 (feat commit)
