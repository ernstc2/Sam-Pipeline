---
phase: 01-foundation
plan: "02"
subsystem: transform
tags: [etl, parsing, validation, tdd]
dependency_graph:
  requires: [01-01]
  provides: [HEADERS, stream_dat, extract_date, table_name]
  affects: [01-03]
tech_stack:
  added: []
  patterns: [generator-streaming, utf8-sig-bom-strip, csv-reader-pipe-delim]
key_files:
  created:
    - sam_pipeline/transform.py
    - tests/test_transform.py
  modified: []
decisions:
  - "D-06: Hardcoded HEADERS list (61 entries) rather than reading SAM Public_Headers.txt at runtime"
metrics:
  duration: "1 min"
  completed: "2026-04-07"
  tasks: 1
  test_count: 14
  test_pass: 14
---

# Phase 1 Plan 02: Transform Layer Summary

Transform module with BOM-stripping pipe-delimited parser, 61-column header constant, column count validation, and filename date extraction -- all tested via TDD against the synthetic fixture.

## What Was Built

### sam_pipeline/transform.py (62 lines)

- **HEADERS**: Hardcoded list of exactly 61 SAM column names matching SAM Public_Headers.txt
- **stream_dat(path)**: Generator that opens with `utf-8-sig` encoding and `newline=""`, discards the original header row, validates every data row has 61 fields, and yields `(row_num, row)` tuples starting at row 2
- **extract_date(filename)**: Extracts 8-digit date via regex, validates it as a real calendar date with `datetime.strptime`, returns the date string
- **table_name(date_str)**: Returns `SAM_PUBLIC_MONTHLY_{date_str}`

### tests/test_transform.py (91 lines)

14 unit tests covering:
- Header count and first/last values
- BOM stripping on first data row
- Original header row discarded (row_num starts at 2)
- Row count matches fixture (50 rows)
- Column count validation on all rows
- Column count guard raises ValueError with row number and counts
- Leading zero preservation in DUNS and Zip fields
- Quoted field with embedded pipe parsed correctly
- Date extraction from valid SAM filenames
- Invalid calendar date rejection (month 13)
- Missing date in filename rejection
- Table name formatting

## Task Commits

| Task | Type | Commit | Description |
|------|------|--------|-------------|
| 1 (RED) | test | d8e3f3d | Failing tests for transform module |
| 1 (GREEN) | feat | 59e0ebb | Implement transform module |

## Deviations from Plan

None -- plan executed exactly as written.

## Verification Results

```
14 passed in 0.07s
```

All tests green. No server dependency required.

## Requirements Covered

| Req ID | Description | Status |
|--------|-------------|--------|
| TF-01 | Strip UTF-8 BOM | Verified (test_bom_stripped) |
| TF-02 | Discard original header row | Verified (test_original_header_discarded) |
| TF-03 | Apply 61 column headers | Verified (test_headers_count, test_headers_first_last) |
| TF-04 | Parse pipe delimiter correctly | Verified (test_quoted_pipe, test_row_count) |
| TF-05 | Column count validation | Verified (test_column_count_valid, test_column_count_guard) |
| TF-06 | Date extraction and validation | Verified (test_extract_date_valid, _invalid, _no_date) |

## Self-Check: PASSED
