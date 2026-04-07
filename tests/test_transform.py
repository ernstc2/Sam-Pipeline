import os
import pytest

from sam_pipeline.transform import HEADERS, stream_dat, extract_date, table_name


class TestHeaders:
    def test_headers_count(self):
        assert len(HEADERS) == 61

    def test_headers_first_last(self):
        assert HEADERS[0] == "DUNS"
        assert HEADERS[-1] == "Alt_Govt_Bus_Poc_Title"


class TestStreamDat:
    def test_bom_stripped(self, sample_dat_path):
        """First row's DUNS value must not start with BOM characters."""
        row_num, row = next(stream_dat(sample_dat_path))
        duns = row[0]
        assert not duns.startswith("\ufeff"), "BOM was not stripped"
        assert duns == "0012345678"

    def test_original_header_discarded(self, sample_dat_path):
        """stream_dat should never yield the header row; first yielded row_num is 2."""
        row_num, row = next(stream_dat(sample_dat_path))
        assert row_num == 2
        # The header row has "DUNS" as a label, but row 2 has a 10-digit number
        assert row[0] != "DUNS"

    def test_row_count(self, sample_dat_path):
        """Fixture has 50 data rows (header excluded)."""
        rows = list(stream_dat(sample_dat_path))
        assert len(rows) == 50

    def test_column_count_valid(self, sample_dat_path):
        """Every row in the fixture must have exactly 61 fields."""
        for row_num, row in stream_dat(sample_dat_path):
            assert len(row) == 61, f"Row {row_num} has {len(row)} fields, expected 61"

    def test_column_count_guard(self, tmp_path):
        """A row with wrong column count raises ValueError with row number and counts."""
        bad_file = tmp_path / "bad.dat"
        # Header row (61 fields) + one bad data row (60 fields)
        header = "|".join(["H"] * 61)
        bad_row = "|".join(["X"] * 60)
        bad_file.write_text(f"{header}\n{bad_row}\n", encoding="utf-8-sig")

        with pytest.raises(ValueError, match=r"Row 2.*expected 61.*got 60"):
            list(stream_dat(str(bad_file)))

    def test_leading_zero_duns(self, sample_dat_path):
        """Row 2 DUNS must preserve leading zeros."""
        row_num, row = next(stream_dat(sample_dat_path))
        assert row[0] == "0012345678"

    def test_leading_zero_zip(self, sample_dat_path):
        """Row 2 Zip (index 19) must preserve leading zeros."""
        row_num, row = next(stream_dat(sample_dat_path))
        assert row[19] == "01234"

    def test_quoted_pipe(self, sample_dat_path):
        """Row 4 has a quoted field with an embedded pipe; must parse correctly."""
        rows = list(stream_dat(sample_dat_path))
        # Row 4 is the 3rd yielded row (row_num=4, index=2)
        row_num, row = rows[2]
        assert row_num == 4
        assert len(row) == 61
        # Field at index 11 (Legal_Business_Name) contains a pipe
        assert "|" in row[11]


class TestExtractDate:
    def test_extract_date_valid(self):
        result = extract_date("SAM_PUBLIC_UTF-8_MONTHLY_20260401.ZIP")
        assert result == "20260401"

    def test_extract_date_invalid_date(self):
        """Month 13 is not a valid calendar date."""
        with pytest.raises(ValueError):
            extract_date("SAM_PUBLIC_UTF-8_MONTHLY_20261301.ZIP")

    def test_extract_date_no_date(self):
        """Filename with no 8-digit sequence raises ValueError."""
        with pytest.raises(ValueError, match="No 8-digit date"):
            extract_date("no_date_here.zip")


class TestTableName:
    def test_table_name(self):
        assert table_name("20260401") == "SAM_PUBLIC_MONTHLY_20260401"
