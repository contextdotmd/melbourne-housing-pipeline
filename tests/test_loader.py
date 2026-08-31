"""Ingestion contract.

The loader is the only place that touches the raw CSV, so it is the only place that can
prove nothing was silently lost or silently coerced. These tests fix that contract before
the loader exists.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ingest.loader import EXPECTED_COLUMNS, SchemaContractError, load_csv

HEADER = (
    "Suburb,Address,Rooms,Type,Price,Method,SellerG,Date,"
    "Postcode,Regionname,Propertycount,Distance,CouncilArea"
)
ROW = (
    "Abbotsford,49 Lithgow St,3,h,1490000,S,Jellis,1/04/2017,"
    "3067,Northern Metropolitan,4019,3,Yarra City Council"
)


def write_csv(tmp_path: Path, *lines: str, name: str = "src.csv") -> Path:
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def landing_rows(result) -> list[dict]:
    return pq.read_table(result.landing_path).to_pylist()


def reject_rows(result) -> list[dict]:
    if result.rejects_path is None:
        return []
    return pq.read_table(result.rejects_path).to_pylist()


# --------------------------------------------------------------------------- header contract


def test_expected_columns_are_the_thirteen_from_the_source_file():
    assert EXPECTED_COLUMNS == (
        "Suburb",
        "Address",
        "Rooms",
        "Type",
        "Price",
        "Method",
        "SellerG",
        "Date",
        "Postcode",
        "Regionname",
        "Propertycount",
        "Distance",
        "CouncilArea",
    )


def test_real_header_is_accepted(tmp_path):
    result = load_csv(write_csv(tmp_path, HEADER, ROW), tmp_path / "out")
    assert result.receipt["header_contract"] == "ok"


def test_missing_column_is_rejected_and_names_the_offender(tmp_path):
    src = write_csv(
        tmp_path,
        HEADER.replace(",CouncilArea", ""),
        ROW.rsplit(",", 1)[0],
    )
    with pytest.raises(SchemaContractError) as excinfo:
        load_csv(src, tmp_path / "out")
    assert "CouncilArea" in str(excinfo.value)


def test_extra_column_is_rejected_and_names_the_offender(tmp_path):
    src = write_csv(tmp_path, HEADER + ",Landsize", ROW + ",650")
    with pytest.raises(SchemaContractError) as excinfo:
        load_csv(src, tmp_path / "out")
    assert "Landsize" in str(excinfo.value)


def test_renamed_column_is_rejected_and_names_both_sides(tmp_path):
    src = write_csv(tmp_path, HEADER.replace("SellerG", "Agent"), ROW)
    with pytest.raises(SchemaContractError) as excinfo:
        load_csv(src, tmp_path / "out")
    message = str(excinfo.value)
    assert "Agent" in message and "SellerG" in message


def test_reordered_columns_are_rejected(tmp_path):
    """Order matters: positional parsing would silently mis-assign values."""
    scrambled = HEADER.split(",")
    scrambled[0], scrambled[1] = scrambled[1], scrambled[0]
    src = write_csv(tmp_path, ",".join(scrambled), ROW)
    with pytest.raises(SchemaContractError):
        load_csv(src, tmp_path / "out")


# --------------------------------------------------------------------------- typing


def test_date_is_parsed_day_first(tmp_path):
    result = load_csv(write_csv(tmp_path, HEADER, ROW), tmp_path / "out")
    assert landing_rows(result)[0]["date"] == dt.date(2017, 4, 1)


def test_ambiguous_date_resolves_day_first_not_month_first(tmp_path):
    """4/1/2017 is 4 January. Read month-first it becomes 1 April — same file, wrong year-shape."""
    row = ROW.replace("1/04/2017", "4/1/2017")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out")
    assert landing_rows(result)[0]["date"] == dt.date(2017, 1, 4)


def test_blank_price_becomes_null_never_zero(tmp_path):
    """23% of the source has no price. Coercing those to 0 would drag every average down."""
    row = ROW.replace(",1490000,", ",,")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out")
    assert landing_rows(result)[0]["price"] is None


def test_distance_keeps_decimal_precision(tmp_path):
    """Distance is decimal (17.6). Typed as an integer it silently truncates."""
    row = ROW.replace(",3,Yarra City Council", ",17.6,Yarra City Council")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out")
    assert landing_rows(result)[0]["distance"] == pytest.approx(17.6)


def test_postcode_is_preserved_as_a_label(tmp_path):
    """Postcodes are identifiers, not quantities — leading zeros must survive."""
    row = ROW.replace(",3067,", ",0800,")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out")
    assert landing_rows(result)[0]["postcode"] == "0800"


def test_whitespace_is_not_silently_meaningful(tmp_path):
    row = ROW.replace("Abbotsford", "  Abbotsford  ")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out")
    assert landing_rows(result)[0]["suburb"] == "Abbotsford"


def test_a_future_event_date_is_rejected(tmp_path):
    """A property cannot have sold tomorrow. 30/12/2087 is corruption, not a record."""
    row = ROW.replace("1/04/2017", "30/12/2087")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out", today=dt.date(2018, 10, 13))
    assert reject_rows(result)[0]["reason"] == "date_in_future"


def test_an_implausibly_old_event_date_is_rejected(tmp_path):
    """Pre-1900 in a modern auction feed is a parse error, not a historical sale."""
    row = ROW.replace("1/04/2017", "30/12/1887")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out", today=dt.date(2018, 10, 13))
    assert reject_rows(result)[0]["reason"] == "date_implausible"


def test_an_old_but_plausible_event_date_is_kept(tmp_path):
    """1980 is unusual for this feed but not impossible — keep it and let the gate flag the shift."""
    row = ROW.replace("1/04/2017", "30/12/1980")
    result = load_csv(write_csv(tmp_path, HEADER, row), tmp_path / "out", today=dt.date(2018, 10, 13))
    assert landing_rows(result)[0]["date"] == dt.date(1980, 12, 30)
    assert result.receipt["rows_rejected"] == 0


def test_the_receipt_records_the_event_date_span(tmp_path):
    """So a feed that suddenly reaches back forty years is visible, not silent."""
    rows = [
        ROW.replace("1/04/2017", "1/04/2016"),
        ROW.replace("49 Lithgow St", "50 Lithgow St").replace("1/04/2017", "13/10/2018"),
    ]
    result = load_csv(write_csv(tmp_path, HEADER, *rows), tmp_path / "out", today=dt.date(2020, 1, 1))
    assert result.receipt["event_date_min"] == "2016-04-01"
    assert result.receipt["event_date_max"] == "2018-10-13"


def test_the_event_date_span_is_null_when_nothing_loaded(tmp_path):
    result = load_csv(write_csv(tmp_path, HEADER), tmp_path / "out")
    assert result.receipt["event_date_min"] is None
    assert result.receipt["event_date_max"] is None


# --------------------------------------------------------------------------- rejects


def test_unparseable_row_is_rejected_and_the_load_continues(tmp_path):
    bad = ROW.replace(",1490000,", ",not-a-number,")
    result = load_csv(write_csv(tmp_path, HEADER, ROW, bad, ROW), tmp_path / "out")

    assert result.receipt["rows_read"] == 3
    assert result.receipt["rows_loaded"] == 2
    assert result.receipt["rows_rejected"] == 1
    assert len(landing_rows(result)) == 2


def test_reject_carries_a_reason_code_and_the_original_line(tmp_path):
    bad = ROW.replace(",1490000,", ",not-a-number,")
    result = load_csv(write_csv(tmp_path, HEADER, bad), tmp_path / "out")

    (rejected,) = reject_rows(result)
    assert rejected["reason"] == "price_not_numeric"
    assert "not-a-number" in rejected["raw_line"]
    assert rejected["source_row"] == 1


def test_unparseable_date_is_rejected_not_guessed(tmp_path):
    bad = ROW.replace("1/04/2017", "31/31/2017")
    result = load_csv(write_csv(tmp_path, HEADER, bad), tmp_path / "out")
    assert reject_rows(result)[0]["reason"] == "date_not_parseable"


def test_short_row_is_rejected(tmp_path):
    result = load_csv(write_csv(tmp_path, HEADER, "Abbotsford,49 Lithgow St,3"), tmp_path / "out")
    assert reject_rows(result)[0]["reason"] == "wrong_field_count"


def test_reject_reasons_are_tallied_in_the_receipt(tmp_path):
    result = load_csv(
        write_csv(
            tmp_path,
            HEADER,
            ROW.replace(",1490000,", ",nope,"),
            ROW.replace("1/04/2017", "31/31/2017"),
            ROW,
        ),
        tmp_path / "out",
    )
    assert result.receipt["reject_reasons"] == {
        "price_not_numeric": 1,
        "date_not_parseable": 1,
    }


# --------------------------------------------------------------------------- the receipt


def test_receipt_reconciles_read_against_loaded_plus_rejected(tmp_path):
    """The assertion that catches silent row loss. Everything else trusts this."""
    result = load_csv(
        write_csv(tmp_path, HEADER, ROW, ROW.replace(",1490000,", ",nope,"), ROW),
        tmp_path / "out",
    )
    r = result.receipt
    assert r["rows_read"] == r["rows_loaded"] + r["rows_rejected"]


def test_receipt_records_source_provenance(tmp_path):
    src = write_csv(tmp_path, HEADER, ROW)
    expected_sha = hashlib.sha256(src.read_bytes()).hexdigest()

    result = load_csv(src, tmp_path / "out")

    assert result.receipt["source_sha256"] == expected_sha
    assert result.receipt["source_bytes"] == src.stat().st_size
    assert result.receipt["source_path"].endswith("src.csv")


def test_receipt_is_written_to_disk_as_json(tmp_path):
    out = tmp_path / "out"
    result = load_csv(write_csv(tmp_path, HEADER, ROW), out)

    written = json.loads((out / "ingest_receipt" / f"{result.load_id}.json").read_text())
    assert written == result.receipt


def test_landing_rows_carry_the_load_id_for_lineage(tmp_path):
    result = load_csv(write_csv(tmp_path, HEADER, ROW), tmp_path / "out")
    assert landing_rows(result)[0]["_load_id"] == result.load_id


# --------------------------------------------------------------------------- idempotency


def test_reloading_the_same_file_is_byte_identical(tmp_path):
    """Re-running the pipeline must not create a second copy of the same data."""
    src = write_csv(tmp_path, HEADER, ROW)
    out = tmp_path / "out"

    first = load_csv(src, out)
    digest_a = hashlib.sha256(first.landing_path.read_bytes()).hexdigest()

    second = load_csv(src, out)
    digest_b = hashlib.sha256(second.landing_path.read_bytes()).hexdigest()

    assert first.load_id == second.load_id
    assert first.landing_path == second.landing_path
    assert digest_a == digest_b


def test_a_changed_file_gets_a_different_load_id(tmp_path):
    out = tmp_path / "out"
    first = load_csv(write_csv(tmp_path, HEADER, ROW, name="a.csv"), out)
    second = load_csv(write_csv(tmp_path, HEADER, ROW, ROW, name="b.csv"), out)
    assert first.load_id != second.load_id


# --------------------------------------------------------------------------- edge cases


def test_header_only_file_loads_zero_rows_without_error(tmp_path):
    result = load_csv(write_csv(tmp_path, HEADER), tmp_path / "out")
    assert result.receipt["rows_read"] == 0
    assert result.receipt["rows_loaded"] == 0
    assert landing_rows(result) == []


def test_completely_empty_file_is_a_contract_failure(tmp_path):
    src = tmp_path / "empty.csv"
    src.write_text("", encoding="utf-8")
    with pytest.raises(SchemaContractError):
        load_csv(src, tmp_path / "out")


def test_utf8_bom_does_not_corrupt_the_first_column_name(tmp_path):
    src = tmp_path / "bom.csv"
    src.write_text("﻿" + HEADER + "\n" + ROW + "\n", encoding="utf-8")
    result = load_csv(src, tmp_path / "out")
    assert result.receipt["header_contract"] == "ok"


def test_an_absurdly_large_integer_is_one_rejected_row_not_a_dead_load(tmp_path):
    """The landing columns are int32; an oversized value must reject the row, not crash the flush."""
    bad = ROW.replace(",3,h,", ",99999999999999,h,")
    result = load_csv(write_csv(tmp_path, HEADER, bad, ROW), tmp_path / "out")
    assert reject_rows(result)[0]["reason"] == "rooms_out_of_range"
    assert result.receipt["rows_loaded"] == 1


def test_nan_and_inf_are_not_prices(tmp_path):
    """float() parses 'nan' happily. A NaN price passes every equality test and poisons averages."""
    rows = [ROW.replace(",1490000,", ",nan,"), ROW.replace(",1490000,", ",inf,")]
    result = load_csv(write_csv(tmp_path, HEADER, *rows), tmp_path / "out")
    assert [r["reason"] for r in reject_rows(result)] == ["price_not_finite", "price_not_finite"]


def test_underscored_and_unicode_digits_are_not_numbers(tmp_path):
    """int('1_000') and int('٣') both succeed in Python. Neither is a number a CSV should carry."""
    rows = [ROW.replace(",1490000,", ",1_000,"), ROW.replace(",3,h,", ",٣,h,")]
    result = load_csv(write_csv(tmp_path, HEADER, *rows), tmp_path / "out")
    assert [r["reason"] for r in reject_rows(result)] == ["price_not_numeric", "rooms_not_integer"]


def test_blank_lines_are_counted_not_silently_swallowed(tmp_path):
    """A blank line mid-file is a corruption symptom. It must be visible on the receipt."""
    result = load_csv(write_csv(tmp_path, HEADER, ROW, "", ROW), tmp_path / "out")
    assert result.receipt["rows_blank"] == 1
    assert result.receipt["rows_read"] == 2
    assert result.receipt["rows_read"] == result.receipt["rows_loaded"] + result.receipt["rows_rejected"]


def test_reject_raw_line_round_trips_csv_quoting(tmp_path):
    """A quoted field holding a comma must come back as one field, or the stored line lies."""
    import csv as csv_module

    quoted = 'Abbotsford,"49 Lithgow St, Unit 2",3,h,nope,S,Jellis,1/04/2017,3067,Northern Metropolitan,4019,3,Yarra City Council'
    result = load_csv(write_csv(tmp_path, HEADER, quoted), tmp_path / "out")

    (rejected,) = reject_rows(result)
    (reparsed,) = csv_module.reader([rejected["raw_line"]])
    assert reparsed[1] == "49 Lithgow St, Unit 2"
    assert len(reparsed) == 13


# --------------------------------------------------------------------------- atomicity


def test_a_crash_mid_load_leaves_no_partial_landing_file(tmp_path, monkeypatch):
    """A partial Parquet file with a valid footer reads as committed data. A crash must leave nothing."""
    from ingest import loader as loader_module

    def explode(*args, **kwargs):
        raise RuntimeError("disk gone")

    monkeypatch.setattr(loader_module, "_flush", explode)

    out = tmp_path / "out"
    with pytest.raises(RuntimeError):
        load_csv(write_csv(tmp_path, HEADER, ROW), out)

    assert not list(out.rglob("*.parquet"))
    assert not list(out.rglob("*.inprogress*"))
    assert not list(out.rglob("*.json"))


def test_a_non_utf8_source_fails_as_a_loader_error_and_leaves_nothing(tmp_path):
    """The orchestrator sees a clean contract failure, not a traceback and a half-written partition."""
    from ingest.loader import LoaderError

    src = tmp_path / "latin1.csv"
    src.write_bytes((HEADER + "\n").encode("utf-8") + ROW.replace("Jellis", "Jell\xe9s").encode("latin-1") + b"\n")

    out = tmp_path / "out"
    with pytest.raises(LoaderError):
        load_csv(src, out)
    assert not list(out.rglob("*.parquet"))
    assert not list(out.rglob("*.inprogress*"))


def test_reingesting_identical_bytes_keeps_the_original_receipt(tmp_path):
    """started_at orders loads for restatement survivorship — a re-send must not re-date the load."""
    src = write_csv(tmp_path, HEADER, ROW)
    out = tmp_path / "out"

    first = load_csv(src, out)
    second = load_csv(src, out)

    assert second.receipt == first.receipt


def test_a_corrupt_receipt_is_replaced_not_fatal(tmp_path):
    """A torn receipt must trigger a clean reload, not wedge every future run of that file."""
    src = write_csv(tmp_path, HEADER, ROW)
    out = tmp_path / "out"

    first = load_csv(src, out)
    first.receipt_path.write_text('{"load_id": "trunc', encoding="utf-8")

    second = load_csv(src, out)
    assert second.receipt["rows_loaded"] == 1
    assert json.loads(second.receipt_path.read_text()) == second.receipt


def test_a_new_loader_version_reprocesses_identical_bytes(tmp_path):
    """New coercion rules must never be bypassed by a partition the old loader wrote."""
    src = write_csv(tmp_path, HEADER, ROW)
    out = tmp_path / "out"

    first = load_csv(src, out)
    stale = dict(first.receipt, loader_version="0.0.1")
    first.receipt_path.write_text(json.dumps(stale), encoding="utf-8")

    second = load_csv(src, out)
    assert second.receipt["loader_version"] != "0.0.1"


def test_a_missing_rejects_file_triggers_a_reload(tmp_path):
    """The no-op path must hand back outputs that actually exist."""
    src = write_csv(tmp_path, HEADER, ROW)
    out = tmp_path / "out"

    first = load_csv(src, out)
    first.rejects_path.unlink()

    second = load_csv(src, out)
    assert second.rejects_path.exists()
