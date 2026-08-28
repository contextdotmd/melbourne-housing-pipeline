"""Load the Melbourne housing CSV into a Parquet landing zone.

Three outputs per run:

* ``landing/``        typed rows that satisfied the contract
* ``rejects/``        rows that did not, each with a reason code and its original text
* ``ingest_receipt/`` one JSON per run recording counts, provenance and timings

A malformed row never fails the load — it is recorded and the load continues. What *does*
fail the load is a header that does not match the contract exactly, because positional
parsing against a drifted header silently mis-assigns every value in the file.

``load_id`` is derived from the source file's SHA-256, so re-running against the same bytes
overwrites the same partition with identical content rather than accumulating copies.
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import re
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

LOADER_VERSION = "0.1.0"

#: The exact header the source file must present, in this order.
EXPECTED_COLUMNS: tuple[str, ...] = (
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

#: Source dates are day-first (``1/04/2017`` is 1 April). Read month-first the file still
#: parses, which is precisely what makes this worth pinning down.
DATE_FORMAT = "%d/%m/%Y"

_POSTCODE = re.compile(r"^\d{3,4}$")

#: Anything earlier than this in a modern auction feed is a parse error, not a historical
#: sale. Kept deliberately loose: an unusually old but possible date is loaded and surfaced
#: through the receipt's date span, rather than silently discarded.
EARLIEST_PLAUSIBLE_EVENT_DATE = dt.date(1900, 1, 1)

# Landing keeps the source's columns and their order, lowercased. Renaming to domain
# vocabulary is staging's job (see CONTEXT.md); landing stays a faithful typed copy.
_LANDING_SCHEMA = pa.schema(
    [
        ("suburb", pa.string()),
        ("address", pa.string()),
        ("rooms", pa.int32()),
        ("type", pa.string()),
        ("price", pa.float64()),
        ("method", pa.string()),
        ("sellerg", pa.string()),
        ("date", pa.date32()),
        ("postcode", pa.string()),
        ("regionname", pa.string()),
        ("propertycount", pa.int32()),
        ("distance", pa.float64()),
        ("councilarea", pa.string()),
        ("_load_id", pa.string()),
        ("_source_row", pa.int32()),
    ]
)

_REJECT_SCHEMA = pa.schema(
    [
        ("source_row", pa.int32()),
        ("reason", pa.string()),
        ("raw_line", pa.string()),
        ("_load_id", pa.string()),
    ]
)


class LoaderError(Exception):
    """Base class for ingestion failures."""


class SchemaContractError(LoaderError):
    """The source header does not match :data:`EXPECTED_COLUMNS`."""


@dataclass(frozen=True)
class LoadResult:
    load_id: str
    receipt: dict
    landing_path: Path
    rejects_path: Path
    receipt_path: Path


class _RowRejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def load_csv(
    source: Path,
    out_root: Path,
    *,
    chunk_size: int = 50_000,
    today: dt.date | None = None,
) -> LoadResult:
    """Load ``source`` into a Parquet landing zone rooted at ``out_root``.

    ``today`` bounds plausible event dates and is injectable so the loader stays
    deterministic under test — a clock read inside the function would make the reject
    classification depend on when the suite happened to run.
    """
    today = today or dt.date.today()
    source = Path(source)
    out_root = Path(out_root)
    started = dt.datetime.now(dt.timezone.utc)
    clock = time.perf_counter()

    raw = source.read_bytes()
    sha256 = hashlib.sha256(raw).hexdigest()
    load_id = sha256[:12]

    landing_path = out_root / "landing" / "listing_outcome" / f"load_id={load_id}" / "part-0.parquet"
    rejects_path = out_root / "rejects" / f"load_id={load_id}" / "part-0.parquet"
    receipt_path = out_root / "ingest_receipt" / f"{load_id}.json"
    for path in (landing_path, rejects_path, receipt_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    rows_read = 0
    rows_loaded = 0
    reasons: Counter[str] = Counter()
    event_date_min: dt.date | None = None
    event_date_max: dt.date | None = None

    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        _assert_header(next(reader, None))

        accepted: list[dict] = []
        rejected: list[dict] = []
        with (
            pq.ParquetWriter(landing_path, _LANDING_SCHEMA) as landing_writer,
            pq.ParquetWriter(rejects_path, _REJECT_SCHEMA) as reject_writer,
        ):
            for source_row, fields in enumerate(reader, start=1):
                if not fields:  # trailing blank line
                    continue
                rows_read += 1
                try:
                    row = _coerce(fields, load_id, source_row, today)
                    accepted.append(row)
                    rows_loaded += 1
                    event_date = row["date"]
                    if event_date_min is None or event_date < event_date_min:
                        event_date_min = event_date
                    if event_date_max is None or event_date > event_date_max:
                        event_date_max = event_date
                except _RowRejected as rejection:
                    reasons[rejection.reason] += 1
                    rejected.append(
                        {
                            "source_row": source_row,
                            "reason": rejection.reason,
                            "raw_line": ",".join(fields),
                            "_load_id": load_id,
                        }
                    )

                if len(accepted) >= chunk_size:
                    _flush(landing_writer, accepted, _LANDING_SCHEMA)
                if len(rejected) >= chunk_size:
                    _flush(reject_writer, rejected, _REJECT_SCHEMA)

            _flush(landing_writer, accepted, _LANDING_SCHEMA)
            _flush(reject_writer, rejected, _REJECT_SCHEMA)

    ended = dt.datetime.now(dt.timezone.utc)
    receipt = {
        "load_id": load_id,
        "loader_version": LOADER_VERSION,
        "source_path": str(source),
        "source_sha256": sha256,
        "source_bytes": len(raw),
        "header_contract": "ok",
        "rows_read": rows_read,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_read - rows_loaded,
        "reject_reasons": dict(sorted(reasons.items())),
        "event_date_min": event_date_min.isoformat() if event_date_min else None,
        "event_date_max": event_date_max.isoformat() if event_date_max else None,
        "landing_path": str(landing_path),
        "rejects_path": str(rejects_path),
        "started_at": _iso(started),
        "ended_at": _iso(ended),
        "duration_s": round(time.perf_counter() - clock, 3),
    }
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")

    return LoadResult(
        load_id=load_id,
        receipt=receipt,
        landing_path=landing_path,
        rejects_path=rejects_path,
        receipt_path=receipt_path,
    )


def _assert_header(header: list[str] | None) -> None:
    if header is None:
        raise SchemaContractError("source file is empty: no header row found")

    actual = tuple(column.strip() for column in header)
    if actual == EXPECTED_COLUMNS:
        return

    missing = [c for c in EXPECTED_COLUMNS if c not in actual]
    extra = [c for c in actual if c not in EXPECTED_COLUMNS]
    problems = []
    if missing:
        problems.append(f"missing {missing}")
    if extra:
        problems.append(f"unexpected {extra}")
    if not problems:
        problems.append(f"wrong order: expected {list(EXPECTED_COLUMNS)}, got {list(actual)}")
    raise SchemaContractError("source header does not match the contract — " + "; ".join(problems))


def _coerce(fields: list[str], load_id: str, source_row: int, today: dt.date) -> dict:
    if len(fields) != len(EXPECTED_COLUMNS):
        raise _RowRejected("wrong_field_count")

    suburb, address, rooms, type_, price, method, sellerg, date, postcode, region, count, distance, council = (
        f.strip() for f in fields
    )

    return {
        "suburb": suburb,
        "address": address,
        "rooms": _int(rooms, "rooms_not_integer"),
        "type": type_,
        "price": _float(price, "price_not_numeric", nullable=True),
        "method": method,
        "sellerg": sellerg,
        "date": _date(date, today),
        "postcode": _postcode(postcode),
        "regionname": region,
        "propertycount": _int(count, "propertycount_not_integer"),
        "distance": _float(distance, "distance_not_numeric", nullable=True),
        "councilarea": council,
        "_load_id": load_id,
        "_source_row": source_row,
    }


def _int(value: str, reason: str) -> int | None:
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        raise _RowRejected(reason) from None


def _float(value: str, reason: str, *, nullable: bool) -> float | None:
    if value == "":
        if nullable:
            return None
        raise _RowRejected(reason)
    try:
        return float(value)
    except ValueError:
        raise _RowRejected(reason) from None


def _date(value: str, today: dt.date) -> dt.date:
    try:
        parsed = dt.datetime.strptime(value, DATE_FORMAT).date()
    except ValueError:
        raise _RowRejected("date_not_parseable") from None

    # A well-formed date can still be impossible. `30/12/2087` parses cleanly and would
    # otherwise sail through typing untouched, landing decades of empty partitions in the
    # date spine and quietly widening every time series.
    if parsed > today:
        raise _RowRejected("date_in_future")
    if parsed < EARLIEST_PLAUSIBLE_EVENT_DATE:
        raise _RowRejected("date_implausible")
    return parsed


def _postcode(value: str) -> str:
    if not _POSTCODE.match(value):
        raise _RowRejected("postcode_invalid")
    return value


def _flush(writer: pq.ParquetWriter, rows: list[dict], schema: pa.Schema) -> None:
    if not rows:
        return
    writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    rows.clear()


def _iso(moment: dt.datetime) -> str:
    return moment.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iter_landing(path: Path) -> Iterator[dict]:  # pragma: no cover - convenience for the REPL
    yield from pq.read_table(path).to_pylist()
