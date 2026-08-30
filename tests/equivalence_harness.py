"""Prove the incremental path and the full-refresh path agree.

An incremental pipeline is only worth having if it cannot drift from the answer a full
rebuild would give. This splits the real source into a sequence of daily files — with a
re-delivered day and a late restatement mixed in, because that is what the feed actually does
— then builds the warehouse twice and compares every table row for row.

    uv run python tests/equivalence_harness.py
"""

from __future__ import annotations

import csv
import datetime as dt
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingest.loader import load_csv  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "raw" / "MELBOURNE_HOUSE_PRICES_LESS.csv"
WORK = Path("/tmp/equivalence")
HEADER = ("Suburb,Address,Rooms,Type,Price,Method,SellerG,Date,"
          "Postcode,Regionname,Propertycount,Distance,CouncilArea")
TODAY = dt.date(2026, 8, 28)

COMPARE = [
    ("staging", "stg__listing_outcome"),
    ("intermediate", "int__listing_outcome_clustered"),
    ("marts", "fact__listing_outcome"),
    ("marts", "dim_suburb"),
    ("marts", "dim_property"),
    ("marts", "dim_agent"),
    ("analytics", "analytics_suburb_monthly_market"),
    ("analytics", "analytics_agent_performance"),
    ("analytics", "analytics_repeat_sales"),
    ("analytics", "analytics_vendor_expectation_gap"),
]


def dbt(*args: str, data_root: Path, db: Path, load_ids: list[str] | None = None) -> None:
    cmd = ["uv", "run", "dbt", *args, "--project-dir", "dbt", "--profiles-dir", "dbt",
           "--vars", f'{{"data_root": "{data_root}"'
                     + (f', "load_ids": {load_ids!r}'.replace("'", '"') if load_ids else "")
                     + "}"]
    env = {**subprocess.os.environ, "DUCKDB_PATH": str(db)}
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env=env)
    if result.returncode:
        print(result.stdout[-4000:], file=sys.stderr)
        raise SystemExit(f"dbt failed: {' '.join(args)}")


def build_daily_files() -> list[Path]:
    """Split the source by event date into five files, then add the two awkward cases."""
    rows = list(csv.DictReader(SOURCE.open(encoding="utf-8-sig")))
    rows.sort(key=lambda r: dt.datetime.strptime(r["Date"], "%d/%m/%Y"))
    size = len(rows) // 5
    chunks = [rows[i * size:(i + 1) * size] for i in range(4)] + [rows[4 * size:]]

    WORK.mkdir(parents=True, exist_ok=True)
    files = []
    for n, chunk in enumerate(chunks, 1):
        path = WORK / f"day{n}.csv"
        write(path, chunk)
        files.append(path)

    # A day re-delivered verbatim. Landing keys on the content hash, so this must be a no-op.
    files.append(files[1])

    # A late restatement: an undisclosed sale from the first file, now with a price.
    restated = next((r for r in chunks[0] if r["Price"].strip() == ""), None)
    if restated:
        fixed = dict(restated, Price="1234567")
        path = WORK / "day6_restatement.csv"
        write(path, [fixed])
        files.append(path)
        print(f"  restating: {fixed['Suburb']} {fixed['Address']} {fixed['Date']} -> 1234567")
    return files


def write(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(HEADER + "\n")
        writer = csv.DictWriter(handle, fieldnames=HEADER.split(","))
        writer.writerows(rows)


def snapshot(db: Path) -> dict[str, list]:
    import duckdb
    con = duckdb.connect(str(db), read_only=True)
    out = {}
    for schema, table in COMPARE:
        cols = [c[0] for c in con.sql(
            f"select column_name from information_schema.columns "
            f"where table_schema='{schema}' and table_name='{table}' "
            f"and column_name not in ('load_id','load_started_at','source_row') "
            f"order by ordinal_position").fetchall()]
        # Round floats before comparing: SUM and AVG are not associative in IEEE-754, so a
        # different grouping order gives 418277.77777777775 against 418277.7777777778. That
        # is arithmetic, not a pipeline difference, and asserting on it would be noise.
        types = dict(con.sql(
            f"select column_name, data_type from information_schema.columns "
            f"where table_schema='{schema}' and table_name='{table}'").fetchall())
        projection = ", ".join(
            f'round("{c}", 6) as "{c}"' if types.get(c) in ("DOUBLE", "FLOAT", "REAL")
            else f'"{c}"'
            for c in cols)
        out[f"{schema}.{table}"] = con.sql(
            f"select {projection} from {schema}.{table} order by all").fetchall()
    con.close()
    return out


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    print("splitting the source into a delivery sequence")
    files = build_daily_files()
    print(f"  {len(files)} deliveries (one re-sent verbatim, one a restatement)\n")

    # ---------------- incremental: load and build, one delivery at a time
    inc_data, inc_db = WORK / "inc_data", WORK / "inc.duckdb"
    print("INCREMENTAL — build after every delivery")
    for n, path in enumerate(files, 1):
        result = load_csv(path, inc_data, today=TODAY)
        dbt("build", data_root=inc_data, db=inc_db, load_ids=[result.load_id])
        print(f"  delivery {n}: {path.name:<22} load {result.load_id}  "
              f"{result.receipt['rows_loaded']:>6,} rows")

    # ---------------- full refresh: same landing zone, rebuilt from scratch
    full_db = WORK / "full.duckdb"
    print("\nFULL REFRESH — same landing zone, rebuilt from nothing")
    dbt("build", "--full-refresh", data_root=inc_data, db=full_db)

    # ---------------- compare
    print("\nCOMPARING")
    a, b = snapshot(inc_db), snapshot(full_db)
    failures = 0
    for name in a:
        same = a[name] == b[name]
        flag = "OK  " if same else "DIFF"
        print(f"  [{flag}] {name:<45} incremental {len(a[name]):>7,} | full {len(b[name]):>7,}")
        if not same:
            failures += 1
            only_inc = [r for r in a[name] if r not in b[name]][:2]
            only_full = [r for r in b[name] if r not in a[name]][:2]
            for r in only_inc:
                print(f"          only in incremental: {str(r)[:150]}")
            for r in only_full:
                print(f"          only in full:        {str(r)[:150]}")
    print()
    if failures:
        print(f"FAILED — {failures} table(s) differ")
        return 1
    print("PASSED — the incremental warehouse is identical to a full rebuild")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
