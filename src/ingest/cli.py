"""Command-line entry point for ingestion.

    uv run python -m ingest.cli <source.csv> <out_root>

Exits non-zero on a contract failure so the orchestrator sees it, and prints the receipt so
the run's counts are visible in the task log without opening a file.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from ingest.loader import LoaderError, load_csv


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    source, out_root = Path(args[0]), Path(args[1])
    try:
        result = load_csv(source, out_root)
    except LoaderError as error:
        print(f"ingestion failed: {error}", file=sys.stderr)
        return 1

    print(json.dumps(result.receipt, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
