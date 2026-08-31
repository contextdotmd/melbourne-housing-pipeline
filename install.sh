#!/usr/bin/env bash
#
# Bootstrap everything needed to run the pipeline.
#
#   ./install.sh                                  dependencies only
#   ./install.sh --csv ~/Downloads/MELB.csv       dependencies + place the source file
#   ./install.sh --with-airflow                   also build the Airflow environment
#   ./install.sh --yes                            never prompt (CI)
#
# Safe to re-run: every step is idempotent.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

SOURCE_NAME="MELBOURNE_HOUSE_PRICES_LESS.csv"
SOURCE_PATH="data/raw/$SOURCE_NAME"
EXPECTED_ROWS=63023

CSV_ARG=""
WITH_AIRFLOW=0
ASSUME_YES=0

while [ $# -gt 0 ]; do
    case "$1" in
        --csv)          CSV_ARG="${2:-}"; shift 2 ;;
        --csv=*)        CSV_ARG="${1#*=}"; shift ;;
        --with-airflow) WITH_AIRFLOW=1; shift ;;
        --yes|-y)       ASSUME_YES=1; shift ;;
        -h|--help)      sed -n '3,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)              echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'
    YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
    BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; RESET=""
fi

step() { printf '\n%s==>%s %s%s\n' "$BOLD" "$RESET" "$1" "$RESET"; }
ok()   { printf '    %s✓%s %s\n' "$GREEN" "$RESET" "$1"; }
warn() { printf '    %s!%s %s\n' "$YELLOW" "$RESET" "$1"; }
die()  { printf '\n%serror:%s %s\n' "$RED" "$RESET" "$1" >&2; exit 1; }

confirm() {
    [ "$ASSUME_YES" = 1 ] && return 0
    [ -t 0 ] || return 1
    printf '    %s [y/N] ' "$1"
    read -r reply
    case "$reply" in [yY]*) return 0 ;; *) return 1 ;; esac
}

# ---------------------------------------------------------------- prerequisites

step "Checking prerequisites"

if command -v uv >/dev/null 2>&1; then
    ok "uv $(uv --version | awk '{print $2}')"
else
    warn "uv is not installed — it manages the Python version and every dependency."
    if confirm "Install it now from https://astral.sh/uv ?"; then
        curl -LsSf https://astral.sh/uv/install.sh | sh
        # shellcheck disable=SC1091
        [ -f "$HOME/.local/bin/env" ] && . "$HOME/.local/bin/env"
        export PATH="$HOME/.local/bin:$PATH"
        command -v uv >/dev/null 2>&1 || die "uv installed but not on PATH — open a new shell and re-run."
        ok "uv $(uv --version | awk '{print $2}')"
    else
        die "uv is required. Install it with:
      curl -LsSf https://astral.sh/uv/install.sh | sh
    then re-run ./install.sh"
    fi
fi

# uv downloads a matching interpreter itself, so a system Python is not required.
ok "Python: uv will provision $(grep -o 'requires-python = "[^"]*"' pyproject.toml | cut -d'"' -f2)"

# ------------------------------------------------------------------ the source

step "Locating the source data"

if [ -n "$CSV_ARG" ]; then
    [ -f "$CSV_ARG" ] || die "no such file: $CSV_ARG"
    mkdir -p data/raw
    if [ "$(cd "$(dirname "$CSV_ARG")" && pwd)/$(basename "$CSV_ARG")" != "$ROOT/$SOURCE_PATH" ]; then
        cp "$CSV_ARG" "$SOURCE_PATH"
        ok "copied $(basename "$CSV_ARG") → $SOURCE_PATH"
    fi
fi

if [ -f "$SOURCE_PATH" ]; then
    rows=$(( $(wc -l < "$SOURCE_PATH") - 1 ))
    if [ "$rows" -eq "$EXPECTED_ROWS" ]; then
        ok "$SOURCE_PATH — $rows rows"
    else
        warn "$SOURCE_PATH — $rows rows (expected $EXPECTED_ROWS; a different vintage is fine,
      the loader asserts the header contract either way)"
    fi
    HAVE_SOURCE=1
else
    HAVE_SOURCE=0
    warn "$SOURCE_PATH not found."
    printf '      The dataset is not committed. Put the CSV there, or re-run with:\n'
    printf '        %s./install.sh --csv /path/to/%s%s\n' "$DIM" "$SOURCE_NAME" "$RESET"
    printf '      Dependencies will still be installed, and the unit tests run without it.\n'
fi

# ---------------------------------------------------------------- dependencies

step "Installing Python dependencies"
uv sync --group dev
ok "environment ready at .venv"

step "Installing dbt packages"
uv run dbt deps --project-dir dbt --profiles-dir dbt
ok "dbt packages installed"

if [ "$WITH_AIRFLOW" = 1 ]; then
    step "Building the Airflow environment"
    # Airflow's transitive pins conflict with dbt-core's, so it gets its own venv.
    # pyarrow and requests are the project code's own imports: the ingest task runs the
    # loader inside this venv, and the failure notifier posts to Slack with requests.
    uv venv .venv-airflow --python 3.12
    uv pip install --python .venv-airflow "apache-airflow==3.3.1" pytest pyarrow requests \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
    ok "Airflow ready at .venv-airflow"
fi

# ----------------------------------------------------------------- smoke tests

step "Verifying the install"
uv run dbt parse --project-dir dbt --profiles-dir dbt >/dev/null
ok "dbt project parses"
uv run --group dev pytest -q tests/test_loader.py tests/test_quality_gate.py >/dev/null
ok "loader and quality-gate tests pass"

# ------------------------------------------------------------------ next steps

printf '\n%sInstalled.%s ' "$GREEN$BOLD" "$RESET"
if [ "$HAVE_SOURCE" = 1 ]; then
    printf 'Build the warehouse:\n\n    make all\n    make test\n\n'
else
    printf 'Add the source CSV, then build:\n\n    ./install.sh --csv /path/to/%s\n    make all\n\n' "$SOURCE_NAME"
fi
printf '%sEverything else: make help%s\n' "$DIM" "$RESET"
