#!/usr/bin/env bash
# Functional test harness for the PostgreSQL path.
#
# Brings up a PostgreSQL instance, seeds it, then validates the driver +
# partitioning logic at two levels:
#   1. Rust integration tests  (tests/test_postgres.rs)   — always run
#   2. DuckDB end-to-end smoke  (scripts/duckdb_smoke.py)  — run when an
#      extension build and the `duckdb` python module are available
#
# Usage:
#   scripts/run_functional_tests.sh                # uses docker compose
#   PG_HOST=127.0.0.1 scripts/run_functional_tests.sh   # use an existing PG
#   EXT_PATH=path/to/ext scripts/run_functional_tests.sh  # also run DuckDB smoke
set -euo pipefail

cd "$(dirname "$0")/.."

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
PG_DB="${PG_DB:-testdb}"
USE_DOCKER="${USE_DOCKER:-auto}"   # auto | yes | no

export PGPASSWORD="$PG_PASSWORD"

started_docker=0
cleanup() {
  if [[ "$started_docker" == "1" ]]; then
    echo "==> tearing down docker compose"
    docker compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ── 1. Ensure a PostgreSQL server is reachable ──────────────────────────────
pg_ready() { pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" >/dev/null 2>&1; }

if [[ "$USE_DOCKER" != "no" ]] && ! pg_ready; then
  if docker compose version >/dev/null 2>&1; then
    echo "==> starting postgres via docker compose"
    docker compose up -d postgres
    started_docker=1
    # docker-compose maps the container to localhost:5432
    PG_HOST=127.0.0.1
    for _ in $(seq 1 30); do pg_ready && break; sleep 2; done
  fi
fi

if ! pg_ready; then
  echo "ERROR: no reachable PostgreSQL at ${PG_HOST}:${PG_PORT}" >&2
  exit 1
fi
echo "==> postgres is ready at ${PG_HOST}:${PG_PORT}"

# ── 2. Seed the database ────────────────────────────────────────────────────
echo "==> seeding ${PG_DB}"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -tc \
  "SELECT 1 FROM pg_database WHERE datname='${PG_DB}'" | grep -q 1 \
  || psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "CREATE DATABASE ${PG_DB}"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 \
  -f test/sql/seed_postgres.sql

# ── 3. Rust integration tests ───────────────────────────────────────────────
echo "==> running Rust integration tests"
PG_HOST="$PG_HOST" PG_PORT="$PG_PORT" PG_USER="$PG_USER" \
  PG_PASSWORD="$PG_PASSWORD" PG_DB="$PG_DB" \
  cargo test --test test_postgres --features postgres -- --ignored --nocapture

# ── 4. DuckDB end-to-end smoke (optional) ───────────────────────────────────
# Runs when the `duckdb` python module is available. Builds and packages the
# extension if EXT_PATH isn't already provided.
if python3 -c "import duckdb" >/dev/null 2>&1; then
  if [[ -z "${EXT_PATH:-}" ]]; then
    echo "==> building + packaging the extension for the DuckDB smoke"
    cargo build --release --features postgres
    LIB=""
    for cand in librust_db_driver.so librust_db_driver.dylib rust_db_driver.dll; do
      [[ -f "target/release/$cand" ]] && LIB="target/release/$cand" && break
    done
    [[ -n "$LIB" ]] || { echo "ERROR: built library not found in target/release" >&2; exit 1; }
    PLATFORM=$(python3 -c "import duckdb; print(duckdb.connect().execute('PRAGMA platform').fetchone()[0])")
    EXT_PATH="$(pwd)/target/release/rust_db_driver.duckdb_extension"
    python3 scripts/package_extension.py --library "$LIB" --out "$EXT_PATH" \
      --platform "$PLATFORM" --capi-version "v1.2.0" --extension-version "v0.1.0"
  fi
  echo "==> running DuckDB end-to-end smoke test"
  PG_CONN="host=${PG_HOST} port=${PG_PORT} dbname=${PG_DB} user=${PG_USER} password=${PG_PASSWORD} sslmode=disable" \
    EXT_PATH="$EXT_PATH" python3 scripts/duckdb_smoke.py
else
  echo "==> skipping DuckDB smoke (install the duckdb python module to enable)"
fi

echo "==> functional tests passed"
