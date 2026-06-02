#!/usr/bin/env bash
# Functional test harness for the PostgreSQL and SQL Server paths.
#
# Brings up the databases, seeds them, then validates the driver +
# partitioning logic at several levels:
#   1. PostgreSQL integration tests (tests/test_postgres.rs) — always run
#   2. DuckDB end-to-end smoke (scripts/duckdb_smoke.py)     — when an
#      extension build and the `duckdb` python module are available
#   3. SQL Server integration tests (tests/test_mssql.rs)    — opt-in; covers
#      the uniqueidentifier-ordered UUID partitioning. Set RUN_MSSQL=no to skip.
#
# Usage:
#   scripts/run_functional_tests.sh                # uses docker compose
#   PG_HOST=127.0.0.1 scripts/run_functional_tests.sh   # use an existing PG
#   EXT_PATH=path/to/ext scripts/run_functional_tests.sh  # also run DuckDB smoke
#   RUN_MSSQL=no scripts/run_functional_tests.sh    # skip SQL Server tests
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

# ── 5. SQL Server functional tests (opt-in) ─────────────────────────────────
# Runs when RUN_MSSQL is yes/auto and either a server is reachable or docker is
# available to start one. Set RUN_MSSQL=no to skip entirely.
RUN_MSSQL="${RUN_MSSQL:-auto}"
MSSQL_HOST="${MSSQL_HOST:-127.0.0.1}"
MSSQL_PORT="${MSSQL_PORT:-1433}"
MSSQL_USER="${MSSQL_USER:-sa}"
MSSQL_PASSWORD="${MSSQL_PASSWORD:-YourStrong!Passw0rd}"
MSSQL_DB="${MSSQL_DB:-testdb}"

# Run sqlcmd, preferring a host install and falling back to the compose
# container's bundled mssql-tools18.
mssql_cmd() {
  if command -v sqlcmd >/dev/null 2>&1; then
    sqlcmd -S "${MSSQL_HOST},${MSSQL_PORT}" -U "$MSSQL_USER" -P "$MSSQL_PASSWORD" -C "$@"
  else
    docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
      -S localhost -U "$MSSQL_USER" -P "$MSSQL_PASSWORD" -C "$@"
  fi
}
mssql_ready() { mssql_cmd -b -Q "SELECT 1" >/dev/null 2>&1; }

if [[ "$RUN_MSSQL" != "no" ]]; then
  if ! mssql_ready && [[ "$USE_DOCKER" != "no" ]] && docker compose version >/dev/null 2>&1; then
    echo "==> starting sql server via docker compose"
    docker compose up -d mssql
    started_docker=1
    MSSQL_HOST=127.0.0.1
    for _ in $(seq 1 40); do mssql_ready && break; sleep 3; done
  fi

  if mssql_ready; then
    echo "==> sql server is ready at ${MSSQL_HOST}:${MSSQL_PORT}"
    echo "==> seeding ${MSSQL_DB}"
    # Pipe the seed in when seeding through the container (no shared filesystem).
    if command -v sqlcmd >/dev/null 2>&1; then
      mssql_cmd -b -i test/sql/seed_mssql.sql
    else
      docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U "$MSSQL_USER" -P "$MSSQL_PASSWORD" -C -b < test/sql/seed_mssql.sql
    fi
    echo "==> running SQL Server integration tests"
    MSSQL_HOST="$MSSQL_HOST" MSSQL_PORT="$MSSQL_PORT" MSSQL_USER="$MSSQL_USER" \
      MSSQL_PASSWORD="$MSSQL_PASSWORD" MSSQL_DB="$MSSQL_DB" \
      cargo test --test test_mssql --features mssql -- --ignored --nocapture
  elif [[ "$RUN_MSSQL" == "yes" ]]; then
    echo "ERROR: RUN_MSSQL=yes but no reachable SQL Server at ${MSSQL_HOST}:${MSSQL_PORT}" >&2
    exit 1
  else
    echo "==> skipping SQL Server tests (no reachable server; set RUN_MSSQL=yes to require)"
  fi
fi

echo "==> functional tests passed"
