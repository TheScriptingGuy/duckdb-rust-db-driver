#!/usr/bin/env python3
"""End-to-end smoke test: load the built extension into DuckDB and query
PostgreSQL through the `postgres_query` table function, including the
partitioned variants.

Environment:
  EXT_PATH   path to the built .duckdb_extension (or the cdylib for dev loads)
  PG_CONN    PostgreSQL connection string for the extension to use
             (default: a localhost testdb connection)

Exits non-zero on the first failed assertion.
"""
import os
import sys

import duckdb

EXT_PATH = os.environ.get("EXT_PATH")
PG_CONN = os.environ.get(
    "PG_CONN",
    "host=127.0.0.1 port=5432 dbname=testdb user=postgres password=postgres sslmode=disable",
)

if not EXT_PATH:
    sys.exit("EXT_PATH must point to the built extension")


def scalar(con, sql, **params):
    return con.execute(sql, params).fetchone()[0]


def main() -> int:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXT_PATH}'")
    print(f"loaded extension: {EXT_PATH}")

    failures = 0

    def check(name, got, want):
        nonlocal failures
        ok = got == want
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: got {got}, want {want}")
        if not ok:
            failures += 1

    # 1) Plain (non-partitioned) scan.
    n = scalar(
        con,
        "SELECT count(*) FROM postgres_query($conn, 'SELECT id FROM orders')",
        conn=PG_CONN,
    )
    check("plain scan of orders", n, 1000)

    # 2) Integer-range partitioned scan must cover the whole table exactly once.
    n = scalar(
        con,
        """
        SELECT count(*) FROM postgres_query(
            $conn,
            'SELECT id FROM orders',
            partition_key = 'id',
            partition_min = 1,
            partition_max = 1000,
            partitions    = 8
        )
        """,
        conn=PG_CONN,
    )
    check("int-range partitioned scan of orders", n, 1000)

    # 3) Partitioned result must equal the single-query result for a filtered set.
    single = scalar(
        con,
        "SELECT count(*) FROM postgres_query($conn, 'SELECT id FROM orders WHERE total > 100')",
        conn=PG_CONN,
    )
    parted = scalar(
        con,
        """
        SELECT count(*) FROM postgres_query(
            $conn,
            'SELECT id FROM orders WHERE total > 100',
            partition_key = 'id', partition_min = 1, partition_max = 1000, partitions = 8
        )
        """,
        conn=PG_CONN,
    )
    check("filtered partitioned == single", parted, single)

    # 4) UUID-range partitioned scan over the full 128-bit space.
    n = scalar(
        con,
        """
        SELECT count(*) FROM postgres_query(
            $conn,
            'SELECT id FROM uitems',
            partition_key      = 'id',
            partition_uuid_min = '00000000-0000-0000-0000-000000000000',
            partition_uuid_max = 'ffffffff-ffff-ffff-ffff-ffffffffffff',
            partitions         = 4
        )
        """,
        conn=PG_CONN,
    )
    check("uuid-range partitioned scan of uitems", n, 500)

    print("OK" if failures == 0 else f"{failures} check(s) failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
