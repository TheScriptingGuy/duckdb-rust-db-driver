-- Seed data for the PostgreSQL functional tests.
-- Loaded into the `testdb` database by scripts/run_functional_tests.sh.
--
-- Two tables exercise the two range-partitioning strategies:
--   * orders  — integer primary key  → partition::by_int_range
--   * uitems  — uuid primary key      → partition::by_uuid_range

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id        integer PRIMARY KEY,
    customer  text NOT NULL,
    total     double precision,
    paid      boolean,
    created   timestamp
);
INSERT INTO orders
SELECT g,
       'cust_' || g,
       (g * 1.5)::float8,
       (g % 2 = 0),
       timestamp '2024-01-01' + (g || ' days')::interval
FROM generate_series(1, 1000) AS g;

DROP TABLE IF EXISTS uitems;
CREATE TABLE uitems (
    id    uuid PRIMARY KEY,
    label text
);
INSERT INTO uitems
SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid,
       'item_' || g
FROM generate_series(1, 500) AS g;
