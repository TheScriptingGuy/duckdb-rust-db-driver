-- Seed data for the SQL Server functional tests.
-- Mirrors test/sql/seed_postgres.sql so the partition strategies can assert the
-- same row counts. Loaded into the `testdb` database by
-- scripts/run_functional_tests.sh (or the CI workflow).
--
-- Three tables exercise the range-partitioning strategies:
--   * orders  — integer primary key       -> partition::by_int_range
--                + a datetime2 column      -> partition::by_datetime_range
--   * uitems  — uniqueidentifier primary key -> partition::by_uuid_range_ordered
--                                              (UuidOrder::SqlServer)

IF DB_ID('testdb') IS NULL
    CREATE DATABASE testdb;
GO
USE testdb;
GO

DROP TABLE IF EXISTS orders;
GO
CREATE TABLE orders (
    id        int PRIMARY KEY,
    customer  nvarchar(100) NOT NULL,
    total     float,
    paid      bit,
    created   datetime2
);
GO
-- 1000 rows: total = id * 1.5, created = '2024-01-01' + id days.
WITH n AS (
    SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS g
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO orders (id, customer, total, paid, created)
SELECT g,
       CONCAT('cust_', g),
       g * 1.5,
       CASE WHEN g % 2 = 0 THEN 1 ELSE 0 END,
       DATEADD(day, g, CAST('2024-01-01' AS datetime2))
FROM n;
GO

DROP TABLE IF EXISTS uitems;
GO
CREATE TABLE uitems (
    id    uniqueidentifier PRIMARY KEY,
    label nvarchar(100)
);
GO
-- 500 rows with GUIDs spread across the value space (deterministic MD5 of the
-- row number), so range slicing genuinely exercises uniqueidentifier ordering.
WITH n AS (
    SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS g
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO uitems (id, label)
SELECT CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varbinary(8), g))),
       CONCAT('item_', g)
FROM n;
GO
