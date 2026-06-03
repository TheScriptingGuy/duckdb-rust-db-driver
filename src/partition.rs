//! Helpers for splitting one logical query into several [`Partition`]s that can
//! be run in parallel via [`crate::driver::DbDriver::query_partitioned`].
//!
//! SQL has no generic, safe way to "cut a query into N", so these helpers wrap
//! your query as a subselect and add a slicing predicate. The integer and
//! offset helpers embed only integer literals; the UUID helper embeds canonical
//! lowercase UUID literals. None of them interpolate caller-supplied row data,
//! so the generated SQL is injection-safe.
//!
//! Several strategies are provided:
//!
//! * [`by_int_range`] — split on an integer key column. Predictable and
//!   efficient when the key is indexed, but you must know the key's bounds.
//! * [`by_uuid_range`] — split on a UUID key column over the 128-bit value
//!   space, assuming byte-value ordering (PostgreSQL `uuid`, MySQL
//!   `BINARY(16)` / lowercase `CHAR(36)`). For SQL Server `uniqueidentifier`,
//!   which compares GUIDs in a different byte order, use
//!   [`by_uuid_range_ordered`] with [`UuidOrder::SqlServer`].
//! * [`by_date_range`] / [`by_datetime_range`] / [`by_timestamp_range`] — split
//!   on a `DATE` / naive `DATETIME` / timezone-aware `TIMESTAMP` key column.
//! * [`by_offset`] — `LIMIT`/`OFFSET` paging. Works without a key, but needs a
//!   stable `ORDER BY` and large offsets get progressively more expensive.

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use uuid::Uuid;

use crate::driver::Partition;

/// Strip a trailing `;` and surrounding whitespace so the query can be safely
/// wrapped in a subselect.
fn clean(base_sql: &str) -> &str {
    base_sql.trim().trim_end_matches(';').trim_end()
}

/// Split a query into contiguous ranges over an integer key column.
///
/// The inclusive interval `[min, max]` is divided into (at most) `partitions`
/// contiguous sub-ranges, and each partition selects the rows whose
/// `key_column` falls in its sub-range using `BETWEEN lo AND hi`. The ranges
/// tile `[min, max]` with no gaps or overlaps, and the final range ends exactly
/// at `max`.
///
/// Empty sub-ranges (which happen when `partitions` exceeds the number of
/// distinct key values) are skipped, so the returned vector may contain fewer
/// than `partitions` entries. `min > max` or `partitions == 0` yields an empty
/// vector.
///
/// ```
/// # use rust_db_driver::partition;
/// let parts = partition::by_int_range("SELECT id, total FROM orders", "id", 1, 100, 4);
/// assert_eq!(parts.len(), 4);
/// assert_eq!(
///     parts[0].sql,
///     "SELECT * FROM (SELECT id, total FROM orders) AS _part WHERE id BETWEEN 1 AND 25"
/// );
/// ```
pub fn by_int_range(
    base_sql: &str,
    key_column: &str,
    min: i64,
    max: i64,
    partitions: u32,
) -> Vec<Partition> {
    if partitions == 0 || min > max {
        return Vec::new();
    }

    let base = clean(base_sql);
    let n = partitions as i128;
    // Number of distinct integer values in the inclusive interval.
    let width = (max as i128) - (min as i128) + 1;
    let min = min as i128;

    let mut out = Vec::with_capacity(partitions as usize);
    for i in 0..n {
        // Half-open value boundaries, then convert to an inclusive [lo, hi].
        let lo = min + (width * i) / n;
        let hi = min + (width * (i + 1)) / n - 1;
        if lo > hi {
            // Empty slice — more partitions than distinct values.
            continue;
        }
        out.push(Partition::new(format!(
            "SELECT * FROM ({base}) AS _part WHERE {key_column} BETWEEN {lo} AND {hi}"
        )));
    }
    out
}

/// Split a query into `LIMIT`/`OFFSET` pages.
///
/// Produces up to `partitions` pages covering `total_rows` rows. A stable
/// `order_by` clause (column list / expression, without the `ORDER BY` keyword)
/// is required: without a deterministic ordering, paging may drop or duplicate
/// rows across partitions.
///
/// Pages are sized as evenly as possible; the last page may be smaller. If
/// `partitions` exceeds `total_rows`, only the non-empty pages are returned.
/// `total_rows == 0` or `partitions == 0` yields an empty vector.
///
/// ```
/// # use rust_db_driver::partition;
/// let parts = partition::by_offset("SELECT * FROM events", "ts, id", 1000, 4);
/// assert_eq!(parts.len(), 4);
/// assert_eq!(
///     parts[0].sql,
///     "SELECT * FROM (SELECT * FROM events) AS _part ORDER BY ts, id LIMIT 250 OFFSET 0"
/// );
/// ```
pub fn by_offset(
    base_sql: &str,
    order_by: &str,
    total_rows: u64,
    partitions: u32,
) -> Vec<Partition> {
    if partitions == 0 || total_rows == 0 {
        return Vec::new();
    }

    let base = clean(base_sql);
    let partitions = partitions as u64;
    // Ceiling division so `partitions` pages always cover every row.
    let page = total_rows.div_ceil(partitions);

    let mut out = Vec::new();
    let mut offset = 0u64;
    while offset < total_rows {
        let limit = page.min(total_rows - offset);
        out.push(Partition::new(format!(
            "SELECT * FROM ({base}) AS _part ORDER BY {order_by} LIMIT {limit} OFFSET {offset}"
        )));
        offset += page;
    }
    out
}

/// Compute `floor(a * num / den)` without overflowing `u128` for the
/// intermediate product. Exact for all inputs where the result fits in `u128`.
fn mul_div(a: u128, num: u128, den: u128) -> u128 {
    (a / den) * num + ((a % den) * num) / den
}

/// Split a query into contiguous ranges over a UUID key column, slicing the
/// full 128-bit value space `[min, max]`.
///
/// Each partition selects `key_column >= '<lo>' AND key_column < '<hi>'` (the
/// final partition uses `<= '<max>'` so the upper bound is inclusive), with
/// canonical lowercase UUID literals. The ranges tile `[min, max]` with no gaps
/// or overlaps. Empty sub-ranges are skipped, so the result may contain fewer
/// than `partitions` entries. `min > max` or `partitions == 0` yields an empty
/// vector.
///
/// # Correctness depends on the backend's UUID ordering
///
/// This splits on the *numeric* (byte) order of the UUID. It is correct when
/// the backend compares the key in that same order:
///
/// * **PostgreSQL `uuid`** — ordered by byte value. ✅
/// * **MySQL** stored as `BINARY(16)`, or lowercase canonical `CHAR(36)` under a
///   binary/`ascii` collation — string comparison matches byte order. ✅
/// * **SQL Server `uniqueidentifier`** — uses a *different* comparison order, so
///   this byte-value slicing can drop or duplicate rows. ❌ Use
///   [`by_uuid_range_ordered`] with [`UuidOrder::SqlServer`] instead, which
///   slices in `uniqueidentifier` order.
///
/// ```
/// # use rust_db_driver::partition;
/// # use uuid::Uuid;
/// let lo = Uuid::from_u128(0); // 0000...0000
/// let hi = Uuid::from_u128(u128::MAX); // ffff...ffff
/// let parts = partition::by_uuid_range("SELECT id FROM t", "id", lo, hi, 2);
/// assert_eq!(parts.len(), 2);
/// // The cut point is MAX/2 = 0x7fff…ffff (half-open boundaries, gap-free).
/// assert_eq!(
///     parts[0].sql,
///     "SELECT * FROM (SELECT id FROM t) AS _part WHERE id >= '00000000-0000-0000-0000-000000000000' \
///      AND id < '7fffffff-ffff-ffff-ffff-ffffffffffff'"
/// );
/// ```
pub fn by_uuid_range(
    base_sql: &str,
    key_column: &str,
    min: Uuid,
    max: Uuid,
    partitions: u32,
) -> Vec<Partition> {
    by_uuid_range_ordered(
        base_sql,
        key_column,
        min,
        max,
        partitions,
        UuidOrder::Lexical,
    )
}

/// How a backend sorts/compares UUID values, which determines how
/// [`by_uuid_range_ordered`] tiles the 128-bit key space.
///
/// SQL Server's `uniqueidentifier` does **not** compare GUIDs by their canonical
/// big-endian value: it weighs the node bytes most significantly and
/// byte-reverses the first three groups. Slicing lexically and handing the
/// boundaries to a `uniqueidentifier` column would drop or duplicate rows, so
/// SQL Server needs its own ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UuidOrder {
    /// Canonical big-endian 128-bit ordering — the UUID compares as the integer
    /// you read off its hex string left-to-right. PostgreSQL `uuid`, MySQL
    /// `BINARY(16)`/lowercase `CHAR(36)`, DuckDB `UUID`. This is what plain
    /// [`by_uuid_range`] uses.
    Lexical,
    /// SQL Server `uniqueidentifier` native ordering. The comparison weighs the
    /// canonical bytes in this significance order (most significant first):
    /// `10,11,12,13,14,15, 8,9, 7,6, 5,4, 3,2,1,0`.
    SqlServer,
}

/// Canonical (RFC 4122) byte indices in SQL Server `uniqueidentifier`
/// significance order, most significant first. Derived from .NET `SqlGuid`'s
/// comparison order translated from its mixed-endian `ToByteArray()` layout back
/// into canonical byte positions.
const SQLSERVER_ORDER: [usize; 16] = [10, 11, 12, 13, 14, 15, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0];

/// Map a UUID to the unsigned 128-bit sort key the chosen backend orders by.
fn uuid_to_key(u: Uuid, order: UuidOrder) -> u128 {
    match order {
        UuidOrder::Lexical => u.as_u128(),
        UuidOrder::SqlServer => {
            let b = u.as_bytes();
            let mut key = 0u128;
            for &idx in &SQLSERVER_ORDER {
                key = (key << 8) | b[idx] as u128;
            }
            key
        }
    }
}

/// Inverse of [`uuid_to_key`]: rebuild the UUID from a backend sort key so the
/// boundary can be emitted as a string literal the column compares cleanly.
fn key_to_uuid(key: u128, order: UuidOrder) -> Uuid {
    match order {
        UuidOrder::Lexical => Uuid::from_u128(key),
        UuidOrder::SqlServer => {
            let kb = key.to_be_bytes();
            let mut out = [0u8; 16];
            for (i, &idx) in SQLSERVER_ORDER.iter().enumerate() {
                out[idx] = kb[i];
            }
            Uuid::from_bytes(out)
        }
    }
}

/// Like [`by_uuid_range`], but slices the UUID space in the backend's own UUID
/// ordering (`order`) rather than assuming byte-value order.
///
/// Pass [`UuidOrder::SqlServer`] for `uniqueidentifier` columns and
/// [`UuidOrder::Lexical`] for PostgreSQL/MySQL/DuckDB. `min`/`max` are
/// interpreted in the chosen ordering; boundaries are computed in that ordering's
/// key space and converted back to UUID literals, so the half-open ranges tile
/// `[min, max]` with no gaps or overlaps under that backend's comparison. Empty
/// sub-ranges are skipped; `min > max` (in the chosen ordering) or
/// `partitions == 0` yields an empty vector.
///
/// ```
/// # use rust_db_driver::partition::{self, UuidOrder};
/// # use uuid::Uuid;
/// let parts = partition::by_uuid_range_ordered(
///     "SELECT id FROM events", "id",
///     Uuid::nil(), Uuid::max(), 4, UuidOrder::SqlServer,
/// );
/// assert_eq!(parts.len(), 4);
/// ```
pub fn by_uuid_range_ordered(
    base_sql: &str,
    key_column: &str,
    min: Uuid,
    max: Uuid,
    partitions: u32,
    order: UuidOrder,
) -> Vec<Partition> {
    let lo_key = uuid_to_key(min, order);
    let hi_key = uuid_to_key(max, order);
    if partitions == 0 || lo_key > hi_key {
        return Vec::new();
    }

    let base = clean(base_sql);
    let n = partitions as u128;
    // Half-open value boundaries across the inclusive interval [min, max] in the
    // chosen ordering's key space: boundary(0) == lo_key, boundary(n) == hi_key.
    let span = hi_key - lo_key;
    let boundary = |k: u128| lo_key + mul_div(span, k, n);

    let mut out = Vec::with_capacity(partitions as usize);
    for i in 0..n {
        let lo = boundary(i);
        let hi = boundary(i + 1);
        let lo_uuid = key_to_uuid(lo, order);
        if i + 1 == n {
            // Last partition: close the interval inclusively at max.
            if lo > hi_key {
                continue;
            }
            out.push(Partition::new(format!(
                "SELECT * FROM ({base}) AS _part WHERE {key_column} >= '{lo_uuid}' \
                 AND {key_column} <= '{max}'"
            )));
        } else {
            if lo >= hi {
                // Empty slice — more partitions than distinct values.
                continue;
            }
            let hi_uuid = key_to_uuid(hi, order);
            out.push(Partition::new(format!(
                "SELECT * FROM ({base}) AS _part WHERE {key_column} >= '{lo_uuid}' \
                 AND {key_column} < '{hi_uuid}'"
            )));
        }
    }
    out
}

/// Interpolate the `i`-th of `n` boundary offsets across a [`Duration`] span,
/// preferring microsecond precision and falling back to seconds for spans too
/// large to express in microseconds. `span` is assumed non-negative.
fn split_duration(span: Duration, i: u32, n: u32) -> Duration {
    if let Some(us) = span.num_microseconds() {
        Duration::microseconds(mul_div(us as u128, i as u128, n as u128) as i64)
    } else {
        let s = span.num_seconds();
        Duration::seconds(mul_div(s as u128, i as u128, n as u128) as i64)
    }
}

/// Build half-open time partitions from pre-formatted ascending boundary
/// literals (`bounds[0] == min` … `bounds[n] == max`).
///
/// Each partition `i` selects `[bounds[i], bounds[i+1])` with
/// `key >= lo AND key < hi`, except the last, which closes inclusively
/// (`<= max`) so the upper endpoint is not dropped. Empty slices (equal adjacent
/// bounds) are skipped. Half-open intervals avoid the double-counting that
/// inclusive `BETWEEN` would cause for continuous types where a row can land
/// exactly on a boundary.
fn time_partitions(base: &str, key_column: &str, bounds: &[String]) -> Vec<Partition> {
    let n = bounds.len().saturating_sub(1);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let lo = &bounds[i];
        let hi = &bounds[i + 1];
        let last = i + 1 == n;
        if !last && lo == hi {
            continue;
        }
        let pred = if last {
            format!("{key_column} >= '{lo}' AND {key_column} <= '{hi}'")
        } else {
            format!("{key_column} >= '{lo}' AND {key_column} < '{hi}'")
        };
        out.push(Partition::new(format!(
            "SELECT * FROM ({base}) AS _part WHERE {pred}"
        )));
    }
    out
}

/// Split a query into contiguous ranges over a `DATE` key column.
///
/// The inclusive interval `[min, max]` is divided into (at most) `partitions`
/// day-aligned sub-ranges using half-open `key >= 'lo' AND key < 'hi'` predicates
/// (the last partition closes inclusively on `max`). Boundaries are emitted as
/// `YYYY-MM-DD` literals. `min > max` or `partitions == 0` yields an empty
/// vector.
///
/// ```
/// # use rust_db_driver::partition;
/// # use chrono::NaiveDate;
/// let min = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
/// let max = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
/// let parts = partition::by_date_range("SELECT * FROM events", "day", min, max, 4);
/// assert_eq!(parts.len(), 4);
/// assert!(parts[0].sql.contains("day >= '2024-01-01' AND day < '2024-04-01'"));
/// ```
pub fn by_date_range(
    base_sql: &str,
    key_column: &str,
    min: NaiveDate,
    max: NaiveDate,
    partitions: u32,
) -> Vec<Partition> {
    if partitions == 0 || min > max {
        return Vec::new();
    }
    let base = clean(base_sql);
    let span = (max - min).num_days().max(0) as u128;
    let n = partitions;
    let bounds: Vec<String> = (0..=n)
        .map(|i| {
            let days = mul_div(span, i as u128, n as u128) as i64;
            (min + Duration::days(days)).format("%Y-%m-%d").to_string()
        })
        .collect();
    time_partitions(base, key_column, &bounds)
}

/// Split a query into contiguous ranges over a naive `DATETIME` / `TIMESTAMP`
/// (no time zone) key column.
///
/// The inclusive interval `[min, max]` is divided into (at most) `partitions`
/// sub-ranges using half-open `key >= 'lo' AND key < 'hi'` predicates (the last
/// closes inclusively on `max`). Boundaries are interpolated at microsecond
/// precision and emitted as `YYYY-MM-DD HH:MM:SS.ffffff` literals. `min > max` or
/// `partitions == 0` yields an empty vector.
///
/// ```
/// # use rust_db_driver::partition;
/// # use chrono::NaiveDate;
/// let min = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
/// let max = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap().and_hms_opt(0, 0, 0).unwrap();
/// let parts = partition::by_datetime_range("SELECT * FROM events", "ts", min, max, 2);
/// assert_eq!(parts.len(), 2);
/// ```
pub fn by_datetime_range(
    base_sql: &str,
    key_column: &str,
    min: NaiveDateTime,
    max: NaiveDateTime,
    partitions: u32,
) -> Vec<Partition> {
    if partitions == 0 || min > max {
        return Vec::new();
    }
    let base = clean(base_sql);
    let span = max - min;
    let n = partitions;
    let bounds: Vec<String> = (0..=n)
        .map(|i| {
            (min + split_duration(span, i, n))
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string()
        })
        .collect();
    time_partitions(base, key_column, &bounds)
}

/// Split a query into contiguous ranges over a timezone-aware `TIMESTAMP` /
/// `TIMESTAMPTZ` / `datetimeoffset` key column.
///
/// Identical tiling to [`by_datetime_range`], but boundaries carry a UTC offset
/// and are emitted as `YYYY-MM-DD HH:MM:SS.ffffff+00:00`. `min > max` or
/// `partitions == 0` yields an empty vector.
///
/// ```
/// # use rust_db_driver::partition;
/// # use chrono::{TimeZone, Utc};
/// let min = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
/// let max = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
/// let parts = partition::by_timestamp_range("SELECT * FROM events", "ts", min, max, 2);
/// assert_eq!(parts.len(), 2);
/// ```
pub fn by_timestamp_range(
    base_sql: &str,
    key_column: &str,
    min: DateTime<Utc>,
    max: DateTime<Utc>,
    partitions: u32,
) -> Vec<Partition> {
    if partitions == 0 || min > max {
        return Vec::new();
    }
    let base = clean(base_sql);
    let span = max - min;
    let n = partitions;
    let bounds: Vec<String> = (0..=n)
        .map(|i| {
            (min + split_duration(span, i, n))
                .format("%Y-%m-%d %H:%M:%S%.6f%:z")
                .to_string()
        })
        .collect();
    time_partitions(base, key_column, &bounds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_range_tiles_interval_without_gaps() {
        let parts = by_int_range("SELECT * FROM t", "id", 1, 100, 4);
        let sqls: Vec<&str> = parts.iter().map(|p| p.sql.as_str()).collect();
        assert_eq!(
            sqls,
            vec![
                "SELECT * FROM (SELECT * FROM t) AS _part WHERE id BETWEEN 1 AND 25",
                "SELECT * FROM (SELECT * FROM t) AS _part WHERE id BETWEEN 26 AND 50",
                "SELECT * FROM (SELECT * FROM t) AS _part WHERE id BETWEEN 51 AND 75",
                "SELECT * FROM (SELECT * FROM t) AS _part WHERE id BETWEEN 76 AND 100",
            ]
        );
    }

    #[test]
    fn int_range_uneven_split_covers_max_exactly() {
        let parts = by_int_range("SELECT * FROM t", "id", 0, 9, 3);
        // 10 values / 3 partitions → 3, 3, 4 (last ends at 9).
        assert_eq!(parts.len(), 3);
        assert!(parts[0].sql.ends_with("BETWEEN 0 AND 2"));
        assert!(parts[1].sql.ends_with("BETWEEN 3 AND 5"));
        assert!(parts[2].sql.ends_with("BETWEEN 6 AND 9"));
    }

    #[test]
    fn int_range_skips_empty_slices() {
        // More partitions than distinct values: only 3 non-empty ranges.
        let parts = by_int_range("SELECT * FROM t", "id", 1, 3, 10);
        assert_eq!(parts.len(), 3);
        assert!(parts[0].sql.ends_with("BETWEEN 1 AND 1"));
        assert!(parts[2].sql.ends_with("BETWEEN 3 AND 3"));
    }

    #[test]
    fn int_range_edge_cases() {
        assert!(by_int_range("SELECT 1", "id", 5, 4, 4).is_empty()); // min > max
        assert!(by_int_range("SELECT 1", "id", 1, 100, 0).is_empty()); // zero partitions
        let single = by_int_range("SELECT 1", "id", 7, 7, 4);
        assert_eq!(single.len(), 1);
        assert!(single[0].sql.ends_with("BETWEEN 7 AND 7"));
    }

    #[test]
    fn int_range_handles_negative_and_extreme_bounds() {
        let parts = by_int_range("SELECT * FROM t", "id", -10, 10, 2);
        assert_eq!(parts.len(), 2);
        assert!(parts[0].sql.ends_with("BETWEEN -10 AND -1"));
        assert!(parts[1].sql.ends_with("BETWEEN 0 AND 10"));
        // i64 extremes must not overflow (computed in i128 internally).
        let wide = by_int_range("SELECT * FROM t", "id", i64::MIN, i64::MAX, 2);
        assert_eq!(wide.len(), 2);
    }

    #[test]
    fn offset_pages_cover_all_rows() {
        let parts = by_offset("SELECT * FROM t", "id", 1000, 4);
        assert_eq!(parts.len(), 4);
        assert!(parts[0].sql.ends_with("LIMIT 250 OFFSET 0"));
        assert!(parts[3].sql.ends_with("LIMIT 250 OFFSET 750"));
    }

    #[test]
    fn offset_uneven_last_page_is_smaller() {
        // 10 rows / 3 → page 4 → pages of 4, 4, 2.
        let parts = by_offset("SELECT * FROM t", "id", 10, 3);
        assert_eq!(parts.len(), 3);
        assert!(parts[0].sql.ends_with("LIMIT 4 OFFSET 0"));
        assert!(parts[1].sql.ends_with("LIMIT 4 OFFSET 4"));
        assert!(parts[2].sql.ends_with("LIMIT 2 OFFSET 8"));
    }

    #[test]
    fn offset_more_partitions_than_rows() {
        let parts = by_offset("SELECT * FROM t", "id", 3, 10);
        // page = ceil(3/10) = 1 → 3 pages of 1 row each.
        assert_eq!(parts.len(), 3);
        assert!(parts[2].sql.ends_with("LIMIT 1 OFFSET 2"));
    }

    #[test]
    fn offset_edge_cases() {
        assert!(by_offset("SELECT 1", "id", 0, 4).is_empty());
        assert!(by_offset("SELECT 1", "id", 100, 0).is_empty());
    }

    #[test]
    fn trailing_semicolon_is_stripped() {
        let parts = by_int_range("SELECT * FROM t ;", "id", 1, 2, 1);
        assert_eq!(
            parts[0].sql,
            "SELECT * FROM (SELECT * FROM t) AS _part WHERE id BETWEEN 1 AND 2"
        );
    }

    #[test]
    fn uuid_range_tiles_full_space_without_gaps() {
        let lo = Uuid::from_u128(0);
        let hi = Uuid::from_u128(u128::MAX);
        let parts = by_uuid_range("SELECT id FROM t", "id", lo, hi, 4);
        assert_eq!(parts.len(), 4);
        // Half-open boundaries at MAX*k/4 → 0x3fff…, 0x7fff…, 0xbfff…; the last
        // range closes inclusively at max.
        assert!(parts[0]
            .sql
            .contains("id >= '00000000-0000-0000-0000-000000000000' AND id < '3fffffff"));
        assert!(parts[1].sql.contains("id >= '3fffffff"));
        assert!(parts[2].sql.contains("id >= '7fffffff"));
        assert!(parts[3].sql.contains("id >= 'bfffffff"));
        assert!(parts[3]
            .sql
            .ends_with("AND id <= 'ffffffff-ffff-ffff-ffff-ffffffffffff'"));
    }

    #[test]
    fn uuid_range_boundaries_are_contiguous() {
        // The high bound of partition i must equal the low bound of partition i+1.
        let parts = by_uuid_range(
            "SELECT id FROM t",
            "id",
            Uuid::from_u128(0),
            Uuid::from_u128(u128::MAX),
            3,
        );
        assert_eq!(parts.len(), 3);
        // partition 0 upper bound:
        assert!(parts[0]
            .sql
            .contains("AND id < '55555555-5555-5555-5555-555555555555'"));
        // partition 1 lower bound matches partition 0 upper bound:
        assert!(parts[1]
            .sql
            .contains("id >= '55555555-5555-5555-5555-555555555555'"));
    }

    #[test]
    fn uuid_range_single_partition_is_inclusive() {
        let lo = Uuid::from_u128(10);
        let hi = Uuid::from_u128(20);
        let parts = by_uuid_range("SELECT id FROM t", "id", lo, hi, 1);
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0].sql,
            format!(
                "SELECT * FROM (SELECT id FROM t) AS _part WHERE id >= '{lo}' AND id <= '{hi}'"
            )
        );
    }

    #[test]
    fn uuid_range_skips_empty_slices() {
        // Three distinct values but ten partitions → at most a handful of ranges,
        // and never more than requested.
        let parts = by_uuid_range(
            "SELECT id FROM t",
            "id",
            Uuid::from_u128(0),
            Uuid::from_u128(2),
            10,
        );
        assert!(!parts.is_empty());
        assert!(parts.len() <= 10);
        // The final partition still closes inclusively at max.
        assert!(parts
            .last()
            .unwrap()
            .sql
            .ends_with("AND id <= '00000000-0000-0000-0000-000000000002'"));
    }

    #[test]
    fn uuid_range_edge_cases() {
        let a = Uuid::from_u128(5);
        let b = Uuid::from_u128(4);
        assert!(by_uuid_range("SELECT 1", "id", a, b, 4).is_empty()); // min > max
        assert!(by_uuid_range("SELECT 1", "id", b, a, 0).is_empty()); // zero partitions
    }

    // ----- SQL Server-ordered UUID partitioning ----------------------------

    fn uuid(s: &str) -> Uuid {
        Uuid::parse_str(s).unwrap()
    }

    #[test]
    fn by_uuid_range_defaults_to_lexical() {
        // The plain helper must be byte-identical to the ordered helper in
        // Lexical mode (the contract the functional Postgres test relies on).
        let lhs = by_uuid_range("SELECT id FROM t", "id", Uuid::nil(), Uuid::max(), 4);
        let rhs = by_uuid_range_ordered(
            "SELECT id FROM t",
            "id",
            Uuid::nil(),
            Uuid::max(),
            4,
            UuidOrder::Lexical,
        );
        let l: Vec<&str> = lhs.iter().map(|p| p.sql.as_str()).collect();
        let r: Vec<&str> = rhs.iter().map(|p| p.sql.as_str()).collect();
        assert_eq!(l, r);
    }

    #[test]
    fn uuid_key_roundtrips_through_both_orderings() {
        let u = uuid("12345678-9abc-def0-1234-56789abcdef0");
        for order in [UuidOrder::Lexical, UuidOrder::SqlServer] {
            assert_eq!(key_to_uuid(uuid_to_key(u, order), order), u);
        }
    }

    #[test]
    fn sqlserver_orders_by_node_group_first() {
        // The first node byte (canonical byte 10) dominates under SQL Server
        // ordering, but is near-least-significant lexically — so the relation
        // flips between the two orderings.
        let node_high = uuid("00000000-0000-0000-0000-100000000000");
        let first_high = uuid("10000000-0000-0000-0000-000000000000");
        assert!(
            uuid_to_key(node_high, UuidOrder::SqlServer)
                > uuid_to_key(first_high, UuidOrder::SqlServer)
        );
        assert!(
            uuid_to_key(node_high, UuidOrder::Lexical)
                < uuid_to_key(first_high, UuidOrder::Lexical)
        );
    }

    #[test]
    fn sqlserver_uuid_partitions_tile_without_gaps() {
        // Under SQL Server's own ordering the half-open ranges must be
        // contiguous: each partition's upper bound equals the next lower bound.
        let parts = by_uuid_range_ordered(
            "SELECT * FROM t",
            "id",
            Uuid::nil(),
            Uuid::max(),
            8,
            UuidOrder::SqlServer,
        );
        assert_eq!(parts.len(), 8);
        let mut prev_hi: Option<u128> = None;
        for (i, p) in parts.iter().enumerate() {
            let (lo, hi) = extract_uuids(&p.sql);
            let lo_k = uuid_to_key(lo, UuidOrder::SqlServer);
            let hi_k = uuid_to_key(hi, UuidOrder::SqlServer);
            assert!(lo_k <= hi_k);
            if let Some(prev) = prev_hi {
                assert_eq!(prev, lo_k, "ranges must be contiguous in SQL Server order");
            }
            prev_hi = Some(hi_k);
            if i == 0 {
                assert_eq!(lo_k, 0);
            }
            if i + 1 == parts.len() {
                assert_eq!(hi_k, u128::MAX);
            }
        }
    }

    // Pull the two quoted UUID literals out of a generated half-open clause.
    fn extract_uuids(sql: &str) -> (Uuid, Uuid) {
        let lits: Vec<&str> = sql.split('\'').filter(|s| s.contains('-')).collect();
        (uuid(lits[0]), uuid(lits[1]))
    }

    // ----- time partitioning -----------------------------------------------

    #[test]
    fn date_range_tiles_year_into_quarters() {
        let min = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let max = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let parts = by_date_range("SELECT * FROM t", "day", min, max, 4);
        assert_eq!(parts.len(), 4);
        assert!(parts[0]
            .sql
            .ends_with("WHERE day >= '2024-01-01' AND day < '2024-04-01'"));
        // 365-day span; ¾ from Jan 1 lands on Sep 30, closed inclusively on max.
        assert!(parts[3]
            .sql
            .ends_with("WHERE day >= '2024-09-30' AND day <= '2024-12-31'"));
    }

    #[test]
    fn datetime_range_half_open_with_inclusive_tail() {
        let min = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let max = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 4)
            .unwrap();
        let parts = by_datetime_range("SELECT * FROM t", "ts", min, max, 4);
        assert_eq!(parts.len(), 4);
        assert!(parts[0]
            .sql
            .contains("ts >= '2024-01-01 00:00:00.000000' AND ts < '2024-01-01 00:00:01.000000'"));
        assert!(parts[3]
            .sql
            .contains("AND ts <= '2024-01-01 00:00:04.000000'"));
    }

    #[test]
    fn timestamp_range_carries_utc_offset() {
        use chrono::{TimeZone, Utc};
        let min = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let max = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 2).unwrap();
        let parts = by_timestamp_range("SELECT * FROM t", "ts", min, max, 2);
        assert_eq!(parts.len(), 2);
        assert!(parts[0]
            .sql
            .contains("ts >= '2024-01-01 00:00:00.000000+00:00'"));
        assert!(parts[1]
            .sql
            .contains("AND ts <= '2024-01-01 00:00:02.000000+00:00'"));
    }

    #[test]
    fn time_range_edge_cases() {
        let d0 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d1 = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
        assert!(by_date_range("SELECT 1", "d", d1, d0, 4).is_empty()); // min > max
        assert!(by_date_range("SELECT 1", "d", d0, d1, 0).is_empty()); // zero partitions
        let same = by_date_range("SELECT 1", "d", d0, d0, 4);
        assert_eq!(same.len(), 1);
        assert!(same[0]
            .sql
            .contains("d >= '2024-01-01' AND d <= '2024-01-01'"));
    }
}
