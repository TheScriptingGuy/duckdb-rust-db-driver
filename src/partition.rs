//! Helpers for splitting one logical query into several [`Partition`]s that can
//! be run in parallel via [`crate::driver::DbDriver::query_partitioned`].
//!
//! SQL has no generic, safe way to "cut a query into N", so these helpers wrap
//! your query as a subselect and add a slicing predicate. The integer and
//! offset helpers embed only integer literals; the UUID helper embeds canonical
//! lowercase UUID literals. None of them interpolate caller-supplied row data,
//! so the generated SQL is injection-safe.
//!
//! Three strategies are provided:
//!
//! * [`by_int_range`] — split on an integer key column. Predictable and
//!   efficient when the key is indexed, but you must know the key's bounds.
//! * [`by_uuid_range`] — split on a UUID key column over the 128-bit value
//!   space. Correct only where the backend orders UUIDs by their byte value
//!   (e.g. PostgreSQL `uuid`, MySQL `BINARY(16)` / lowercase `CHAR(36)`); see
//!   the function docs for caveats.
//! * [`by_offset`] — `LIMIT`/`OFFSET` paging. Works without a key, but needs a
//!   stable `ORDER BY` and large offsets get progressively more expensive.

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
///   range slicing can drop or duplicate rows. ❌ Use [`by_offset`] instead, or
///   partition on a different (integer) column.
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
    let (min_u, max_u) = (min.as_u128(), max.as_u128());
    if partitions == 0 || min_u > max_u {
        return Vec::new();
    }

    let base = clean(base_sql);
    let n = partitions as u128;
    // Half-open value boundaries across the inclusive interval [min, max].
    // boundary(0) == min, boundary(n) == max; partition i spans
    // [boundary(i), boundary(i + 1)).
    let span = max_u - min_u;
    let boundary = |k: u128| min_u + mul_div(span, k, n);

    let mut out = Vec::with_capacity(partitions as usize);
    for i in 0..n {
        let lo = boundary(i);
        let hi = boundary(i + 1);
        let lo_uuid = Uuid::from_u128(lo);
        if i + 1 == n {
            // Last partition: close the interval inclusively at max.
            if lo > max_u {
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
            let hi_uuid = Uuid::from_u128(hi);
            out.push(Partition::new(format!(
                "SELECT * FROM ({base}) AS _part WHERE {key_column} >= '{lo_uuid}' \
                 AND {key_column} < '{hi_uuid}'"
            )));
        }
    }
    out
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
}
