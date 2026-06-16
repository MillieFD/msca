/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! End-to-end integration tests over the public [`Dataset`](clem::Dataset) API.
//!
//! These tests treat [`clem`](clem) as an external user would: records derive
//! [`Data`](clem::Data) and [`Read`](clem::Read), are ingested through an
//! [`Accumulator`](clem::Accumulator), committed via [`Dataset::write`](clem::Dataset::write), and
//! read back via [`Dataset::query`](clem::Dataset::query). The derived implementations carry the
//! full burden of serialization, filtering, and reconstruction.
//!
//! A single shared set of fixtures is reused across every test to avoid proliferating bespoke
//! derived types. The fixtures are deliberately declared `pub` to additionally confirm that
//! [`#[derive(Read)]`](clem::Read) supports types of any visibility: the generated context inherits
//! the source type's visibility rather than leaking a private type through the public `Read::Ctx`
//! associated type.

use clem::{Accumulate, Data, Dataset, Query, Read, Schema};

/// A sensor reading composed of three independent fixed-width primitive columns.
#[derive(Debug, Clone, PartialEq, Data, Read)]
pub struct Reading {
    sensor: u32,
    latitude: f64,
    longitude: f64,
}

/// A sensor status record exercising the [`bool`] column read path alongside a primitive key.
#[derive(Debug, Clone, PartialEq, Data, Read)]
pub struct Flag {
    sensor: u32,
    active: bool,
}

/// Returns a unique scratch path under the system temporary directory, removing any stale file
/// left behind by a previous run.
fn scratch(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("clem-it-{name}.clem"));
    std::fs::remove_file(&path).ok();
    path
}

/// Build a [`Reading`] keyed by `sensor` with placeholder coordinates for column-agnostic tests.
fn reading(sensor: u32) -> Reading {
    Reading { sensor, latitude: 0.0, longitude: 0.0 }
}

/// Drain a [`Reading`] query result set into the bare `sensor` column for terse filter assertions.
fn sensors(query: Query) -> Vec<u32> {
    query.collect::<Reading>().expect("collect failed").into_iter().map(|row| row.sensor).collect()
}

/// Commit each `sensor` value as a single-segment `readings` schema, returning the scratch path
/// alongside the open [`Dataset`].
async fn seed(name: &str, sensors: &[u32]) -> (std::path::PathBuf, Dataset) {
    let path = scratch(name);
    let mut dataset = Dataset::new(&path).await.expect("new failed");
    let mut acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
    sensors.iter().for_each(|&sensor| acc.push(reading(sensor)));
    dataset.write(acc).await.expect("write failed");
    (path, dataset)
}

/// A populated [`Accumulator`](clem::Accumulator) reports its row count without performing file IO.
#[test]
fn accumulator_counts_rows() {
    let mut schema = Schema::new("readings");
    let mut acc = Reading::accumulator(&mut schema).expect("accumulator failed");
    assert!(acc.is_empty());
    acc.push(Reading {
        sensor: 1,
        latitude: 51.5,
        longitude: -0.1,
    });
    acc.push(Reading {
        sensor: 2,
        latitude: 48.9,
        longitude: 2.4,
    });
    assert_eq!(acc.count(), 2);
}

/// Records written through the public API are read back unchanged and in insertion order.
#[test]
fn writes_are_read_back() {
    smol::block_on(async {
        let path = scratch("round-trip");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        let mut acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
        let rows = vec![
            Reading {
                sensor: 1,
                latitude: 51.5,
                longitude: -0.1,
            },
            Reading {
                sensor: 2,
                latitude: 48.9,
                longitude: 2.4,
            },
        ];
        rows.iter().cloned().for_each(|row| acc.push(row));
        dataset.write(acc).await.expect("write failed");
        let read: Vec<Reading> =
            dataset.query("readings").expect("query failed").collect().expect("collect failed");
        std::fs::remove_file(&path).ok();
        assert_eq!(read, rows);
    });
}

/// Multiple data segments under one schema are read back as a single contiguous sequence.
#[test]
fn chains_multiple_segments() {
    smol::block_on(async {
        let path = scratch("segments");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        for batch in [[1u32, 2, 3], [4, 5, 6]] {
            let mut acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
            batch.into_iter().for_each(|sensor| acc.push(reading(sensor)));
            dataset.write(acc).await.expect("write failed");
        }
        let read = sensors(dataset.query("readings").expect("query failed"));
        std::fs::remove_file(&path).ok();
        assert_eq!(read, (1..=6).collect::<Vec<u32>>());
    });
}

/// Writing an empty [`Accumulator`](clem::Accumulator) is a no-op; the query yields no rows.
#[test]
fn empty_accumulator_writes_nothing() {
    smol::block_on(async {
        let path = scratch("empty");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        let acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
        let count = dataset.write(acc).await.expect("write failed");
        let read: Vec<Reading> =
            dataset.query("readings").expect("query failed").collect().expect("collect failed");
        std::fs::remove_file(&path).ok();
        assert_eq!(count, 0);
        assert!(read.is_empty());
    });
}

/// A schema mixing a [`bool`] column with a primitive key round-trips through the boolean stream.
#[test]
fn round_trips_bool_column() {
    smol::block_on(async {
        let path = scratch("bool");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        let mut acc = dataset.schema::<Flag>("flags").await.expect("schema failed");
        let rows = vec![
            Flag { sensor: 1, active: true },
            Flag { sensor: 2, active: false },
            Flag { sensor: 3, active: true },
        ];
        rows.iter().cloned().for_each(|row| acc.push(row));
        dataset.write(acc).await.expect("write failed");
        let read: Vec<Flag> =
            dataset.query("flags").expect("query failed").collect().expect("collect failed");
        std::fs::remove_file(&path).ok();
        assert_eq!(read, rows);
    });
}

/// A cloned [`Accumulator`](clem::Accumulator) starts empty and accumulates independently of its
/// source; both commit as separate data segments under the shared schema.
#[test]
fn clone_accumulates_independently() {
    smol::block_on(async {
        let path = scratch("clone");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        let mut acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
        [1u32, 2, 3].into_iter().for_each(|sensor| acc.push(reading(sensor)));
        let mut clone = acc.clone();
        assert!(clone.is_empty()); // Clone copies the schema binding, not accumulated data
        [10u32, 20].into_iter().for_each(|sensor| clone.push(reading(sensor)));
        assert_eq!(acc.count(), 3); // Pushing into the clone leaves the source unaffected
        dataset.write(acc).await.expect("write acc failed");
        dataset.write(clone).await.expect("write clone failed");
        let read = sensors(dataset.query("readings").expect("query failed"));
        std::fs::remove_file(&path).ok();
        assert_eq!(read, [1, 2, 3, 10, 20]);
    });
}

/// A half-open [`range`](clem::Query::range) retains only the rows whose value falls inside it.
#[test]
fn range_filters_rows() {
    smol::block_on(async {
        let (path, dataset) = seed("range", &[10, 20, 30, 40]).await;
        let query = dataset.query("readings").expect("query failed");
        let read = sensors(query.range("sensor", 15u32..35).expect("range failed"));
        std::fs::remove_file(&path).ok();
        assert_eq!(read, [20, 30]);
    });
}

/// An inclusive upper bound retains the boundary row; the exclusive bound drops it.
#[test]
fn range_bound_inclusivity() {
    smol::block_on(async {
        let (path, dataset) = seed("range-bounds", &[10, 20, 30]).await;
        let exclusive = dataset.query("readings").expect("query failed");
        let exclusive = sensors(exclusive.range("sensor", 20u32..30).expect("range failed"));
        let inclusive = dataset.query("readings").expect("query failed");
        let inclusive = sensors(inclusive.range("sensor", 20u32..=30).expect("range failed"));
        std::fs::remove_file(&path).ok();
        assert_eq!(exclusive, [20]);
        assert_eq!(inclusive, [20, 30]);
    });
}

/// A [`range`](clem::Query::range) disjoint from a buffer's statistics prunes it before any IO.
#[test]
fn range_prunes_disjoint_buffers() {
    smol::block_on(async {
        let path = scratch("range-prune");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        for batch in [[0u32, 1, 2], [10, 11, 12]] {
            let mut acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
            batch.into_iter().for_each(|sensor| acc.push(reading(sensor)));
            dataset.write(acc).await.expect("write failed");
        }
        let query = dataset.query("readings").expect("query failed");
        let read = sensors(query.range("sensor", 9u32..20).expect("range failed"));
        std::fs::remove_file(&path).ok();
        assert_eq!(read, [10, 11, 12]); // The disjoint first buffer is never read.
    });
}

/// [`stride`](clem::Query::stride) decimates the result set, retaining every nth row.
#[test]
fn stride_decimates_rows() {
    smol::block_on(async {
        let (path, dataset) = seed("stride", &[0, 1, 2, 3, 4, 5]).await;
        let read = sensors(dataset.query("readings").expect("query failed").stride(2));
        std::fs::remove_file(&path).ok();
        assert_eq!(read, [0, 2, 4]);
    });
}

/// A zero [`stride`](clem::Query::stride) is coerced to the minimum of `1`, retaining every row.
#[test]
fn stride_zero_keeps_all_rows() {
    smol::block_on(async {
        let (path, dataset) = seed("stride-zero", &[0, 1, 2, 3]).await;
        let read = sensors(dataset.query("readings").expect("query failed").stride(0));
        std::fs::remove_file(&path).ok();
        assert_eq!(read, [0, 1, 2, 3]);
    });
}

/// Filtering a column with an incompatible value type is rejected before any rows are read.
#[test]
fn range_type_mismatch_errors() {
    smol::block_on(async {
        let (path, dataset) = seed("mismatch", &[1, 2, 3]).await;
        let query = dataset.query("readings").expect("query failed");
        let result = query.range("sensor", 0u16..10); // `sensor` is a `u32` column
        std::fs::remove_file(&path).ok();
        assert!(result.is_err());
    });
}

/// Querying an unregistered schema name is rejected.
#[test]
fn query_unknown_schema_errors() {
    smol::block_on(async {
        let (path, dataset) = seed("unknown", &[1, 2, 3]).await;
        let result = dataset.query("missing");
        std::fs::remove_file(&path).ok();
        assert!(result.is_err());
    });
}

/// Re-registering an identical schema under the same name is deduplicated without error.
#[test]
fn identical_schema_reregistration_succeeds() {
    smol::block_on(async {
        let path = scratch("dedup");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        dataset.schema::<Reading>("readings").await.expect("first registration failed");
        let result = dataset.schema::<Reading>("readings").await;
        std::fs::remove_file(&path).ok();
        assert!(result.is_ok());
    });
}

/// Registering an incompatible layout under an existing name is rejected.
#[test]
fn incompatible_schema_reregistration_errors() {
    smol::block_on(async {
        let path = scratch("collision");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        dataset.schema::<Reading>("data").await.expect("first registration failed");
        let result = dataset.schema::<Flag>("data").await; // Same name, different columns.
        std::fs::remove_file(&path).ok();
        assert!(result.is_err());
    });
}

/// Distinct schemas coexist in one dataset and are queried independently by name.
#[test]
fn distinct_schemas_coexist() {
    smol::block_on(async {
        let path = scratch("multi-schema");
        let mut dataset = Dataset::new(&path).await.expect("new failed");
        let mut readings = dataset.schema::<Reading>("readings").await.expect("schema failed");
        readings.push(reading(1));
        dataset.write(readings).await.expect("write readings failed");
        let mut flags = dataset.schema::<Flag>("flags").await.expect("schema failed");
        flags.push(Flag { sensor: 9, active: true });
        dataset.write(flags).await.expect("write flags failed");
        let read_readings = sensors(dataset.query("readings").expect("query failed"));
        let read_flags: Vec<Flag> =
            dataset.query("flags").expect("query failed").collect().expect("collect failed");
        std::fs::remove_file(&path).ok();
        assert_eq!(read_readings, [1]);
        assert_eq!(read_flags, [Flag { sensor: 9, active: true }]);
    });
}

/// Data committed before closing a dataset is recovered intact when the file is reopened.
#[test]
fn reopen_preserves_committed_data() {
    smol::block_on(async {
        let path = scratch("reopen");
        let expected = Reading { sensor: 7, latitude: 1.0, longitude: 2.0 };
        {
            let mut dataset = Dataset::new(&path).await.expect("new failed");
            let mut acc = dataset.schema::<Reading>("readings").await.expect("schema failed");
            acc.push(expected.clone());
            dataset.write(acc).await.expect("write failed");
        } // The dataset is dropped here, closing the underlying file.
        let dataset = Dataset::open(&path).await.expect("open failed");
        let read: Vec<Reading> =
            dataset.query("readings").expect("query failed").collect().expect("collect failed");
        std::fs::remove_file(&path).ok();
        assert_eq!(read, [expected]);
    });
}
