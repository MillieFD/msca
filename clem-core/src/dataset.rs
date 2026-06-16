/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Public-facing user interface for [clem](crate) datasets.
//!
//! ---
//!
//! [`Dataset`] is the primary entry-point for working with a [clem](crate) file; providing a
//! high-level surface for registering [`Data`] types and [querying](query) stored data while
//! delegating low-level IO to an internal [`File`] handle.

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use crate::io::File;
use crate::query::{self, Query};
use crate::{io, schema, Accumulate, Accumulator, Data, Schema};

/* ------------------------------------------------------------------------------ Public Exports */

/// A high-level handle to an open [`clem`](crate) dataset.
// TODO → Dataset is the main user interaction surface; doc must be comprehensive and clear.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub struct Dataset {
    /// Underlying [`File`] handle backing this dataset.
    pub(crate) file: File,
    /// Read-only [memory map](Mmap) backed by the [clem](crate) file.
    ///
    /// ### ⚠️ Warning
    ///
    /// Undefined behaviour may occur if the mapped region is modified. The [`Mmap`] is therefore
    /// tightly scoped; mapping only the immutable segment region to reduce the risk of undefined
    /// behaviour. Refer to the [`File::mmap`] documentation for more details.
    pub mmap: Arc<Mmap>,
}

impl Dataset {
    /// Create a new empty [`Dataset`] at the specified [`path`](P).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`path`](P)
    /// - The current process lacks read and write permissions
    /// - The platform does not support [memory mapping](memmap2)
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    pub async fn new<P>(path: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let file = File::create(path).await?;
        let mmap = unsafe { file.mmap(file.header.tail)? }.into();
        Ok(Self { file, mmap })
    }

    /// Open an existing [`Dataset`] with read and write permissions at the specified [`path`](P).
    ///
    /// A [`Mmap`] is scoped to the immutable segment file region. Implementors must ensure that the
    /// provided [`path`](P) remains valid and accessible for the entire duration of the operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`path`](P)
    /// - The current process lacks read and write permissions
    /// - Unexpected `EOF` while parsing the [`Header`] or [`Manifest`]
    /// - The platform does not support [memory mapping](memmap2)
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    pub async fn open<P>(path: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path).await?;
        let mmap = unsafe { file.mmap(file.header.tail)? }.into();
        Ok(Self { file, mmap })
    }

    /// Register the [`Schema`] for [`I`] under `name` and return an empty [`Accumulator`].
    ///
    /// No file [`IO`](io) occurs until the accumulated data is [written](Self::write) to disk.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Collision`][1] if a schema is already registered with the requested `name`
    /// but an incompatible column layout.
    ///
    /// [1]: schema::Error::Collision
    pub fn schema<I>(&mut self, name: &str) -> Result<Accumulator<I>, schema::Error>
    where
        I: Data,
    {
        let mut schema = Schema::new(name);
        let boxed = I::accumulator(&mut schema)?;
        let sector = schema.finish(&mut self.file)?.sector;
        Ok(Accumulator {
            data: boxed,
            name: name.to_string(),
            schema: sector,
        })
    }
    /// Initialise a new [`Query`] over the named [`Schema`][1].
    ///
    /// The query begins with **every** column and **every** buffer from the specified schema.
    /// [`Filter`] functions are applied subtractively to reduce the result set. No file
    /// [`IO`](io) occurs until the query is executed via [`Query::read`] or [`Query::column`].
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Query`] wrapping [`query::Error::Column`] if the requested `name` is not
    /// found in the [`Manifest`][2].
    ///
    /// [1]: crate::manifest::Schema
    /// [2]: crate::manifest::Manifest
    pub fn query(&self, name: &str) -> Result<Query, query::Error> {
        let columns = self
            .file
            .manifest
            .schemas
            .get(name)
            .ok_or_else(|| query::Error::column(name))?
            .columns
            .iter()
            .map(query::Column::map) // Clone each entry
            .collect();
        Ok(Query {
            mmap: self.mmap.clone(), // Inexpensive Arc Clone
            columns,
            stride: NonZeroU32::MIN,
        })
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::accumulate::Accumulator;
    use crate::io::File;
    use crate::read::{Outcome, Read, Stream};
    use crate::schema::Schema;

    /// A minimal external record used to exercise the composite read path. The [`Read`]
    /// implementation below mirrors the code generated by `#[derive(Read)]`.
    #[derive(Debug, PartialEq)]
    struct Row {
        v: u32,
    }

    /// Generated-style composite context holding one column [`Stream`] per [`Row`] field.
    struct Ctx<'a> {
        v: Stream<'a, u32>,
    }

    impl<'a> TryFrom<&'a Query> for Ctx<'a> {
        type Error = query::Error;

        fn try_from(query: &'a Query) -> Result<Self, Self::Error> {
            Ok(Self { v: query.column::<u32>("v")? })
        }
    }

    impl Read for Row {
        type Ctx<'a> = Ctx<'a>;

        type Src<'a> = ();

        fn next<'a>(_: &mut Self::Src<'a>, ctx: &mut Self::Ctx<'a>) -> Outcome<Row> {
            match ctx.v.next() {
                Some(Outcome::Success(v)) => Outcome::Success(Row { v }),
                Some(Outcome::Excluded) => Outcome::Excluded,
                Some(Outcome::Error(error)) => Outcome::Error(error),
                Some(Outcome::Finished) | None => Outcome::Finished,
            }
        }
    }

    /// Unique scratch path for a round-trip test, cleared before use.
    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("clem-{name}.clem"));
        std::fs::remove_file(&path).ok();
        path
    }

    /// Write a single `u32` column named `v` under the `readings` schema; one segment per batch.
    async fn write(path: &Path, batches: &[&[u32]]) {
        let mut file = File::create(path).await.expect("create failed");
        let mut schema = Schema::new("readings");
        let accs: Vec<_> =
            batches.iter().map(|_| schema.column::<u32, _>("v").expect("column failed")).collect();
        let sector = schema.finish(&mut file).expect("finish failed").sector;
        for (mut data, values) in accs.into_iter().zip(batches) {
            values.iter().for_each(|&v| data.push(v));
            let accumulator = Accumulator {
                data,
                name: "readings".to_owned(),
                schema: sector,
            };
            file.write(accumulator).await.expect("write failed");
        }
    }

    #[test]
    fn round_trip_range_prunes_and_filters() {
        smol::block_on(async {
            let path = scratch("range");
            write(&path, &[&[10, 20, 30, 40]]).await;
            let dataset = Dataset::open(&path).await.expect("open failed");
            let rows: Vec<Row> = dataset
                .query("readings")
                .expect("query failed")
                .range("v", 15u32..35)
                .expect("range failed")
                .collect()
                .await
                .expect("collect failed");
            std::fs::remove_file(&path).ok();
            assert_eq!(rows, vec![Row { v: 20 }, Row { v: 30 }]);
        });
    }

    #[test]
    fn round_trip_stride_decimates() {
        smol::block_on(async {
            let path = scratch("stride");
            write(&path, &[&[0, 1, 2, 3, 4, 5]]).await;
            let dataset = Dataset::open(&path).await.expect("open failed");
            let rows: Vec<Row> = dataset
                .query("readings")
                .expect("query failed")
                .stride(2)
                .collect()
                .await
                .expect("collect failed");
            std::fs::remove_file(&path).ok();
            assert_eq!(rows, vec![Row { v: 0 }, Row { v: 2 }, Row { v: 4 }]);
        });
    }

    #[test]
    fn round_trip_chains_segments() {
        smol::block_on(async {
            let path = scratch("segments");
            write(&path, &[&[1, 2, 3], &[4, 5, 6]]).await;
            let dataset = Dataset::open(&path).await.expect("open failed");
            let rows: Vec<Row> = dataset
                .query("readings")
                .expect("query failed")
                .collect()
                .await
                .expect("collect failed");
            std::fs::remove_file(&path).ok();
            assert_eq!(rows, (1..=6).map(|v| Row { v }).collect::<Vec<Row>>());
        });
    }

    /// Each committed buffer [`Sector`](crate::Sector) begins at a 64-bit alignment boundary.
    #[test]
    fn round_trip_aligns_buffers() {
        smol::block_on(async {
            let path = scratch("align");
            write(&path, &[&[1, 2, 3], &[4, 5, 6, 7]]).await;
            let file = File::open(&path).await.expect("open failed");
            let offsets: Vec<u64> = file
                .manifest
                .schemas
                .values()
                .flat_map(|schema| schema.columns.values())
                .flat_map(|column| column.buffers.iter())
                .map(|buffer| buffer.sector.offset)
                .collect();
            std::fs::remove_file(&path).ok();
            assert_eq!(offsets.len(), 2);
            offsets.iter().for_each(|offset| assert_eq!(offset % 8, 0));
        });
    }

    #[test]
    fn query_unknown_schema_errors() {
        smol::block_on(async {
            let path = scratch("unknown");
            write(&path, &[&[1, 2, 3]]).await;
            let dataset = Dataset::open(&path).await.expect("open failed");
            let result = dataset.query("missing");
            std::fs::remove_file(&path).ok();
            assert!(result.is_err());
        });
    }
}
