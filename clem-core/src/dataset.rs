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

use std::collections::hash_map::{Entry, VacantEntry};
use std::hash::Hash;
use std::path::Path;
use std::sync::Arc;

use funty::Unsigned;
use memmap2::Mmap;

use crate::io::File;
use crate::query::{self, Query};
use crate::read::{Composite, Outcome, Read};
use crate::schema::number;
use crate::{io, Accumulate, Accumulator, Data, Error, Schema};

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
        let mmap = unsafe { file.mmap()? }.into();
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
        let mmap = unsafe { file.mmap()? }.into();
        Ok(Self { file, mmap })
    }

    /// Register the [`Schema`] for [`I`] under `name` and return an empty [`Accumulator`].
    ///
    /// Unseen [`Schema`] are eagerly written to disk. Existing [`Schema`] are deduplicated without
    /// file [`IO`](io).
    ///
    /// ### Errors
    ///
    /// - [`io::Error::Schema`] wrapping [`Error::Collision`][1] if a schema is already registered
    ///   with the requested `name` but an incompatible column layout.
    /// - [`io::Error::Io`] if the underlying [write-cycle](io) fails.
    ///
    /// [1]: crate::schema::Error::Collision
    pub async fn schema<I>(&mut self, name: &str) -> Result<Accumulator<I>, io::Error>
    where
        I: Data,
    {
        let mut schema = Schema::new(name);
        let boxed = I::accumulator(&mut schema)?;
        Ok(Accumulator {
            data: boxed,
            name: name.to_string(),
            schema: schema.finish(self).await?,
        })
    }

    /// [`Write`][1] the accumulated data and return the number of written items.
    ///
    /// Empty accumulators are ignored.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Io`][2] if the underlying [write-cycle](io) fails, or [`Error::Number`][3]
    /// if a `u64` overflow occurs while computing the on-disk [`Sector`][4].
    ///
    /// [1]: crate::segment::Segment::write
    /// [2]: io::Error::Io
    /// [3]: io::Error::Number
    /// [4]: io::Sector
    pub async fn write<I>(&mut self, accumulator: Accumulator<I>) -> Result<u64, io::Error> {
        let count = match accumulator.is_empty() {
            true => return Ok(0),
            false => accumulator.count(),
        };
        self.file.write(accumulator).await?;
        // SAFETY: Undefined behaviour if mapped region is modified (refer to mmap documentation)
        self.mmap = unsafe { self.file.mmap()? }.into();
        Ok(count)
    }

    /// Returns a **stable on-disk index** per unique [`item`](I) – in request order – writing
    /// unseen items to the [`Dataset`].
    ///
    /// The provided [collection](S) is deduplicated in a single **O(N)** pass, with unseen items
    /// [pushed](Accumulate::push) to an in-memory [`Accumulator`]:
    ///
    /// - [`I`] matches an existing or accumulated item → Reuse existing [`index`](N).
    /// - [`I`] is genuinely unique → Assign the next available index.
    ///
    /// Any accumulated items are then written to the [`Dataset`] as a single [`Segment`][1]. An
    /// empty accumulator performs no [`IO`](io). Exclusive `&mut` access ensures indices are
    /// assigned atomically. Indices are guaranteed to remain stable due to the immutable nature of
    /// the on-disk segment region.
    ///
    /// ### Guidance
    ///
    /// Items are deduplicated according to their [`Hash`] implementation. Implementers are advised
    /// to manually implement the [`Hash`] trait for full control over deduplication behaviour. A
    /// [transparent][2] wrapper may be necessary to override a pre-existing hash implementation.
    ///
    /// This function is generic over [`N`]; implementers can specify any [`Unsigned`] integer type
    /// according to the expected number of unique items and the desired return type. An [`Error`]
    /// is raised if any index overflows [`N`].
    ///
    /// ### Errors
    ///
    /// - [`Error::Schema`] wrapping [`Error::Collision`][3] if a [`Schema`] is already registered
    ///   with the requested `name` but an incompatible column layout.
    /// - [`Error::Query`] if a failure occurs during [`Iterator`] construction.
    /// - [`Error::Number`] if an index overflows [`N`].
    /// - [`Error::Io`] if [deserialization][3] or the underlying [write-cycle](io) fails.
    ///
    /// [1]: crate::segment::Segment
    /// [2]: https://doc.rust-lang.org/nomicon/other-reprs.html#reprtransparent
    /// [3]: crate::schema::Error::Collision
    /// [3]: io::Deserialize

    pub async fn get_or_insert<N, I, S>(&mut self, name: &str, items: S) -> Result<Box<[N]>, Error>
    where
        N: Unsigned,
        S: IntoIterator<Item = I>,
        I: Data + Read + Eq + Hash + 'static,
        for<'q> I::Src<'q>: Composite<'q, Query> + Iterator<Item = Outcome<I>> + 'q,
    {
        let mut acc = self.schema::<I>(name).await?;
        let query = self.query(name)?;
        let mut map = query.unique::<I, N>()?;
        let count = query.count(); // initial number of items (includes duplicates)
        let mut next = N::try_from(count).ok();
        let items = items.into_iter();
        let mut out = Vec::with_capacity(items.size_hint().0);
        for item in items {
            out.push(match map.entry(item) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => Self::insert(entry, &mut next)?,
            });
        }
        let mut new: Box<[(I, N)]> = map.into_iter().filter(|e| e.1.as_u64() >= count).collect();
        new.sort_unstable_by_key(|e| e.1);
        new.into_iter().for_each(|e| acc.push(e.0));
        self.write(acc).await?;
        Ok(out.into_boxed_slice())
    }

    /// [`Insert`](VacantEntry::insert) the next available index into the provided [`VacantEntry`].
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`][1] if the inserted index overflows [`N`].
    ///
    /// [1]: number::Error::Zero
    fn insert<I, N>(entry: VacantEntry<I, N>, next: &mut Option<N>) -> Result<N, number::Error>
    where
        N: Unsigned,
    {
        let index = next.ok_or(number::Error::Zero)?;
        *next = index.checked_add(N::ONE);
        entry.insert(index);
        Ok(index)
    }

    /// Initialise a new [`Query`] over the named [`Schema`](manifest::Schema).
    ///
    /// The query begins with **every** column and **every** buffer from the specified schema.
    /// Each [`Column`](query::Column) is filtered subtractively to reduce the result set. No file
    /// [`IO`](io) occurs until the query is executed via a terminal method such as [`Query::read`]
    /// or [`Query::item`].
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Column`][2] if the requested `name` is not found in the [`Manifest`][3].
    ///
    /// [1]: manifest::Schema
    /// [2]: query::Error::Column
    /// [3]: manifest::Manifest
    pub fn query(&self, name: &str) -> Result<Query, query::Error> {
        let columns = self
            .file
            .manifest
            .schemas
            .get(name)
            .ok_or_else(|| query::Error::Column { name: name.into() })?
            .columns
            .iter()
            .map(query::Column::map) // Clone each entry
            .collect();
        Ok(Query {
            mmap: self.mmap.clone(), // Inexpensive Arc Clone
            columns,
        })
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::io::{File, SizedBuf};
    use crate::manifest::Buffer;
    use crate::schema::{self, number, Type};
    use crate::segment::Variant;
    use crate::{BoxAcc, Columns, Describe, Serialize};

    /// A minimal external record used to exercise the write path in white-box layout tests. The
    /// [`Data`] implementation below mirrors the code generated by `#[derive(Data)]`; clem-core
    /// cannot depend on the derive crate. Round-trip reads are covered by the integration tests in
    /// the project `tests` directory, which use the real procedural macros.
    #[derive(Debug, PartialEq)]
    struct Row {
        v: u32,
    }

    /// Generated-style composite accumulator holding one boxed sub-accumulator per [`Row`] field.
    struct Acc {
        v: BoxAcc<u32>,
    }

    impl Accumulate<Row> for Acc {
        fn push(&mut self, value: Row) {
            self.v.push(value.v);
        }

        fn discard(&mut self) {
            self.v.discard();
        }

        fn is_empty(&self) -> bool {
            self.v.is_empty()
        }

        fn count(&self) -> u64 {
            self.v.count()
        }
    }

    impl Describe<Row> for Acc {
        fn boxed(&self) -> BoxAcc<Row> {
            Box::new(Acc { v: self.v.boxed() })
        }

        fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, schema::Error> {
            self.v.buffers(offset, columns)
        }
    }

    impl Serialize for Acc {
        type Buffer = Vec<u8>;

        fn size(&self) -> Result<NonZeroU64, number::Error> {
            SizedBuf::new(&*self.v).size()
        }

        fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], number::Error> {
            SizedBuf::new(&*self.v).serialize_into(buf)
        }

        fn serialize(&self) -> Result<Vec<u8>, number::Error> {
            let size = self.size()?.get().try_into().map_err(number::Error::from)?;
            let mut buf = vec![0u8; size];
            self.serialize_into(&mut buf)?;
            Ok(buf)
        }
    }

    impl Data for Row {
        fn accumulator(schema: &mut Schema) -> Result<BoxAcc<Row>, schema::Error> {
            Ok(Box::new(Acc { v: schema.column::<u32>("v")? }))
        }
    }

    /// Unique scratch path for a layout test, cleared before use.
    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("clem-{name}.clem"));
        std::fs::remove_file(&path).ok();
        path
    }

    /// Write `u32` values into the `readings` schema via the public [`Dataset`] API; one data
    /// segment per batch.
    async fn write(path: &Path, batches: &[&[u32]]) {
        let mut dataset = Dataset::new(path).await.expect("new failed");
        for values in batches {
            let mut acc = dataset.schema::<Row>("readings").await.expect("schema failed");
            values.iter().for_each(|&v| acc.push(Row { v }));
            dataset.write(acc).await.expect("write failed");
        }
    }

    /// Each committed buffer [`Sector`](crate::Sector) begins at a 64-bit alignment boundary.
    #[test]
    fn buffers_align_to_boundary() {
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
                .map(Buffer::offset)
                .collect();
            std::fs::remove_file(&path).ok();
            assert_eq!(offsets.len(), 2);
            offsets.iter().for_each(|offset| assert_eq!(offset % 8, 0));
        });
    }

    /// A registered schema is persisted as an on-disk [`Schema`](crate::Schema) segment; the
    /// recorded [`Sector`](crate::Sector) points at the segment (variant `0x01`), never at the data
    /// segment that follows it.
    #[test]
    fn schema_segment_persisted() {
        smol::block_on(async {
            let path = scratch("schema-persisted");
            write(&path, &[&[1, 2, 3]]).await;
            let file = File::open(&path).await.expect("open failed");
            let sector = file.manifest.schemas.get("readings").expect("schema missing").sector;
            let bytes = std::fs::read(&path).expect("read failed");
            std::fs::remove_file(&path).ok();
            assert_eq!(bytes[sector.offset as usize], Variant::Schema as u8);
        });
    }

    /// Each data segment header references the on-disk schema segment by offset; the pointer
    /// resolves to a real [`Schema`](crate::Schema) segment rather than dangling into unrelated
    /// bytes.
    #[test]
    fn data_segment_references_schema() {
        smol::block_on(async {
            let path = scratch("schema-pointer");
            write(&path, &[&[1, 2, 3]]).await;
            let file = File::open(&path).await.expect("open failed");
            let sector = file.manifest.schemas.get("readings").expect("schema missing").sector;
            let bytes = std::fs::read(&path).expect("read failed");
            std::fs::remove_file(&path).ok();
            // The data segment begins immediately after the schema segment.
            let data = sector.next().expect("sector overflow").get() as usize;
            assert_eq!(bytes[data], Variant::Data as u8);
            // Segment layout: variant (1) + size (8) + schema offset (8) + count (8) + padding.
            let ptr = u64::from_le_bytes(bytes[data + 9..data + 17].try_into().expect("8 bytes"));
            assert_eq!(ptr, sector.offset);
        });
    }

    /// Registering an incompatible schema under an existing name is rejected, and the original
    /// registration is left intact — never overwritten.
    #[test]
    fn schema_collision_rejected() {
        smol::block_on(async {
            let path = scratch("schema-collision");
            let mut dataset = Dataset::new(&path).await.expect("new failed");
            let mut first = Schema::new("readings");
            first.column::<u32>("v").expect("column failed");
            first.finish(&mut dataset).await.expect("first finish failed");
            let mut second = Schema::new("readings");
            second.column::<u64>("v").expect("column failed");
            let result = second.finish(&mut dataset).await;
            let column = dataset.file.manifest.schemas.get("readings").expect("schema missing");
            let ty = column.columns.get("v").expect("column missing").ty.clone();
            std::fs::remove_file(&path).ok();
            assert!(result.is_err()); // Incompatible re-registration is rejected
            assert_eq!(ty, Type::U32); // Original column type survives (never overwritten)
        });
    }

    /// A single-byte corruption inside the manifest region is detected when the file is reopened;
    /// the file header points at a manifest segment whose trailing checksum no longer verifies.
    #[test]
    fn manifest_corruption_detected_on_open() {
        smol::block_on(async {
            let path = scratch("corrupt-manifest");
            write(&path, &[&[1, 2, 3]]).await;
            let mut bytes = std::fs::read(&path).expect("read failed");
            let last = bytes.len() - 1;
            bytes[last] ^= u8::MAX; // Corrupt the manifest segment checksum trailer
            std::fs::write(&path, &bytes).expect("write failed");
            let err = File::open(&path).await.expect_err("corruption undetected");
            std::fs::remove_file(&path).ok();
            assert!(matches!(err, io::Error::Checksum));
        });
    }
}
