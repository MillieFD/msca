/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Data **streaming** interface for [query](crate::query) execution.
//!
//! ---
//!
//! [clem](crate) maximises IO performance by storing on-disk data as columnar [buffers](Buffer)
//! optimised for range-based queries across an arbitrary number of dimensions; however, this
//! underlying format is generally unsuitable for direct manipulation by end-users.
//!
//! This module provides an [iterator-based](Iterator) interface to coordinate the transition from
//! raw binary data into supported rust types; corresponding to **phase 3** of the [read-cycle](io).
//!
//! ### Segment Composition
//!
//! Each [`Dataset`][1] is partitioned into self-describing segments which are immutable once
//! written. Each segment begins with a minimal header consisting of a [`variant`][2] identifier and
//! [`next`](num::NonZeroU64) offset.
//!
//! - [`Schema`][3] segments describe the structure of encoded data.
//! - [`Data`][4] segments carry columnar [buffers](Buffer) for a specified schema.
//!
//! Multimodality and schema evolution are realised by appending additional schema segments. Data
//! storage and file extensibility are realised by appending additional data segments. Format
//! extensibility may be achieved via the introduction of new segment variants in future releases.
//!
//! ### Lazy Zero-Copy Reads
//!
//! Each [`Query`][5] column is packaged into a lazy zero-copy [`Stream`] that:
//!
//! 1. Pulls bytes from the retained on-disk [buffers](Buffer).
//! 2. [Deserializes](Deserialize) bytes into the requested Rust type.
//! 3. Evaluates query [filters](Filter) on the deserialized item.
//!
//! Streams chain transparently across segments, abstracting away the underlying file structure to
//! provide a seamless interface for end-users.
//!
//! ### Concurrency Model
//!
//! Segments are immutable once written, meaning readers do not require coordination after
//! extracting their list of candidate segments. A concurrent writer appending a new segment must
//! acquire exclusive mutable access to update the [manifest][3] and file [header](io::Header). This
//! temporarily blocks new readers from accessing the manifest but does not affect in-flight reads.
//!
//! This design ensures:
//!
//! - **Multiple readers** can build candidate segment lists from the manifest simultaneously.
//! - **A writer** updating the manifest does not block phase three readers.
//! - **Segment IO** is fully parallel; readers and writers never contend on segment data regions.
//!
//! This module addresses **phase three** of the [read-cycle](io).
//!
//! [1]: crate::dataset::Dataset
//! [2]: crate::segment::Variant
//! [3]: crate::schema::Schema
//! [4]: crate::Data
//! [5]: crate::query::Query

use std::collections::HashSet;
use std::slice::Iter;
use std::{iter, num};

use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;
use memmap2::Mmap;
use smol::stream::StreamExt;

use crate::io::{Deserialize, Deserializer, Error};
use crate::manifest::Buffer;
use crate::query::{Evaluate, Filter};

/* ------------------------------------------------------------------------------ Public Exports */

/// Shorthand type-erased stack-allocated [pointer](Box) to a lazy [`Iterator`] yielding one
/// [`Outcome`] per candidate [`Item`](I), or [`None`] once every candidate [`Buffer`] is consumed.
pub type Stream<'a, I> = Box<dyn Iterator<Item = Outcome<I>> + 'a>;

/// The result of [deserializing](Deserialize) one [`Item`](I) from a [`Read`](Read) [`Stream`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub enum Outcome<I> {
    /// A [deserialized](Deserialize::deserialize) [`Item`](I) which satisfies every [`Filter`].
    Include(I),
    /// The [`Item`](I) was rejected by one or more [filters](Filter).
    Exclude,
    /// An [`Error`] occurred during [deserialization](Deserialize) or [filtering](Filter).
    Error(Error),
}

impl<I> Outcome<I> {
    fn map<F, O>(&self, f: F) -> Outcome<O>
    where
        F: FnMut(I) -> O,
    {
        match self {
            Outcome::Include(v) => Self::Include(f(v)),
            Outcome::Exclude => Self::Exclude,
            Outcome::Error(e) => e.into(),
        }
    }
}

impl<I> From<Error> for Outcome<I> {
    fn from(e: Error) -> Self {
        Outcome::Error(e)
    }
}

/// A minimal columnar **data source** with [deserialization](Deserialize) context; used during
/// [`Query`](crate::Query) execution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct Column<'a> {
    /// [`Iterator`] over the retained [`Buffer`] descriptors for this [`Column`] across all data
    /// segments; advanced in situ as each buffer is exhausted.
    pub(crate) buffers: Iter<'a, Buffer>,
    /// Read-only [memory map](Mmap) backed by the immutable segment region of a [clem](crate) file.
    ///
    /// Refer to the [safety documentation](io::File::mmap) for details.
    pub(crate) mmap: &'a Mmap,
    /// Deduplicated [`Filter`](Filter) [`Set`](HashSet) used to [`Evaluate`] deserialized items.
    pub(crate) filters: &'a HashSet<Filter>,
}

impl<'a> Column<'a> {
    fn stream<I>(&mut self) -> Stream<I>
    where
        I: Read + Evaluate,
    {
        self.buffers
            .flat_map(|buf| buf.sector.slice(self.mmap))
            .flat_map(match I::Src::try_from {
                Ok(src) => src.boxed(self.filters),
                Err(e) => iter::once(Outcome::Error(e)).into(),
            })
            .into()
    }
}

/// A **stateful cursor** over paired validity and value data streams for a single [`Column`]; used
/// to [`Deserialize`] optional non-niche items.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
struct OptBitVec<'a, I> {
    /// Validity bits in [`Lsb0`] order. One bit per item.
    ///
    /// - `true` → [`Some`]
    /// - `false` → [`None`]
    bits: Stream<'a, bool>,
    /// Data **source** from which items are [deserialized](Deserialize).
    data: Stream<'a, I>,
}

impl<'a, I> TryFrom<&'a [u8]> for OptBitVec<'a, I> {
    type Error = Error;

    fn try_from(src: &'a [u8]) -> Result<Self, Self::Error> {
        todo!("read the validity bits and data buffer into Self")
    }
}

/// A **stateful cursor** over paired offset and value data streams for a single [`Column`]; used to
/// [`Deserialize`](Deserialize) [unsized][1] items.
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[doc(hidden)] // Reachable via Read::Src for unsized readers
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
struct Seq<'a, I> {
    /// Cumulative non-zero end offsets for each [`Item`](I).
    ///
    /// The [`u64::MIN`] niche is used to encode [`None`] for optional unsized items.
    offsets: Stream<'a, u64>,
    /// Inclusive start offset for the next item.
    start: u64,
    /// Flattened data **source** from which items are [deserialized](Deserialize).
    data: Stream<'a, I>,
}

/* --------------------------------------------------------------------- Reader Trait Definition */

trait Reader<I> {
    /// Additional context required to construct a new empty instance of [`Self`].
    type Ctx;

    /// Returns a new empty instance of [`Self`] boxed as a [`Stream`] trait object.
    fn boxed(&self, ctx: Self::Ctx) -> Stream<I>;
}

/* ----------------------------------------------------------------- Reader Trait Implementation */

impl<'a, I> Reader<I> for &'a [u8]
where
    I: Deserialize + Evaluate,
{
    type Ctx = &'a HashSet<Filter>;

    fn boxed(&self, ctx: Self::Ctx) -> Stream<I> {
        let iter = iter::from_fn(move || {
            self.deserialize_into().map_or_else(
                |error| match error {
                    Error::Truncated { actual: 0, .. } => None,
                    other => Outcome::Error(other).into(),
                },
                |item: I| item.evaluate(ctx).into(),
            )
        });
        Box::new(iter)
    }
}

impl<'a> Reader<bool> for &'a BitSlice<u8, Lsb0> {
    type Ctx = &'a HashSet<Filter>;

    fn boxed(&self, ctx: Self::Ctx) -> Stream<bool> {
        let iter = iter::from_fn(move || {
            self.split_first().map_or_else(
                |error| match error {
                    Error::Truncated { actual: 0, .. } => None,
                    other => Outcome::Error(other).into(),
                },
                |item: bool| item.evaluate(ctx).into(),
            )
        });
        Box::new(iter)
    }
}

impl<'a, I> Reader<Option<I>> for OptBitVec<'a, I>
where
    Option<I>: Read<Src<'a> = Self>,
{
    type Ctx = &'a HashSet<Filter>;

    fn boxed(&self, ctx: Self::Ctx) -> Stream<Option<I>> {
        let mut bits = self.bits;
        let mut data = self.data;
        let iter = iter::from_fn(move || match bits.next()? {
            Outcome::Include(true) => data.next()?.map(Some).into(),
            Outcome::Include(false) => Outcome::Include(None).into(),
            other => other.into(),
        });
        Box::new(iter)
    }
}

impl<'a, I> Reader<I> for Seq<'a, I>
where
    I: Read<Src<'a> = Self>,
{
    type Ctx = &'a HashSet<Filter>;

    fn boxed(&self, ctx: Self::Ctx) -> Stream<I> {
        todo!()
    }
}

/* ----------------------------------------------------------------------- Read Trait Definition */

/// A **data type** that can be lazily [streamed](Stream) from a [`Dataset`](crate::Dataset).
///
/// ### Guidance
///
/// Default implementations are provided for all supported primitive types. Implementors are advised
/// to [`derive`][1] this trait for composite types, which zips one [sub-stream](Stream) per field.
// [1]: TODO → add link to clem-derive crate or feature
pub trait Read {
    /// The [stateful data source](Reader) from which to [`Deserialize`] values of [`Self`].
    type Src<'a>: Reader<Self> + TryFrom<&'a [u8]>;
}

/* ------------------------------------------------------------------- Read Trait Implementation */

impl Read for u8 {
    type Src<'a> = &'a [u8];
}

impl Read for u16 {
    type Src<'a> = &'a [u8];
}

impl Read for u32 {
    type Src<'a> = &'a [u8];
}

impl Read for u64 {
    type Src<'a> = &'a [u8];
}

impl Read for u128 {
    type Src<'a> = &'a [u8];
}

impl Read for i8 {
    type Src<'a> = &'a [u8];
}

impl Read for i16 {
    type Src<'a> = &'a [u8];
}

impl Read for i32 {
    type Src<'a> = &'a [u8];
}

impl Read for i64 {
    type Src<'a> = &'a [u8];
}

impl Read for i128 {
    type Src<'a> = &'a [u8];
}

impl Read for f32 {
    type Src<'a> = &'a [u8];
}

impl Read for f64 {
    type Src<'a> = &'a [u8];
}

impl Read for char {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroU8 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroU16 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroU32 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroU64 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroU128 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroI8 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroI16 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroI32 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroI64 {
    type Src<'a> = &'a [u8];
}

impl Read for num::NonZeroI128 {
    type Src<'a> = &'a [u8];
}

impl Read for bool {
    type Src<'a> = &'a BitSlice<u8, Lsb0>;
}

impl Read for Option<u8> {
    type Src<'a> = OptBitVec<'a, u8>;
}

impl Read for Option<u16> {
    type Src<'a> = OptBitVec<'a, u16>;
}

impl Read for Option<u32> {
    type Src<'a> = OptBitVec<'a, u32>;
}

impl Read for Option<u64> {
    type Src<'a> = OptBitVec<'a, u64>;
}

impl Read for Option<u128> {
    type Src<'a> = OptBitVec<'a, u128>;
}

impl Read for Option<i8> {
    type Src<'a> = OptBitVec<'a, i8>;
}

impl Read for Option<i16> {
    type Src<'a> = OptBitVec<'a, i16>;
}

impl Read for Option<i32> {
    type Src<'a> = OptBitVec<'a, i32>;
}

impl Read for Option<i64> {
    type Src<'a> = OptBitVec<'a, i64>;
}

impl Read for Option<i128> {
    type Src<'a> = OptBitVec<'a, i128>;
}

impl Read for Option<f32> {
    type Src<'a> = OptBitVec<'a, f32>;
}

impl Read for Option<f64> {
    type Src<'a> = OptBitVec<'a, f64>;
}

impl Read for Option<bool> {
    type Src<'a> = OptBitVec<'a, bool>;
}

impl Read for Option<char> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroU8> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroU16> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroU32> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroU64> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroU128> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroI8> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroI16> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroI32> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroI64> {
    type Src<'a> = &'a [u8];
}

impl Read for Option<num::NonZeroI128> {
    type Src<'a> = &'a [u8];
}

impl<I> Read for Vec<I>
where
    I: Read,
{
    type Src<'a> = Seq<'a, I>;
}

impl<I> Read for Option<Vec<I>>
where
    I: Read,
{
    type Src<'a> = Seq<'a, I>;
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::num::NonZeroU64;

    use bitvec::vec::BitVec;
    use memmap2::MmapMut;

    use super::*;
    use crate::accumulate::Serialize;
    use crate::Sector;

    /// Build a read-only [`Mmap`] from the provided bytes for stream unit tests.
    fn map(bytes: &[u8]) -> Mmap {
        let mut mmap = MmapMut::map_anon(bytes.len().max(1)).expect("Anonymous map failed");
        mmap[..bytes.len()].copy_from_slice(bytes);
        mmap.make_read_only().expect("Read-only conversion failed")
    }

    /// Build a single buffer covering `len` bytes from `offset` with the provided row `count`.
    fn buffer(offset: u64, len: u64, count: u64) -> Buffer {
        Buffer {
            sector: Sector {
                offset,
                length: NonZeroU64::new(len).expect("Empty buffer"),
            },
            count: NonZeroU64::new(count).expect("Zero rows"),
            min: [0x00; 16],
            max: [0xFF; 16],
        }
    }

    /// Drain a [`Stream`] into a [`Vec`], dropping every [excluded](Outcome::Excluded) row and
    /// panicking on any [`Error`](Outcome::Error) outcome.
    fn drain<I>(stream: Stream<'_, I>) -> Vec<I>
    where
        I: Debug,
    {
        stream
            .filter_map(|outcome| match outcome {
                Outcome::Success(item) => Some(item),
                Outcome::Excluded => None,
                other => panic!("Unexpected outcome → {other:?}"),
            })
            .collect()
    }

    #[test]
    fn values_round_trip() {
        let data: Vec<u32> = vec![10, 20, 30];
        let bytes = data.serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, bytes.len() as u64, 3)];
        let filters = HashSet::new();
        let ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        assert_eq!(drain(u32::boxed(ctx)), data);
    }

    #[test]
    fn values_chains_buffers() {
        let bytes = vec![1u16, 2].serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![
            buffer(0, bytes.len() as u64, 2),
            buffer(0, bytes.len() as u64, 2),
        ];
        let filters = HashSet::new();
        let ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        assert_eq!(drain(u16::boxed(ctx)), vec![1, 2, 1, 2]);
    }

    #[test]
    fn filter_excludes_out_of_range() {
        let data: Vec<u32> = vec![10, 20, 30, 40];
        let bytes = data.serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, bytes.len() as u64, 4)];
        let filters = HashSet::from([Filter::bounds(&(20u32..40))]);
        let ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        assert_eq!(drain(u32::boxed(ctx)), vec![20, 30]);
    }

    /// A [`Filter`] disjoint from every item returns an empty result set.
    #[test]
    fn disjoint_filter_excludes_all() {
        let data: Vec<u32> = vec![10, 20, 30];
        let bytes = data.serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, bytes.len() as u64, 3)];
        let filters = HashSet::from([Filter::bounds(&(100u32..200))]);
        let ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        assert!(drain(u32::boxed(ctx)).is_empty());
    }

    #[test]
    fn bits_round_trip() {
        let data: BitVec = [true, false, true, true].into_iter().collect();
        let bytes = data.serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, bytes.len() as u64, 4)];
        let filters = HashSet::new();
        let ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        assert_eq!(drain(bool::boxed(ctx)), vec![true, false, true, true]);
    }

    #[test]
    fn next_refills_from_buffers() {
        let bytes = vec![7u16].serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, bytes.len() as u64, 1)];
        let filters = HashSet::new();
        let mut ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        let mut src = b"".iter(); // Empty source; next must refill from the first buffer.
        assert!(matches!(u16::next(&mut src, &mut ctx), Outcome::Success(7)));
        assert!(matches!(u16::next(&mut src, &mut ctx), Outcome::Finished));
    }

    #[test]
    fn next_refills_bits_from_buffers() {
        let data: BitVec = [true, false].into_iter().collect();
        let bytes = data.serialize().expect("Serialize failed");
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, bytes.len() as u64, 2)];
        let filters = HashSet::new();
        let mut ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        let mut src = Default::default(); // Empty cursor; next must refill from the first buffer.
        assert!(matches!(
            bool::next(&mut src, &mut ctx),
            Outcome::Success(true)
        ));
        assert!(matches!(
            bool::next(&mut src, &mut ctx),
            Outcome::Success(false)
        ));
        assert!(matches!(bool::next(&mut src, &mut ctx), Outcome::Finished));
    }

    #[test]
    fn next_finished_on_empty() {
        let mmap = map(b"");
        let filters = HashSet::new();
        let buffers: Vec<Buffer> = Vec::new();
        let mut ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        let mut src = b"".iter();
        assert!(matches!(u32::next(&mut src, &mut ctx), Outcome::Finished));
    }

    #[test]
    fn truncated_buffer_errors_then_chains() {
        // Buffer one carries a dangling byte that cannot encode a second u16; buffer two is intact.
        let mut bytes = vec![1u16].serialize().expect("Serialize failed");
        bytes.push(9); // Dangling byte
        let offset = bytes.len() as u64;
        let tail = vec![2u16].serialize().expect("Serialize failed");
        let length = tail.len() as u64;
        bytes.extend_from_slice(&tail);
        let mmap = map(&bytes);
        let buffers = vec![buffer(0, offset, 1), buffer(offset, length, 1)];
        let filters = HashSet::new();
        let ctx = Column {
            buffers: buffers.iter(),
            mmap: &mmap,
            filters: &filters,
        };
        let outcomes: Vec<Outcome<u16>> = u16::boxed(ctx).collect();
        assert!(matches!(
            outcomes[..],
            [Outcome::Success(1), Outcome::Error(_), Outcome::Success(2)]
        ));
    }
}
