/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Data **streaming** interface for [query] execution.
//!
//! ---
//!
//! [`clem`](crate) maximises IO performance by storing on-disk data as columnar [buffers](Buffer)
//! optimised for range-based queries across an arbitrary number of dimensions; however, this
//! underlying format is generally unsuitable for direct manipulation by end-users.
//!
//! This module provides an [iterator-based](Iterator) interface to coordinate the transition from
//! raw binary data into supported rust types; corresponding to **phase 3** of the [read-cycle](io).
//! The on-disk layout minimises contention for multiple simultaneous readers.
//!
//! ### Segment Composition
//!
//! Each [clem](crate) dataset is partitioned into self-describing segments which are immutable once
//! written. Each segment begins with a minimal header consisting of a [`variant`][1] identifier and
//! [`length`](NonZeroU64).
//!
//! - [`Schema`] segments describe the structure of encoded data.
//! - [`Data`][2] segments carry columnar [buffers](Buffer) for a specified schema.
//!
//! Multimodality and schema evolution are realised by appending additional schema segments. Data
//! storage and file extensibility are realised by appending additional data segments. Format
//! extensibility may be achieved via the introduction of new segment variants in future releases.
//!
//! ### Lazy Zero-Copy Reads
//!
//! Each [`Query`](query::Query) column is packaged into a lazy zero-copy [`Stream`] that:
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
//! [1]: crate::segment::Variant
//! [2]: crate::Data
//! [3]: crate::manifest::Manifest

use std::collections::HashSet;
use std::iter::from_fn;
use std::slice::Iter;

use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;
use bitvec::view::BitView;
use memmap2::Mmap;

use crate::io::{self, Deserialize};
use crate::manifest::Buffer;
use crate::query::Filter;
use crate::schema::{Schema, Unfolder};

/* ------------------------------------------------------------------------------ Public Exports */

/// Shorthand type-erased stack-allocated [pointer](Box) to a lazy [`Iterator`] yielding one
/// deserialized [`Outcome`] per candidate [`Item`](I).
///
/// Constructed via [`Read::boxed`]. Returns [`None`] once every candidate [`Buffer`] is consumed.
pub type Stream<'a, I> = Box<dyn Iterator<Item = Outcome<I>> + 'a>;

/// The result of [deserializing](Deserialize) one [`Item`](I) from a [`Read`](Read) [`Stream`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub enum Outcome<I> {
    /// A [deserialized](Deserialize::deserialize) [`Item`](I) which satisfies every [`Filter`].
    Success(I),
    /// The [`Item`](I) was rejected by one or more [filters](Filter) during [deserialization][1].
    ///
    /// [1]: Deserialize::deserialize
    Excluded,
    /// Every candidate [`Item`](I) has been [`Read`].
    Finished,
    /// An [`Error`](io::Error) occurred while [deserializing](Deserialize) or [filtering](Filter)
    /// the [`Item`](I).
    Error(io::Error),
}

/// A minimal column **data source** with [deserialization](Deserialize) context; used during
/// [`Query`] execution.
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
    /// Deduplicated [`Filter`] set used to [`evaluate`](Filter::evaluate) deserialized items.
    pub(crate) filters: &'a HashSet<Filter>,
}

impl<'a> Column<'a> {
    /// Returns a read-only [memory map](Mmap) [slice][1] over the raw data bytes of the specified
    /// [`Buffer`]. Excludes the buffer [`header`](Buffer::HEADER).
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Truncated`](io::Error::Truncated) if the buffer extends beyond the end of
    /// the [`Mmap`] or is shorter than the fixed-length buffer header.
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    fn bytes(&self, buffer: &Buffer) -> Result<&'a [u8], io::Error> {
        let bytes = buffer.sector.slice(self.mmap)?;
        let actual = bytes.len();
        bytes.get(Buffer::HEADER..).ok_or(io::Error::truncated(Buffer::HEADER, actual))
    }

    /// Returns a read-only [memory map](Mmap) [`BitSlice`] over the raw data bytes of the specified
    /// [`Buffer`].
    ///
    /// Excludes the buffer [`header`](Buffer::HEADER) and leverages [`Buffer::count`] to discard
    /// any trailing bit padding.
    ///
    /// ### Errors
    ///
    /// - [`Error::Truncated`](io::Error::Truncated) if the buffer extends beyond the end of
    /// the [`Mmap`] or contains fewer bits than the expected `count`.
    /// - [`Error::Number`](io::Error::Number) if the row count overflows [`usize`].
    fn bits(&self, buffer: &Buffer) -> Result<&'a BitSlice<u8, Lsb0>, io::Error> {
        let bytes = buffer.sector.slice(self.mmap)?;
        let bits = bytes
            .get(Buffer::HEADER..)
            .ok_or_else(|| io::Error::truncated(Buffer::HEADER, bytes.len()))?
            .view_bits::<Lsb0>();
        let count: usize = buffer.count.get().try_into()?;
        bits.get(..count).ok_or_else(|| io::Error::truncated(count, bits.len()))
    }
}

/* ----------------------------------------------------------------------- Read Trait Definition */

/// An in-memory **data type** that can be lazily [deserialized](Deserialize) and [filtered](Filter)
/// from a [clem](crate) file as a [`Stream`] of [`Outcome<Self>`](Outcome) items.
///
/// ### Guidance
///
/// Default implementations are provided for all supported primitive types. Implementors are advised
/// to [`#[derive(Read)]`][1] for composite types, which zips one [`Stream`] per field and applies
/// the appropriate [filters](Filter) during iteration.
// [1]: TODO → add link to clem-derive crate or feature
pub trait Read: Sized {
    /// Additional context required to construct a [`Stream`] of [`Self`].
    ///
    /// Primitive types read from a [`Column`]. Composite types read from a zipped context holding
    /// one [`Stream`] per field; constructed from a [`Query`](crate::Query) via [`TryFrom`].
    type Ctx<'a>;

    /// Evaluate [`self`](Read) against every [`Filter`]:
    ///
    /// - `true` ← All filters pass
    /// - `false` ← One or more filters fail
    ///
    /// Items are excluded from the result set if any filter fails.
    ///
    /// ### Errors
    ///
    /// Returns [`Error`](io::Error) if a stored filter bound cannot [`Deserialize`] as [`Self`].
    fn filter(&self, filters: &HashSet<Filter>) -> Result<bool, io::Error>
    where
        Self: for<'b> Deserialize<Src<'b> = &'b [u8]> + PartialOrd,
    {
        filters.iter().try_fold(true, |keep, filter| match keep {
            true => filter.evaluate(self),
            false => Ok(false),
        })
    }

    /// [`Deserialize`] and [`Filter`] one instance of [`Self`] from `src`.
    fn next<'a>(src: &mut Iter<'a, u8>, ctx: &mut Column<'a>) -> Outcome<Self>
    where
        Self: for<'b> Deserialize<Src<'b> = &'b [u8]> + PartialOrd,
    {
        while src.as_slice().is_empty() {
            let buffer = match ctx.buffers.next() {
                Some(buffer) => buffer,
                None => return Outcome::Finished,
            };
            match ctx.bytes(buffer) {
                Ok(data) => *src = data.iter(),
                Err(e) => return Outcome::Error(e),
            }
        }
        let bytes = match Self::take(src.as_slice()) {
            Ok(bytes) => bytes,
            Err(error) => {
                // NOTE: Discard the truncated remainder; resume from the next buffer to avoid loop
                *src = Default::default();
                return Outcome::Error(error);
            }
        };
        *src = src.as_slice().get(bytes.len()..).unwrap_or_default().iter();
        let item = match Self::deserialize(bytes) {
            Ok(item) => item,
            Err(e) => return Outcome::Error(e),
        };
        match item.filter(ctx.filters) {
            Ok(true) => Outcome::Success(item),
            Ok(false) => Outcome::Excluded,
            Err(e) => Outcome::Error(e),
        }
    }

    /// Construct a lazy [`Iterator`] from the provided [`context`](Self::Ctx); yielding one
    /// [deserialized](Deserialize) [`Outcome`] per candidate [`Item`](Self).
    ///
    /// ### Guidance
    ///
    /// This function provides the top-level iteration pipeline. Implementations should pull
    /// successive rows via [`Read::next`] and translate [`Outcome::Finished`] into [`None`] to
    /// terminate the [`Iterator`].
    ///
    /// ### Errors
    ///
    /// Refer to each implementation for a description of the possible error conditions.
    fn iter(ctx: Self::Ctx<'_>) -> Result<impl Iterator<Item = Outcome<Self>>, query::Error>;

    /// Construct a type-erased [`Stream`] of [`Self`] from the provided [`context`](Self::Ctx);
    /// uses [`Read::iter`] internally.
    ///
    /// ### Errors
    ///
    /// See [`Read::iter`] for a description of the possible error conditions.
    fn boxed<'a>(ctx: Self::Ctx<'a>) -> Result<Stream<'a, Self>, query::Error>
    where
        Self: 'a,
    {
        Ok(Box::new(Self::iter(ctx)?))
    }
}

/* ------------------------------------------------------------------- Read Trait Implementation */

/// Blanket [`Read`] implementation for fixed-width primitives.
impl<I> Read for I
where
    I: for<'b> Deserialize<Src<'b> = &'b [u8]> + PartialOrd,
    Schema: Unfolder<I>,
{
    type Ctx<'a> = Column<'a>;

    /// Construct a fixed-width byte pipeline over the retained column [buffers](Buffer); yielding
    /// one [`Outcome`] wrapping a [deserialized][1] instance of [`Self`] per iteration.
    ///
    /// ### Errors
    ///
    /// Iterator construction is infallible; this function will never return [`query::Error`].
    /// [Deserialization][1] failures are surfaced lazily via [`Outcome::Error`].
    ///
    /// Refer to the [trait-level documentation](Read::iter) for more details.
    ///
    /// [1]: Deserialize::deserialize
    // TODO → Is the Result return type required by composite readers? Could simplify fn signature?
    fn iter(mut ctx: Self::Ctx<'_>) -> Result<impl Iterator<Item = Outcome<Self>>, query::Error> {
        let mut src = Default::default();
        Ok(from_fn(move || match Self::next(&mut src, &mut ctx) {
            Outcome::Finished => None,
            outcome => Some(outcome),
        }))
    }
}

impl Read for bool {
    type Ctx<'a> = Column<'a>;

    /// Construct a [bit-packed](Column::bits) pipeline over the retained column [buffers](Buffer);
    /// yielding one [`Outcome`] wrapping a [deserialized][1] instance of [`Self`] per iteration.
    ///
    /// ### Errors
    ///
    /// Iterator construction is infallible; this function will never return [`query::Error`].
    /// [Deserialization][1] failures are surfaced lazily via [`Outcome::Error`].
    ///
    /// Refer to the [trait-level documentation](Read::iter) for more details.
    ///
    /// [1]: Deserialize::deserialize
    fn iter(mut ctx: Self::Ctx<'_>) -> Result<impl Iterator<Item = Outcome<Self>>, query::Error> {
        let mut src = BitSlice::empty().iter();
        Ok(from_fn(move || {
            loop {
                if let Some(bit) = src.next() {
                    return Some(Outcome::Success(*bit));
                }
                let buffer = ctx.buffers.next()?;
                match ctx.bits(buffer) {
                    Ok(valid) => src = valid.iter(),
                    Err(error) => return Some(Outcome::Error(error)),
                }
            }
        }))
    }
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
        assert_eq!(drain(u32::boxed(ctx).expect("Stream failed")), data);
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
        assert_eq!(
            drain(u16::boxed(ctx).expect("Stream failed")),
            vec![1, 2, 1, 2]
        );
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
        assert_eq!(drain(u32::boxed(ctx).expect("Stream failed")), vec![20, 30]);
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
        assert_eq!(
            drain(bool::boxed(ctx).expect("Stream failed")),
            vec![true, false, true, true]
        );
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
    fn next_finished_on_empty() {
        let mmap = map(b"");
        let buffers: Vec<Buffer> = Vec::new();
        let filters = HashSet::new();
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
        let outcomes: Vec<Outcome<u16>> = u16::boxed(ctx).expect("Stream failed").collect();
        assert!(matches!(
            outcomes[..],
            [Outcome::Success(1), Outcome::Error(_), Outcome::Success(2)]
        ));
    }
}
