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
use std::ops::Mul;
use std::slice::Iter;

use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;
use bitvec::view::BitView;
use memmap2::Mmap;

use crate::io::{self, Deserialize};
use crate::manifest::Buffer;
use crate::query::{self, Filter};
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
#[derive(Copy, Clone, Debug)]
pub struct Column<'a> {
    /// Retained [`Buffer`] descriptors for this [`Column`] across all data segments.
    pub(crate) buffers: &'a [Buffer],
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
        let count = buffer.count.get().try_into()?;
        let bytes = buffer.sector.slice(self.mmap)?.view_bits::<Lsb0>();
        let actual = bytes.len().mul(8);
        bytes.get(..count).ok_or(io::Error::truncated(count, actual))
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
    /// Primitive types read from a [`Column`]; composite types read from a [`Query`](crate::Query).
    type Ctx<'a>;

    /// Pull the exact number of bytes required to [deserialize](Deserialize) one instance of
    /// [`Self`] from `src`.
    ///
    /// Returns a read-only [memory map](Mmap) [slice][1] over the extracted bytes and advances
    /// `src` by the number of bytes read.
    ///
    /// ### Guidance
    ///
    /// The default implementation leverages [`size_of`]`::<Self>()` for fixed-size types. Unsized
    /// types must override this default implementation with type-specific size determination logic
    /// such as reading an on-disk [`length`][2] prefix.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Truncated`](io::Error::Truncated) if `src` contains fewer than the
    /// requested number of bytes.
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    /// [2]: std::num::NonZeroU64
    // TODO → Deserialize::take fulfils a similar function to Read::take.
    // TODO → Consider adding Read: Deserialize trait bound; then remove Read::take
    // TODO → Update Deserialize::take to split the source slice (zero copy)
    fn take<'a>(src: &mut Iter<'a, u8>) -> Result<&'a [u8], io::Error> {
        let n = size_of::<Self>();
        let (data, rem) = src
            .as_slice()
            .split_at_checked(n)
            .ok_or(io::Error::Truncated { expected: n, actual: src.len() })?;
        *src = rem.iter();
        Ok(data)
    }

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
    fn next(src: &mut Iter<'_, u8>, filters: &HashSet<Filter>) -> Outcome<Self>
    where
        Self: for<'b> Deserialize<Src<'b> = &'b [u8]> + PartialOrd,
    {
        // TODO → advance to the next buffer if current src is exhausted; replace src in situ
        // TODO → return Outcome::Finished only if no more buffers remain
        if src.as_slice().is_empty() {
            return Outcome::Finished;
        }
        let item = match <Self as Read>::take(src).and_then(Self::deserialize) {
            Ok(item) => item,
            Err(e) => return Outcome::Error(e),
        };
        match item.filter(filters) {
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
    /// This function provide provides the top-level iteration pipeline. Implementations should pull
    /// successive rules via [`Read::next`] and translate [`Outcome::Finished`] into [`None`] to
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