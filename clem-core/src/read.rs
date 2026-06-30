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

use crate::io::{Deserialize, Error};
use crate::manifest::Buffer;
use crate::query::{Evaluate, Filter};
use crate::segment::Align;

/* ------------------------------------------------------------------------------ Public Exports */

/// Shorthand type-erased stack-allocated [pointer](Box) to a lazy [`Iterator`] yielding one
/// [`Outcome`] per candidate [`Item`](I), or [`None`] once every candidate [`Buffer`] is consumed.
pub type Stream<'a, I> = Box<dyn Iterator<Item = Outcome<I>> + 'a>;

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
    /// Construct a [`Stream`] that will lazily:
    ///
    /// 1. Pull bytes from the retained on-disk [buffers](Buffer).
    /// 2. [`Deserialize`] bytes into the requested [`item`](I) type.
    /// 3. [`Evaluate`] each query [`Filter`] on the deserialized item.
    ///
    /// Streams chain transparently across segments, abstracting away the underlying file structure
    /// to provide a seamless interface for end-users.
    ///
    /// Refer to the [module-level documentation](self) for details.
    pub(crate) fn stream<I>(self) -> Stream<'a, I>
    where
        I: Read + 'a,
        I::Src<'a>: Reader<'a, I> + TryFrom<&'a [u8], Error = Error>,
    {
        let stream = self.buffers.flat_map(move |buf| {
            buf.sector
                .slice(self.mmap)
                .map(|bytes| match I::Src::try_from(bytes) {
                    Ok(src) => src.boxed(self.filters),
                    Err(e) => Outcome::Error(e).once(),
                })
                .map_err(Outcome::Error)
                .unwrap_or_else(Outcome::once)
                .take(buf.count.get() as usize)
        });
        Box::from(stream)
    }
}

/// A **stateful cursor** over paired validity and value sub-buffers for a single [`Column`]; used
/// to [`Deserialize`] optional non-niche items.
#[doc(hidden)] // Reachable via Read::Src for optional non-niche readers
struct OptBitVec<'a> {
    /// [`Stream`] over the validity sub-buffer where `true` → [`Some`] and `false` → [`None`].
    mask: Stream<'a, bool>,
    /// Concatenated data sub-buffer from which [`Some`] items are [deserialized](Deserialize).
    data: &'a [u8],
}

/// A **stateful cursor** over paired offset and value sub-buffers for a single [`Column`]; used to
/// [`Deserialize`](Deserialize) [unsized][1] items.
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[doc(hidden)] // Reachable via Read::Src for unsized readers
struct Seq<'a> {
    /// [`Stream`] over the `ends` sub-buffer yielding one `u64` cumulative end offset for each
    /// [`Some`] or [`u64::MAX`] for [`None`].
    ends: Stream<'a, u64>,
    /// Concatenated data sub-buffer from which [`Some`] items are [deserialized](Deserialize).
    data: &'a [u8],
}

/* ------------------------------------------------------------------------- Read Stream Outcome */

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
    /// Map an [`Include`](Outcome::Include) [`Item`](I) through `f`, preserving
    /// [`Exclude`](Outcome::Exclude) and [`Error`](Outcome::Error). Consumes [`self`](Outcome) so the
    /// inner item can be moved into a wrapping type (for example [`Some`]) without cloning.
    fn map<F, O>(self, f: F) -> Outcome<O>
    where
        F: FnOnce(I) -> O,
    {
        match self {
            Self::Include(v) => Outcome::Include(f(v)),
            Self::Exclude => Outcome::Exclude,
            Self::Error(e) => Outcome::Error(e),
        }
    }

    /// Construct a [`Stream`] that yields [`self`](Outcome) exactly [once](iter::once).
    fn once<'a>(self) -> Stream<'a, I>
    where
        I: 'a,
    {
        let out = iter::once(self);
        Box::from(out)
    }
}

impl<I> From<Error> for Outcome<I> {
    fn from(e: Error) -> Self {
        Outcome::Error(e)
    }
}


    fn try_from(src: &'a [u8]) -> Result<Self, Self::Error> {
    }
}

/* --------------------------------------------------------------------- Reader Trait Definition */

/// A **stateful data source** used to construct a lazy [`Stream`].
#[doc(hidden)] // pub required for Query::column trait bounds; not intended as a stable API
pub trait Reader<I> {
    /// Returns a new instance of [`Self`] boxed as a [`Stream`] trait object.
    #[rustfmt::skip] // Single line where clause improves readability
    fn boxed<'a, F>(self, f: &F) -> Stream<I> where F: IntoIterator<Item = &'a Filter>;

    /// Constructs a new instance of [`Self`] from the provided byte [slice][1].
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    #[rustfmt::skip] // Single line where clause improves readability
    fn try_from_slice(src: &[u8]) -> Result<Self, Error> where Self: Sized;
}

/* ----------------------------------------------------------------- Reader Trait Implementation */

impl<'a, I> Reader<I> for &'a [u8]
where
    I: Deserialize + Evaluate,
{
    fn boxed<F>(mut self, filters: &'a F) -> Stream<I>
    where
        &'a F: IntoIterator<Item = &'a Filter>,
    {
        let iter = iter::from_fn(move || {
            let f = filters.into_iter();
            I::deserialize(&mut self)
                .map(|item| item.evaluate(f))
                .unwrap_or_else(Outcome::Error)
                .into()
        });
        Box::new(iter)
    }

    fn try_from_slice(src: &'a [u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(src)
    }
}

impl<'a> Reader<bool> for &'a BitSlice<u8, Lsb0> {
    fn boxed<F>(self, filters: &'a F) -> Stream<bool>
    where
        &'a F: IntoIterator<Item = &'a Filter>,
    {
        let iter = self.iter().by_vals().map(move |bit| {
            let f = filters.into_iter();
            bit.evaluate(f)
        });
        Box::new(iter)
    }

    fn try_from_slice(src: &'a [u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Self::try_from(src).or_else(|b| {
            Error::Truncated {
                expected: NonZeroUsize::MIN.get(),
                actual: b.len(),
            }
            .into()
        })
    }
}
        });
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

impl<'a, I> Reader<I> for OptBitVec<'a>
where
    I: Read<Src<'a> = Self>,
{
    type Ctx = &'a HashSet<Filter>;

    fn boxed(&self, ctx: Self::Ctx) -> Stream<I> {
        let mut bits = self.bits.boxed(ctx);
        let mut data = self.data.boxed(ctx);
        let iter = iter::from_fn(move || match bits.next()? {
            Outcome::Include(true) => data.next()?.map(Some).into(),
            Outcome::Include(false) => Outcome::Include(None).into(),
            other => other.into(),
        });
        Box::new(iter)
    }
}

impl<'a, I> Reader<I> for Seq<'a>
where
    I: Read<Src<'a> = Self>,
{
    type Ctx = &'a HashSet<Filter>;

    fn boxed(&self, ctx: Self::Ctx) -> Stream<I> {
        let mut offsets: Stream<u64> = self.offsets.boxed(ctx);
        let mut data = self.data.boxed(ctx);
        let mut start = u64::MIN;
        Box::new(iter)
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
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<u16> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<u32> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<u64> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<u128> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<i8> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<i16> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<i32> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<i64> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<i128> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<f32> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<f64> {
    type Src<'a> = OptBitVec<'a>;
}

impl Read for Option<bool> {
    type Src<'a> = OptBitVec<'a>;
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
    type Src<'a> = Seq<'a>;
}

impl<I> Read for Option<Vec<I>>
where
    I: Read,
{
    type Src<'a> = Seq<'a>;
}

impl Read for String {
    type Src<'a> = Seq<'a>;
}

impl Read for Option<String> {
    type Src<'a> = Seq<'a>;
}

impl Read for &str {
    type Src<'a> = Seq<'a>;
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {}
