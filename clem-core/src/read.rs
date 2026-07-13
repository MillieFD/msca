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
//! [clem](crate) maximises IO performance by storing on-disk data as columnar [buffers][1]
//! optimised for range-based queries across an arbitrary number of dimensions; however, this
//! underlying format is generally unsuitable for direct manipulation by end-users.
//!
//! This module provides an [iterator-based](Iterator) interface to coordinate the transition from
//! raw binary data into supported rust types; corresponding to **phase 3** of the [read-cycle][2].
//!
//! ### Segment Composition
//!
//! Each [`Dataset`][3] is partitioned into self-describing segments which are immutable once
//! written. Each segment begins with a minimal header consisting of a [`variant`][4] identifier and
//! [`next`](num::NonZeroU64) offset.
//!
//! - [`Schema`][5] segments describe the structure of encoded data.
//! - [`Data`][6] segments carry columnar [buffers][1] for a specified schema.
//!
//! Multimodality and schema evolution are realised by appending additional schema segments. Data
//! storage and file extensibility are realised by appending additional data segments. Format
//! extensibility may be achieved via the introduction of new segment variants in future releases.
//!
//! ### Lazy Zero-Copy Reads
//!
//! Each column is packaged into a lazy zero-copy stream that:
//!
//! 1. Pulls bytes from the retained on-disk buffers.
//! 2. [Deserializes](Deserialize) bytes into the requested type **exactly once**.
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
//! [1]: crate::manifest::Buffer
//! [2]: crate::io
//! [3]: crate::dataset::Dataset
//! [4]: crate::segment::Variant
//! [5]: crate::schema::Schema
//! [6]: crate::Data

use std::{iter, num};

use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;

use crate::io::{Deserialize, Deserializer, Error, SizedBuf};
use crate::query;

/* ------------------------------------------------------------------------------ Public Exports */

/// Shorthand type-erased stack-allocated [pointer](Box) to a lazy [`Iterator`] yielding one
/// [`Outcome`] per [`Item`](I), or [`None`] once every candidate [`Buffer`][1] is consumed.
///
/// [1]: crate::manifest::Buffer
pub type Stream<'a, I> = Box<dyn Iterator<Item = Outcome<I>> + 'a>;

/// A **stateful cursor** over paired validity and value sub-buffers for a single [`Column`]; used
/// to [`Deserialize`] optional non-niche items.
#[doc(hidden)] // Reachable via Read::Src for optional non-niche readers
pub struct OptBitVec<'a, I>
where
    I: Read,
{
    /// Byte [slice][1] over the validity sub-buffer where `true` → [`Some`] and `false` → [`None`].
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    mask: &'a BitSlice<u8, Lsb0>,
    /// A **stateful reader** over the concatenated data sub-buffer from which [`Some`] items are
    /// [deserialized](Deserialize).
    data: I::Src<'a>,
}

impl<'de, I> Deserialize<'de> for OptBitVec<'de, I>
where
    I: Read,
    I::Src<'de>: Deserialize<'de, Ok = I::Src<'de>> + Default,
{
    type Ok = Self;

    fn deserialize(src: &mut &'de [u8]) -> Result<Self, Error> {
        let mask = SizedBuf::deserialize(src)?.deserialize_into()?;
        let data = match src.is_empty() {
            true => I::Src::default(),
            false => SizedBuf::deserialize(src)?.deserialize_into()?,
        };
        Ok(Self { mask, data })
    }
}

/// A **stateful cursor** over paired offset and value sub-buffers for a single [`Column`]; used to
/// [`Deserialize`](Deserialize) [unsized][1] items.
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[doc(hidden)] // Reachable via Read::Src for unsized readers
pub struct Seq<'a> {
    /// Byte [slice][1] over the `ends` sub-buffer yielding one `u64` cumulative end offset for each
    /// [`Some`] or [`u64::MAX`] for [`None`].
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    ends: &'a [u8],
    /// Concatenated data sub-buffer from which [`Some`] items are [deserialized](Deserialize).
    data: &'a [u8],
}

impl<'de> Deserialize<'de> for Seq<'de> {
    type Ok = Self;

    fn deserialize(src: &mut &'de [u8]) -> Result<Self, Error> {
        let ends = SizedBuf::deserialize(src)?.deserialize_into()?;
        let data = match src.is_empty() {
            true => <&[u8]>::default(),
            false => SizedBuf::deserialize(src)?.deserialize_into()?,
        };
        Ok(Self { ends, data })
    }
}

/// A **deserialisation primitive** for [niche][1] [optional](Option) items; a compiler optimisation
/// technique that leverages unused bit patterns (niches) to represent additional states without
/// increasing the [size](Sized) of the type.
///
/// ### Data Layout
///
/// The corresponding [`OptInSitu`][2] accumulator inlines [`Some`] and [`None`] items directly into
/// a single data buffer for supported niche types; no validity mask is required. The alternative
/// [`OptBitVec`][3] accumulator provides a fallback implementation for non-niche types.
///
/// ### Guidance
///
/// Implementers are advised to use niche-optimised types when possible to improve storage density
/// and random access read performance.
///
/// [1]: https://doc.rust-lang.org/std/option/index.html#representation
/// [2]: crate::accumulate::OptInSitu
/// [3]: crate::accumulate::OptBitVec
#[doc(hidden)] // Reachable via Read::Src for optional niche readers
pub struct OptInSitu<'a>(&'a [u8]);

impl<'de> Deserialize<'de> for OptInSitu<'de> {
    type Ok = Self;

    fn deserialize(src: &mut &'de [u8]) -> Result<Self, Error> {
        // NOTE: consumes the whole source; a buffer sector must frame the slice externally
        <&[u8]>::deserialize(src).map(OptInSitu)
    }
}

/* ------------------------------------------------------------------------- Read Stream Outcome */

/// The result of [deserializing](Deserialize) one [`Item`](I) from a [`Read`](Read) [`Stream`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub enum Outcome<I> {
    /// A [deserialized](Deserialize::deserialize) item that satisfies every column filter.
    Include(I),
    /// A [deserialized](Deserialize::deserialize) item that was rejected by one or more filter.
    Exclude(I),
    /// An [`Error`] occurred during [deserialization](Deserialize).
    Error(Error),
}

impl<I> Outcome<I> {
    /// Converts [`Outcome`](Outcome)`<`[`I`](I)`>` into [`Outcome`](Outcome)`<`[`O`](O)`>` by applying
    /// the specified [closure](F) to the contained item.
    fn map<F, O>(self, f: F) -> Outcome<O>
    where
        F: FnOnce(I) -> O,
    {
        match self {
            Self::Include(i) => Outcome::Include(f(i)),
            Self::Exclude(i) => Outcome::Exclude(f(i)),
            Self::Error(e) => Outcome::Error(e),
        }
    }

    /// Construct a [`Stream`] that yields [`self`](Outcome) exactly [once](iter::once).
    fn once<'a>(self) -> Stream<'a, I>
    where
        I: 'a,
    {
        iter::once(self).into_box()
    }

    /// Construct a [`Stream`] that yields [`self`](Outcome) exactly `n` times; [`Include`][1] and
    /// [`Exclude`][2] clone the inner item while [`Error`][3] yields [`once`](Self::once).
    ///
    /// [1]: Outcome::Include
    /// [2]: Outcome::Exclude
    /// [3]: Outcome::Error
    fn repeat<'a>(self, n: usize) -> Stream<'a, I>
    where
        I: Clone + 'a,
    {
        match self {
            Self::Include(item) => iter::repeat_n(item, n).map(Outcome::Include).into_box(),
            Self::Exclude(item) => iter::repeat_n(item, n).map(Outcome::Exclude).into_box(),
            Self::Error(error) => Self::Error(error).once(),
        }
    }

    /// Convert an included outcome into an excluded outcome without changing the inner [`item`](I).
    ///
    /// - [`Include`](Outcome::Include) converted to [`Exclude`](Outcome::Exclude)
    /// - [`Exclude`](Outcome::Exclude) and [`Error`](Outcome::Error) remain unchanged
    ///
    /// The resulting [`Outcome`] is guaranteed to never contain [`Outcome::Include`].
    fn exclude(self) -> Self {
        match self {
            Outcome::Include(i) => Outcome::Exclude(i),
            other => other,
        }
    }
}

impl<I> From<Error> for Outcome<I> {
    fn from(e: Error) -> Self {
        Outcome::Error(e)
    }
}

/* --------------------------------------------------------------------- Reader Trait Definition */

/// A **stateful data source** used to construct a lazy [`Stream`].
#[doc(hidden)] // pub required for Query::column trait bounds; not intended as a stable API
pub trait Reader<'a, I> {
    /// Returns a new boxed [`Stream`] trait object **without** any [filters](Filter).
    ///
    /// The resulting [`Stream`] will never return [`Outcome::Exclude`] but [`Outcome::Error`]
    /// remains possible.
    fn boxed(self) -> Stream<'a, I>
    where
        Self: Sized,
    {
        self.with_filters(&[])
    }

    /// Returns a new boxed [`Stream`] trait object that lazily [evaluates](Evaluate) each item
    /// during [deserialization](Deserialize).
    ///
    /// Simple implementations use each borrowed [`Filter`] directly with zero allocation. Complex
    /// composite readers may [`Clone`] relevant [filters](F) into one or more owned collections
    /// which are then re-borrowed by relevant sub-readers.
    fn with_filters<'f, F>(self, filters: &'f F) -> Stream<'a, I>
    where
        Self: Sized,
        'f: 'a,
        &'f F: IntoIterator<Item = &'f Filter>;
}

/* ----------------------------------------------------------------- Reader Trait Implementation */

impl<'a, I> Reader<'a, I> for &'a [u8]
where
    I: for<'de> Deserialize<'de, Ok = I> + Evaluate,
{
    fn with_filters<'f, F>(mut self, filters: &'f F) -> Stream<'a, I>
    where
        'f: 'a,
        &'f F: IntoIterator<Item = &'f Filter>,
    {
        iter::from_fn(move || {
            let f = filters.into_iter();
            I::deserialize(&mut self)
                .map(|item| item.evaluate(f))
                .unwrap_or_else(Outcome::Error)
                .into()
        })
        .into_box()
    }
}

impl<'a> Reader<'a, bool> for &'a BitSlice<u8, Lsb0> {
    fn with_filters<'f, F>(self, filters: &'f F) -> Stream<'a, bool>
    where
        'f: 'a,
        &'f F: IntoIterator<Item = &'f Filter>,
    {
        self.iter()
            .by_vals()
            .map(move |bit| {
                let f = filters.into_iter();
                bit.evaluate(f)
            })
            .into_box()
    }
}

impl<'a, I> Reader<'a, Option<I>> for OptBitVec<'a, I>
where
    I: Read + Evaluate + 'a,
    I::Src<'a>: Reader<'a, I>,
{
    fn with_filters<'f, F>(self, filters: &'f F) -> Stream<'a, Option<I>>
    where
        'f: 'a,
        &'f F: IntoIterator<Item = &'f Filter>,
    {
        // Validity is resolved against the mask: `is_some` drops `None` rows and `is_none` drops
        // every present row. Only value predicates reach the value reader, which cannot assess an
        // always-present value and rightly rejects the `is_some` / `is_none` markers.
        let some = filters.into_iter().any(|f| matches!(f, Filter::IsSome));
        let none = filters.into_iter().any(|f| matches!(f, Filter::IsNone));
        let mut mask = self.mask.boxed();
        let mut data = self.data.boxed();
        // Assess one present value against the value predicates, wrapping the survivor in [`Some`].
        let present = move |value: Outcome<I>| {
            let assessed = match value {
                Outcome::Include(v) | Outcome::Exclude(v) => {
                    let values = filters.into_iter().filter(|f| !f.validity());
                    v.evaluate(values)
                }
                Outcome::Error(e) => return Outcome::Error(e),
            };
            match assessed {
                Outcome::Include(v) if none => Outcome::Exclude(Some(v)),
                other => other.map(Some),
            }
        };
        iter::from_fn(move || {
            Some(match mask.next()? {
                Outcome::Include(true) | Outcome::Exclude(true) => present(data.next()?),
                Outcome::Include(false) | Outcome::Exclude(false) if some => Outcome::Exclude(None),
                Outcome::Include(false) | Outcome::Exclude(false) => Outcome::Include(None),
                Outcome::Error(e) => Outcome::Error(e),
            })
        })
        .into_box()
    }
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
    type Src<'a>;
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
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroU8> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroU16> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroU32> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroU64> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroU128> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroI8> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroI16> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroI32> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroI64> {
    type Src<'a> = OptInSitu<'a>;
}

impl Read for Option<num::NonZeroI128> {
    type Src<'a> = OptInSitu<'a>;
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

/* ------------------------------------------------------------------- Evaluate Trait Definition */

/// A **deserialized item** that can be tested against the provided filter.
///
/// This trait is used to subtractively reduce the [`query`] results set.
#[doc(hidden)] // pub required for filter trait bounds; not intended as a stable API
pub trait Evaluate<I = Self> {
    /// Apply the specified [`filter`](F) to the item operand and wrap in [`Outcome`].
    #[rustfmt::skip] // Single line where clause improves readability
    fn evaluate<F>(self, filter: F) -> Outcome<Self> where F: Fn(&I) -> bool, Self: Sized;

    /// Returns `true` if `self` is [`Some`].
    fn some(&self) -> bool;
}

/* --------------------------------------------------------------- Evaluate Trait Implementation */

impl<I> Evaluate for I
where
    I: Unfold<RawAcc = Vec<Self>>,
{
    fn evaluate<F>(self, filter: F) -> Outcome<Self>
    where
        F: Fn(&Self) -> bool,
        Self: Sized,
    {
        match filter(&self) {
            true => Outcome::Include(self),
            false => Outcome::Exclude(self),
        }
    }

    fn some(&self) -> bool {
        true
    }
}

impl Evaluate for bool {
    fn evaluate<F>(self, filter: F) -> Outcome<Self>
    where
        F: Fn(&Self) -> bool,
    {
        match filter(&self) {
            true => Outcome::Include(self),
            false => Outcome::Exclude(self),
        }
    }

    fn some(&self) -> bool {
        true
    }
}

impl<I> Evaluate<I> for Option<I>
where
    I: Unfold + Evaluate,
{
    fn evaluate<F>(self, filter: F) -> Outcome<Self>
    where
        F: Fn(&I) -> bool,
    {
        match self {
            None => Outcome::Include(None),
            Some(item) => item.evaluate(filter).map(Some),
        }
    }

    fn some(&self) -> bool {
        self.is_some()
    }
}

impl Evaluate for String {
    fn evaluate<F>(self, filter: F) -> Outcome<Self>
    where
        F: Fn(&Self) -> bool,
    {
        match filter(&self) {
            true => Outcome::Include(self),
            false => Outcome::Exclude(self),
        }
    }

    fn some(&self) -> bool {
        true
    }
}

impl Evaluate for &str {
    fn evaluate<F>(self, filter: F) -> Outcome<Self>
    where
        F: Fn(&Self) -> bool,
    {
        match filter(&self) {
            true => Outcome::Include(self),
            false => Outcome::Exclude(self),
        }
    }

    fn some(&self) -> bool {
        true
    }
}

impl<I> Evaluate for Vec<I> {
    fn evaluate<F>(self, filter: F) -> Outcome<Self>
    where
        F: Fn(&Self) -> bool,
    {
        match filter(&self) {
            true => Outcome::Include(self),
            false => Outcome::Exclude(self),
        }
    }

    fn some(&self) -> bool {
        true
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    /// Append a length-prefixed, 64-bit-aligned sized region to `out`, mirroring the on-disk
    /// [`SizedBuf`](crate::io) layout independently of the production serializer.
    fn sized(payload: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        out.extend_from_slice(payload);
        let pad = (8 - (payload.len() & 7)) & 7;
        out.resize(out.len() + pad, 0);
    }

    /// [`Seq::deserialize`] splits the composite body into its raw `ends` and `data` sub-buffers.
    #[test]
    fn seq_deserialize_splits_ends_and_data() {
        let mut buf = Vec::new();
        sized(&3u64.to_le_bytes(), &mut buf); // one cumulative end offset
        sized(b"abc", &mut buf);
        let mut src = buf.as_slice();
        let seq = Seq::deserialize(&mut src).expect("Deserialize failed");
        assert_eq!(seq.ends, &3u64.to_le_bytes());
        assert_eq!(seq.data, b"abc");
    }

    /// [`OptBitVec::deserialize`] splits the composite body into its validity `mask` and value
    /// `data` sub-buffers; the mask marks rows 0 and 2 as present.
    #[test]
    fn opt_bit_vec_deserialize_splits_mask_and_data() {
        let mut buf = Vec::new();
        sized(&[0b0000_0101], &mut buf);
        let data = [1u32.to_le_bytes(), 3u32.to_le_bytes()].concat();
        sized(&data, &mut buf);
        let mut src = buf.as_slice();
        let opt = OptBitVec::<u32>::deserialize(&mut src).expect("Deserialize failed");
        assert!(opt.mask[0] && !opt.mask[1] && opt.mask[2]);
        assert_eq!(opt.data, data.as_slice());
    }

    /// [`OptBitVec::deserialize`] accepts the omitted value sub-buffer written by an all-[`None`]
    /// column; the exhausted source yields an empty data cursor.
    #[test]
    fn opt_bit_vec_deserialize_omitted_data() {
        let mut buf = Vec::new();
        sized(&[0b0000_0000], &mut buf);
        let mut src = buf.as_slice();
        let opt = OptBitVec::<u32>::deserialize(&mut src).expect("Deserialize failed");
        assert!(!opt.mask[0]);
        assert!(opt.data.is_empty());
    }

    /// [`Outcome::repeat`] yields the cloned item exactly `n` times; excluded values repeat as
    /// [`Outcome::Exclude`] to keep composite readers in lockstep.
    #[test]
    fn outcome_repeat_clones() {
        let items: Vec<u32> = Outcome::Include(7u32)
            .repeat(3)
            .map(|out| out.result().expect("Repeat yielded an error"))
            .collect();
        assert_eq!(items, [7, 7, 7]);
        let excluded: Vec<Outcome<u32>> = Outcome::Exclude(7u32).repeat(2).collect();
        assert_eq!(excluded.len(), 2);
        assert!(excluded.iter().all(|out| matches!(out, Outcome::Exclude(7))));
    }
}
