/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! This module provides composable in-memory data accumulation primitives, each mapped to a
//! separate on-disk space optimisation strategy:
//!
//! - [`OptInSitu`] → Data buffer with [validity](Option) via niche.
//! - [`OptBitVec`] → Data buffer with [validity](Option) [mask](BitVec).
//! - [`Seq`] → Data buffer with [offset](NonZeroU64) metadata.
//! - [`OptSeq`] → [`Seq`] with [offset](NonZeroU64) and [validity](Option) metadata.
//! - [`Flatten`] → Collapses nested [`Option`] layers.
//!
//! Each accumulator type implements the [`Accumulate`] trait, which defines a shared interface for
//! handling in-memory value accumulation.

use std::num::*;
use std::ops::Sub;

use bitvec::field::BitField;
use bitvec::vec::BitVec;
use minicbor::{CborLen, Decode, Encode};

use crate::schema::number::Error;
use crate::schema::{self, size_of_opt, Unfold};
use crate::segment::Variant;

/// Shorthand type-erased stack-allocated [pointer](Box) to an [`Accumulate`] trait object backed by
/// a heap-allocated growable [`Buffer`](Serialize::Buffer).
// NOTE: Buffer must be a growable Vec; compiler cannot predict the number of accumulated items
// TODO → Impl Debug + Display + Clone (empty accumulator via Accumulate::boxed).
pub type BoxAcc<I> = Box<dyn Accumulate<Item = I, Buffer = Vec<u8>>>;

/// An **in-memory data accumulator** used to build data segments for the specified [`Schema`].
///
/// ### Segment Composition
///
/// Each [clem](crate) file is partitioned into self-describing segments which are immutable once
/// written. Each segment begins with a minimal header consisting of a [`variant`](Variant) ID and
/// [`length`](NonZeroU64).
///
/// - [`Schema`][1] segments describe the structure of encoded data.
/// - [`Data`][2] segments carry columnar buffers for a specified schema instance.
///
/// Each data segment is associated with a **single** schema segment. This association is primarily
/// included for data integrity and crash recovery; the optimised read path filters data segments by
/// schema using the `manifest`.
///
/// ```text
/// data-segment
/// ├─ header
/// │  ├─ variant: u8
/// │  ├─ length: NonZeroU64
/// │  ├─ schema: NonZeroU64
/// │  └─ count: NonZeroU64
/// ├─ buffer 0
/// ⋮
/// └─ buffer N
/// ```
///
/// The [`Schema`][1] maps each **platform-agnostic** primitive [`Type`][3] to a contiguous buffer;
/// providing essential context for buffer deserialization. Each `Accumulator` holds a [`Sector`][4]
/// for the corresponding schema which is written to disk within each data segment header. All
/// columns contain an equal number of rows indicated by `count` in the segment header.
///
/// Refer to the [schema module documentation](crate::schema) for more details.
///
/// [1]: schema::Schema
/// [2]: crate::Data
/// [3]: schema::Type
/// [4]: crate::Sector
pub struct Accumulator<I> {
    /// Type-erased [`Accumulate`] trait object.
    pub data: BoxAcc<I>,
    /// [Name](String) of the corresponding [`Schema`][1] registered in the [`Manifest`][2].
    ///
    /// [1]: crate::Schema
    /// [2]: crate::manifest::Manifest
    pub(crate) name: String,
    /// [`Sector`](crate::Sector) of the corresponding [`Schema`](crate::Schema) segment describing
    /// the structure of accumulated data.
    pub schema: crate::Sector,
}

impl<I> Accumulator<I> {
    /// Total length of the data segment header in bytes.
    ///
    /// Refer to the [`Accumulator`] documentation for more details regarding header layout.
    pub(crate) const HEADER: usize = size_of::<Variant>()
        + size_of::<NonZeroU64>()
        + size_of::<NonZeroU64>()
        + size_of::<NonZeroU64>();
}

/* --------------------------------------------------------------------------- Data Accumulators */

/// Data accumulator for [optional](Option) values with niche optimisation; a compiler optimisation
/// technique that leverages unused bit patterns (niches) to represent additional states without
/// increasing the [size](size_of) of the type.
///
/// ### Data Layout
///
/// [`OptInSitu`] encodes [`Some`] and [`None`] values directly in a single data buffer for
/// supported niche types; no validity mask is required.
///
/// [`OptBitVec`] provides a fallback implementation for non-niche types.
///
/// ### Guidance
///
/// Implementors are advised to use niche-optimised types when possible to improve storage
/// efficiency and random read performance.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub struct OptInSitu<T> {
    /// Contiguous payload encoding [`Some`] and [`None`] values directly.
    #[cbor(n(0), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub data: Vec<Option<T>>,
}

impl<T> Default for OptInSitu<T> {
    fn default() -> Self {
        // NOTE: cannot use generic static assertion; must be verified per monomorphisation.
        debug_assert_eq!(size_of::<T>(), size_of_opt::<T>(), "Use OptBitVec");
        Self { data: Vec::new() }
    }
}

/// Data accumulator for [optional](Option) values without niche optimisation.
///
/// ### Data Layout
///
/// [`OptBitVec`] encodes [validity](Option) and [value](T) separately for non-niche types:
///
/// 1. A packed [`BitVec`] encodes [`Some`] as `true`.
/// 2. A contiguous data buffer encodes values.
///
/// [`T::default`] generates placeholder values for [`None`] entries in the data buffer. This
/// design maintains the alignment necessary for **O(1) random access** by index.
///
/// ### Guidance
///
/// The sibling [`OptInSitu`] type encodes [`Some`] and [`None`] values directly in a single data
/// buffer for supported niche types; no validity mask required. Implementors are advised to use
/// niche-optimised types when possible to improve storage efficiency and random read performance.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct OptBitVec<T>
where
    T: Unfold + Default,
{
    /// Validity mask where `true → `[`Some`] and `false → `[`None`].
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BitVec::is_empty")
    )]
    pub mask: BitVec,
    /// Contiguous payload padded with [`Default::default`] for [`None`] entries.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

/// Data accumulator for [unsized][1] values.
///
/// ### Data Layout
///
/// It is not possible to predetermine the disk space required by each instance of an unsized type;
/// there is no guarantee that two [`Vec<T>`] contain the same number of elements. [Clem](crate)
/// therefore unfolds unsized types into:
///
/// 1. Columnar `offsets` bufffer describing boundaries.
/// 2. Contiguous `data` buffer encoding values.
///
/// This design ensures **O(1) random access** and avoids per-element pointer chasing. Sequential
/// scans across the contained [elements](T) remain linear; leveraging columnar optimisations for
/// SIMD and prefetch.
///
/// ```text
/// offsets: [3, 6, 6]
/// values:  [a, b, c, d, e, f, g, h]
/// ```
///
/// The serialized on-disk example above is deserialized into the memory representation below.
/// Implementers can specify which type to use for offset storage based on the number of expected
/// elements.
///
/// ```text
/// Row 0 → values[..3] → "abc"
/// Row 1 → values[3..6] → "def"
/// Row 2 → values[6..6] → "" (empty)
/// Row 3 → values[6..] → "gh"
/// ```
///
/// Nested unsized types use **multiple offset layers** alongside a **single data buffer**. This
/// composable design preserves the performance advantages associated with contiguous value storage;
/// namely predictable vectorised traversal. Scanning performance across the contiguous inner
/// `values` buffer is unaffected by deep nesting. The inner offsets buffer is aligned in memory
/// order of traversal to improve cache locality during nested iteration and reduce TLB misses.
///
/// ```text
/// inner offsets
/// outer offsets
/// values
/// ```
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct Seq<T>
where
    T: Unfold,
{
    /// Cumulative end offsets. `offsets[i]` marks the inclusive end of element `i` and the
    /// exclusive start of element `i + 1`.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    // TODO Allow users to specify the offset type based on the number of expected elements.
    pub offsets: Vec<NonZeroU64>,
    /// Flattened element buffer.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

/// Data accumulator for [optional](Option) [unsized][1] values.
///
/// ### Data Layout
///
/// It is not possible to predetermine the disk space required by each instance of an unsized type;
/// there is no guarantee that two [`Vec<T>`] contain the same number of elements. [`Clem`](crate)
/// therefore unfolds unsized types into:
///
/// 1. Columnar `offsets` bufffer describing boundaries.
/// 2. Contiguous `data` buffer encoding values.
///
/// [`OptSeq`] leverages niche-optimisation on the `offsets` buffer to simultaneously encode
/// validity without requiring an auxiliary bitmap. `None` rows append no data.
///
/// See the [documentation](Seq) on non-optional unsized type accumulation for more details.
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct OptSeq<T>
where
    T: Unfold,
{
    /// Cumulative end offsets per row; [`None`] marks a null row (no data appended).
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub offsets: Vec<Option<NonZeroU64>>,
    /// Flattened element buffer; only [`Some`] rows contribute entries.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

/// Stateless type-level wrapper that flattens nested types on [`push`](Accumulate::push). All
/// storage lives in the inner accumulator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[doc(hidden)]
pub struct Flatten<T>(#[n(0)] pub T);

/* ----------------------------------------------------------------- Accumulate Trait Definition */

/// An in-memory **data accumulator** that ingests values of the specified [`type`](Self::Item) and
/// encodes into an optimised on-disk format.
pub trait Accumulate: Serialize {
    /// The input type accepted by [`Self::push`].
    type Item;

    /// Returns a new empty instance of [`Self`] boxed as a [`BoxAcc`] trait object.
    // NOTE: Buffer must be a growable Vec; compiler cannot predict the number of accumulated items
    fn boxed() -> BoxAcc<Self::Item>
    where
        Self: Default + Serialize<Buffer = Vec<u8>> + 'static,
    {
        Box::new(Self::default())
    }

    /// Append an [`Item`](Self::Item) to the [accumulator](Self)
    fn push(&mut self, value: Self::Item);

    /// Reinitialise the [accumulator](Self) without writing to disk. All data is permanently lost.
    ///
    /// Note that this method may not affect the allocated capacity of the underlying storage.
    fn discard(&mut self);

    /// Returns `true` if the [accumulator](Self) contains no data.
    fn is_empty(&self) -> bool;

    /// Returns the number of accumulated rows.
    fn count(&self) -> u64;

    /// Returns the minimum accumulated value, or [`None`] if the [`Item`](Self::Item) is not
    /// meaningfully [orderable](PartialOrd).
    fn min(&self) -> Option<Self::Item> {
        None
    }

    /// Returns the maximum accumulated value, or [`None`] if the [`Item`](Self::Item) is not
    /// meaningfully [orderable](PartialOrd).
    fn max(&self) -> Option<Self::Item> {
        None
    }
}

/* ------------------------------------------------------------- Accumulate Trait Implementation */

impl<I> Accumulate for Accumulator<I> {
    type Item = I;

    fn push(&mut self, value: Self::Item) {
        self.data.push(value);
    }

    fn discard(&mut self) {
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.data.count()
    }
}

impl Accumulate for BitVec {
    type Item = bool;

    fn push(&mut self, value: Self::Item) {
        BitVec::push(self, value);
    }

    fn discard(&mut self) {
        BitVec::clear(self);
    }

    fn is_empty(&self) -> bool {
        BitVec::is_empty(self)
    }

    fn count(&self) -> u64 {
        BitVec::len(self) as u64
    }
}

impl<T: Serialize> Accumulate for Vec<T> {
    type Item = T;

    fn push(&mut self, value: Self::Item) {
        Vec::push(self, value);
    }

    fn discard(&mut self) {
        Vec::clear(self);
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn count(&self) -> u64 {
        Vec::len(self) as u64
    }
}

impl<T> Accumulate for OptInSitu<T>
where
    Option<T>: Serialize,
{
    type Item = Option<T>;

    fn push(&mut self, value: Self::Item) {
        self.data.push(value);
    }

    fn discard(&mut self) {
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.data.count()
    }
}

impl<T> Accumulate for OptBitVec<T>
where
    T: Unfold + Default,
{
    type Item = Option<T>;

    fn push(&mut self, value: Self::Item) {
        self.mask.push(value.is_some());
        self.data.push(value.unwrap_or_default());
    }

    fn discard(&mut self) {
        self.mask.discard();
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.mask.is_empty() && self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.mask.len() as u64
    }
}

impl<T> Accumulate for Seq<T>
where
    T: Unfold,
{
    type Item = Vec<T>;

    fn push(&mut self, value: Self::Item) {
        // 1. Calculate offset
        let prev = self.offsets.last().copied().unwrap_or(NonZeroU64::MIN);
        let next = prev.saturating_add(value.len() as u64);
        // 2. Push to buffers
        value.into_iter().for_each(|x| self.data.push(x));
        self.offsets.push(next);
    }

    fn discard(&mut self) {
        self.offsets.discard();
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.offsets.is_empty() && self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.offsets.len() as u64
    }
}

impl<T> Accumulate for OptSeq<T>
where
    T: Unfold,
{
    type Item = Option<Vec<T>>;

    fn push(&mut self, value: Self::Item) {
        let prev = self.offsets.last().copied().flatten().unwrap_or(NonZeroU64::MIN);
        match value {
            Some(value) => {
                let next = prev.saturating_add(value.len() as u64);
                value.into_iter().for_each(|x| self.data.push(x));
                self.offsets.push(Some(next));
            }
            None => self.offsets.push(None),
        }
    }

    fn discard(&mut self) {
        self.offsets.discard();
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.offsets.is_empty() && self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.offsets.len() as u64
    }
}

impl<A, B> Accumulate for Flatten<A>
where
    A: Accumulate<Item = Option<B>>,
{
    type Item = Option<Option<B>>;

    fn push(&mut self, value: Self::Item) {
        self.0.push(value.flatten());
    }

    fn discard(&mut self) {
        self.0.discard();
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn count(&self) -> u64 {
        self.0.count()
    }
}

impl<A, B> From<A> for Flatten<A>
where
    A: Accumulate<Item = Option<B>>,
{
    fn from(value: A) -> Self {
        Self(value)
    }
}

/* ------------------------------------------------------------------ Serialize Trait Definition */

/// A **buffer** that can hold the serialized byte representation of a value.
pub trait Buffer: AsRef<[u8]> + AsMut<[u8]> {
    /// [`Serialize`] the provided [`item`](I) and append into [`self`](Buffer).
    fn serialize_push<I>(self, item: &I) -> Result<Self, Error>
    where
        I: Serialize<Buffer = Self>,
        Self: Sized,
    {
        item.serialize_into(self)
    }
}

/// Blanket implementation that covers stack-allocated byte arrays and heap-allocated byte vectors.
///
/// This design defines a fixed-size buffer for types with a known size at compile time, while
/// facilitating dynamic buffer sizing for types that require heap allocation.
impl<T> Buffer for T where T: AsRef<[u8]> + AsMut<[u8]> {}

/// A **type** that can be serialized into a canonical [`clem`](crate) binary representation for
/// on-disk storage.
#[doc(hidden)]
pub trait Serialize {
    /// The [`Buffer`] type returned by [`Self::serialize`].
    ///
    /// Fixed-size types can specify an appropriate array to leverage stack allocation. Unsized
    /// types should specify a heap-allocated buffer to accommodate dynamic sizing at runtime.
    type Buffer: Buffer;

    /// Returns the number of bytes required to encode `self`.
    ///
    /// Defaults to the [`size_of`](size_of)`::<Self>` if not otherwise specified.
    fn size(&self) -> Result<NonZeroU64, Error>;

    /// Serialize `self` into the provided [`Buffer`].
    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error>;

    /// Serialize `self` and return the encoded bytes in a new [`Buffer`].
    fn serialize(&self) -> Result<Self::Buffer, Error>;

    /// Serialize `self` into the provided [`Vec<u8>`] sink.
    ///
    /// This function provides a unified interface for serializing fixed-size (stack) and
    /// dynamic-size (heap) types into a growable [`Buffer`].
    fn extend(&self, mut sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        let bytes = self.serialize()?;
        sink.extend_from_slice(bytes.as_ref());
        Ok(sink)
    }
}

/* -------------------------------------------------------------- Serialize Trait Implementation */

impl Serialize for char {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        static_assertions::assert_eq_size!(char, u32);
        u32::from(*self).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        u32::from(*self).serialize()
    }
}

impl Serialize for u8 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf[0] = *self;
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok([*self])
    }
}

impl Serialize for u16 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for u32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for u64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for u128 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for NonZeroU8 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroU16 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroU32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroU64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroU128 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for i8 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i16 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i128 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for NonZeroI8 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroI16 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroI32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroI64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for NonZeroI128 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.get().size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for f32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for f64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        buf = self.to_le_bytes();
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for Option<char> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes u32::MAX (outside the valid scalar range).
        self.map_or(u32::MAX, u32::from).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(u32::MAX, u32::from).serialize()
    }
}

impl Serialize for Option<NonZeroU8> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes u8::MIN (outside the valid non-zero range).
        self.map_or(u8::MIN, NonZeroU8::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(u8::MIN, NonZeroU8::get).serialize()
    }
}

impl Serialize for Option<NonZeroU16> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes u16::MIN (outside the valid non-zero range).
        self.map_or(u16::MIN, NonZeroU16::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(u16::MIN, NonZeroU16::get).serialize()
    }
}

impl Serialize for Option<NonZeroU32> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes u32::MIN (outside the valid non-zero range).
        self.map_or(u32::MIN, NonZeroU32::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(u32::MIN, NonZeroU32::get).serialize()
    }
}

impl Serialize for Option<NonZeroU64> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes u64::MIN (outside the valid non-zero range).
        self.map_or(u64::MIN, NonZeroU64::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(u64::MIN, NonZeroU64::get).serialize()
    }
}

impl Serialize for Option<NonZeroU128> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes u128::MIN (outside the valid non-zero range).
        self.map_or(u128::MIN, NonZeroU128::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(u128::MIN, NonZeroU128::get).serialize()
    }
}

impl Serialize for Option<NonZeroI8> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes 0i8 (outside the valid non-zero range).
        self.map_or(0i8, NonZeroI8::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(0i8, NonZeroI8::get).serialize()
    }
}

impl Serialize for Option<NonZeroI16> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes 0i16 (outside the valid non-zero range).
        self.map_or(0i16, NonZeroI16::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(0i16, NonZeroI16::get).serialize()
    }
}

impl Serialize for Option<NonZeroI32> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes 0i32 (outside the valid non-zero range).
        self.map_or(0i32, NonZeroI32::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(0i32, NonZeroI32::get).serialize()
    }
}

impl Serialize for Option<NonZeroI64> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes 0i64 (outside the valid non-zero range).
        self.map_or(0i64, NonZeroI64::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(0i64, NonZeroI64::get).serialize()
    }
}

impl Serialize for Option<NonZeroI128> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = size_of::<Self>().try_into()?;
        size.try_into().map_err(Error::from)
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // None writes 0i128 (outside the valid non-zero range).
        self.map_or(0i128, NonZeroI128::get).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.map_or(0i128, NonZeroI128::get).serialize()
    }
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let prefix = size_of::<NonZeroU64>().try_into()?;
        // Recursively sum the sizes of all elements.
        self.iter()
            .try_fold(u64::MIN, |total, element| {
                let size = element.size()?.get();
                total.checked_add(size).ok_or(Error::Zero)
            })?
            .checked_add(prefix) // Add length prefix
            .and_then(NonZeroU64::new)
            .ok_or(Error::Zero)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let size = self.size()?.get().sub(8).to_le_bytes();
        buf.extend_from_slice(&size);
        self.iter().try_fold(buf, |sink, element| element.extend(sink))
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = Vec::with_capacity(size).serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.serialize_into(sink)
    }
}

impl Serialize for BitVec {
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        // Each byte encodes 8 bits; round up (↑) BitVec::len to the nearest whole byte.
        let payload: u64 = self.len().div_ceil(8).try_into()?;
        // 8 byte NonZeroU64 length prefix
        payload.checked_add(8).and_then(NonZeroU64::new).ok_or(Error::Zero)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // NOTE: Self::size returns Error if BitVec::len overflows u64 (not expected in production)
        let size = self.size()?.get().sub(8).to_le_bytes();
        buf.extend_from_slice(&size);
        // Intermediate chunks contain 8 bits in Lsb0 order; the final chunk may contain ≤ 8 bits.
        // BitVec::load_le packs each chunk into one u8 in LE order, padding with zeros if the final
        // chunk is shorter than 8 bits. The resulting bytes are pushed into the provided buffer.
        buf.reserve(self.len().div_ceil(8));
        self.chunks(8).for_each(|bits| buf.push(bits.load_le::<u8>()));
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = Vec::with_capacity(size).serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.serialize_into(sink)
    }
}

impl<T> Serialize for OptInSitu<T>
where
    Vec<Option<T>>: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.data.size()
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.data.serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.data.serialize()
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.data.extend(sink)
    }
}

impl<T> Serialize for OptBitVec<T>
where
    T: Unfold + Default,
    T::RawAcc: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let mask = self.mask.size()?;
        let data = self.data.size()?.get();
        // 8 byte NonZeroU64 length prefix
        mask.checked_add(data).ok_or(Error::Zero)?.checked_add(8).ok_or(Error::Zero)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let size = self.size()?.get().sub(8).to_le_bytes();
        buf.extend_from_slice(&size);
        buf.serialize_push(&self.mask)?.serialize_push(&self.data)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = Vec::with_capacity(size).serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.serialize_into(sink)
    }
}

impl<T> Serialize for Seq<T>
where
    T: Unfold,
    T::RawAcc: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let offsets = self.offsets.size()?;
        let data = self.data.size()?.get();
        // 8 byte NonZeroU64 length prefix
        offsets.checked_add(data).ok_or(Error::Zero)?.checked_add(8).ok_or(Error::Zero)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let size = self.size()?.get().sub(8).to_le_bytes();
        buf.extend_from_slice(&size);
        buf.serialize_push(&self.offsets)?.serialize_push(&self.data)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = Vec::with_capacity(size).serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.serialize_into(sink)
    }
}

impl<T> Serialize for OptSeq<T>
where
    T: Unfold,
    T::RawAcc: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let offsets = self.offsets.size()?;
        let data = self.data.size()?.get();
        // 8 byte NonZeroU64 length prefix
        offsets.checked_add(data).ok_or(Error::Zero)?.checked_add(8).ok_or(Error::Zero)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let size = self.size()?.get().sub(8).to_le_bytes();
        buf.extend_from_slice(&size);
        buf.serialize_push(&self.offsets)?.serialize_push(&self.data)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = Vec::with_capacity(size).serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.serialize_into(sink)
    }
}

impl<T> Serialize for Flatten<T>
where
    T: Serialize,
{
    type Buffer = T::Buffer;

    fn size(&self) -> Result<NonZeroU64, Error> {
        self.0.size() // Transparent wrapper
    }

    fn serialize_into(&self, buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        self.0.serialize_into(buf) // Transparent wrapper
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.0.serialize() // Transparent wrapper
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.0.extend(sink) // Transparent wrapper
    }
}

impl<I> Serialize for Accumulator<I> {
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let header = Self::HEADER.try_into()?;
        self.data.size()?.get().checked_add(header).and_then(NonZeroU64::new).ok_or(Error::Zero)
    }

    fn serialize_into(&self, mut buf: Self::Buffer) -> Result<Self::Buffer, Error> {
        // 1. Header fields (see accumulator documentation)
        buf.push(Variant::Data as u8);
        buf.extend_from_slice(&self.data.size()?.get().to_le_bytes());
        buf.extend_from_slice(&self.schema.offset.to_le_bytes());
        buf.extend_from_slice(&self.data.count().to_le_bytes());
        // 2. Columnar data buffers
        self.data.serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = Vec::with_capacity(size).serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {}
