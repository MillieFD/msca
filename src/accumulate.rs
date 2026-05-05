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

use crate::schema::Unfold;
use bitvec::vec::BitVec;
use minicbor::{CborLen, Decode, Encode};
use std::num::{self, NonZeroU64};

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
        debug_assert_eq!(size_of::<Option<T>>(), size_of::<T>(), "Use OptBitVec");
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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct OptBitVec<T: Unfold + Default> {
    /// Validity mask where `true → `[`Some`] and `false → `[`None`].
    #[cbor(n(0), skip_if = "BitVec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BitVec::is_empty")
    )]
    pub mask: BitVec,
    /// Contiguous payload padded with [`Default::default`] for [`None`] entries.
    #[cbor(n(1), skip_if = "Accumulate::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

impl<T: Unfold + Default> Default for OptBitVec<T> {
    fn default() -> Self {
        debug_assert!(size_of::<Option<T>>() > size_of::<T>(), "Use OptInSitu");
        Self {
            mask: BitVec::new(),
            data: T::RawAcc::default(),
        }
    }
}

/// Data accumulator for [unsized][1] values.
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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Seq<T: Unfold> {
    /// Cumulative end offsets. `offsets[i]` marks the inclusive end of element `i` and the
    /// exclusive start of element `i + 1`.
    #[cbor(n(0), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    // TODO Allow users to specify the offset type based on the number of expected elements.
    pub offsets: Vec<NonZeroU64>,
    /// Flattened element buffer.
    #[cbor(n(1), skip_if = "Accumulate::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

impl<T: Unfold> Default for Seq<T> {
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            data: T::RawAcc::default(),
        }
    }
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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct OptSeq<T: Unfold> {
    /// Cumulative end offsets per row; [`None`] marks a null row (no data appended).
    #[cbor(n(0), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub offsets: Vec<Option<NonZeroU64>>,
    /// Flattened element buffer; only [`Some`] rows contribute entries.
    #[cbor(n(1), skip_if = "Accumulate::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

impl<T: Unfold> Default for OptSeq<T> {
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            data: T::RawAcc::default(),
        }
    }
}

/// Stateless type-level wrapper that flattens nested types on [`push`](Accumulate::push). All
/// storage lives in the inner accumulator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Flatten<T>(#[n(0)] pub T);

/* ----------------------------------------------------------------- Accumulate Trait Definition */

/// An in-memory **data accumulator** that can ingest values of the specified [`type`](Self::Item)
/// and encode into an optimised on-disk format.
pub trait Accumulate: Default {
    /// The input type accepted by [`Self::push`].
    type Item;

    /// Append a single value of `T` to [`Self`].
    fn push(&mut self, value: Self::Item);

    /// Reinitialise [`Self`] without writing to disk. All accumulated data is permanently lost.
    ///
    /// Note that this method may not affect the allocated capacity of the underlying storage.
    fn discard(&mut self);

    /// Returns `true` if the accumulator contains no values.
    fn is_empty(&self) -> bool;
}

/* ------------------------------------------------------------- Accumulate Trait Implementation */

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
}

impl<T> Accumulate for Vec<T> {
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
}

impl<T> Accumulate for OptInSitu<T> {
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
        self.data.extend(value);
        self.offsets.push(next);
    }

    fn discard(&mut self) {
        self.offsets.discard();
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.offsets.is_empty() && self.data.is_empty()
    }
}

impl<T> Accumulate for OptSeq<T>
where
    T: Unfold,
{
    type Item = Option<Vec<T>>;

    fn push(&mut self, value: Self::Item) {
        let prev = self
            .offsets
            .last()
            .copied()
            .flatten()
            .unwrap_or(NonZeroU64::MIN);
        match value {
            Some(value) => {
                let next = prev.saturating_add(value.len() as u64);
                self.data.extend(value);
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
}

impl<S, T> Accumulate for Flatten<S>
where
    S: Accumulate<Item = Option<T>>,
{
    type Item = Option<Option<T>>;

    fn push(&mut self, value: Self::Item) {
        self.0.push(value.flatten());
    }

    fn discard(&mut self) {
        self.0.discard();
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/* ------------------------------------------------------------------ Serialize Trait Definition */

/// A **type** that can be serialized into a canonical [`clem`](crate) byte representation for
/// on-disk storage.
pub trait Serialize {
    /// Returns the number of bytes required to encode `self`.
    fn size(&self) -> usize {
        size_of::<Self>()
    }

    /// [`Serialize`](Self::serialize) `self` and append the encoded bytes to the provided buffer.
    fn serialize_into(&self, buf: &mut Vec<u8>);

    /// Serialize `self` and return the encoded bytes as a tight [`Box<[u8]>`].
    fn serialize(&self) -> Box<[u8]> {
        let size = self.size();
        let mut buf = Vec::with_capacity(size);
        self.serialize_into(&mut buf);
        debug_assert_eq!(buf.len(), size, "Actual size ≠ predicted size");
        buf.into_boxed_slice()
    }
}

/* -------------------------------------------------------------- Serialize Trait Implementation */

impl Serialize for char {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        debug_assert_eq!(size_of::<Self>(), size_of::<u32>(), "char is not 4 bytes");
        u32::from(*self).serialize_into(buf);
    }
}

impl Serialize for u8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }
}

impl Serialize for u16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for u32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for u64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for u128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for num::NonZeroU8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for i8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for num::NonZeroI8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for f32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for f64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for Option<char> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        // None writes u32::MAX (outside the valid scalar range).
        self.map_or(u32::MAX, u32::from).serialize_into(buf);
    }
}

impl Serialize for Option<num::NonZeroU8> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u8.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU16> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u16.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU32> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u32.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU64> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u64.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU128> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u128.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI8> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i8.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI16> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i16.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI32> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i32.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI64> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i64.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI128> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i128.serialize_into(buf),
        }
    }
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    fn size(&self) -> usize {
        self.iter().map(T::size).sum()
    }
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.iter().for_each(|v| v.serialize_into(buf))
    }
}

impl Serialize for BitVec {
    fn size(&self) -> usize {
        (self.len() + 7) / 8
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        // TODO → Ensure correctness. Is this conversion possible without intermediate allocation?
        let bytes = self.chunks(8).collect::<Vec<u8>>();
        buf.extend_from_slice(&bytes);
    }
}

impl<T> Serialize for OptInSitu<T>
where
    Vec<Option<T>>: Serialize,
{
    fn size(&self) -> usize {
        self.data.size()
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.data.serialize_into(buf)
    }
}

impl<T> Serialize for OptBitVec<T>
where
    T: Unfold + Default,
    T::RawAcc: Serialize,
{
    fn size(&self) -> usize {
        self.mask.size() + self.data.size()
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.mask.serialize_into(buf);
        self.data.serialize_into(buf);
    }
}

impl<T> Serialize for Seq<T>
where
    T: Unfold,
    T::RawAcc: Serialize,
{
    fn size(&self) -> usize {
        self.offsets.size() + self.data.size()
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.offsets.serialize_into(buf);
        self.data.serialize_into(buf);
    }
}

impl<T> Serialize for OptSeq<T>
where
    T: Unfold,
    T::RawAcc: Serialize,
{
    fn size(&self) -> usize {
        self.offsets.size() + self.data.size()
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.offsets.serialize_into(buf);
        self.data.serialize_into(buf);
    }
}

impl<T> Serialize for Flatten<T>
where
    T: Serialize,
{
    fn size(&self) -> usize {
        self.0.size() // Transparent wrapper
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.0.serialize_into(buf); // Transparent wrapper
    }
}
