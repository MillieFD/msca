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

use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::num::*;
use std::ops::Sub;

use bitvec::field::BitField;
use bitvec::vec::BitVec;
use minicbor::{CborLen, Decode, Encode};
use static_assertions::const_assert;

use crate::manifest::{self, Column, B};
use crate::number::Error;
use crate::schema::{size_of_opt, Unfold};
use crate::segment::{Align, Variant};
use crate::Sector;

/// Shorthand type-erased stack-allocated [pointer](Box) to an [`Accumulate`] trait object backed by
/// a heap-allocated growable [`Buffer`](Serialize::Buffer).
// NOTE: Buffer must be a growable Vec; compiler cannot predict the number of accumulated items
pub type BoxAcc<I> = Box<dyn Accumulate<I, Buffer = Vec<u8>>>;

/// Shorthand type-erased [`Iterator`] over mutable [`Column`] descriptors.
// NOTE: Deterministic runtime order via BTreeMap; #[derive] ensures identical compile time order.
pub type Columns<'a> = dyn Iterator<Item = &'a mut Column> + 'a;

/// An **in-memory staging buffer** used to build data segments for the specified [`Schema`].
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
/// │  ├─ count: NonZeroU64
/// │  └─ alignment padding
/// ├─ buffer 0
/// ⋮
/// └─ buffer N
/// ```
///
/// The [`Schema`][1] maps each **platform-agnostic** primitive [`Type`][3] to a contiguous buffer;
/// providing essential context for buffer deserialization. Each `Accumulator` holds a [`Sector`]
/// for the corresponding schema which is written to disk within each data segment header. All
/// columns contain an equal number of rows indicated by `count` in the segment header.
///
/// Refer to the [schema module documentation](crate::schema) for more details.
///
/// ### Concurrent Producers
///
/// Initialise a new accumulator via [`Dataset::schema`][4] which:
///
/// - Constructs a [`Schema`][2] for the specified [`Item`](I) type.
/// - Eagerly writes a schema segment to disk if unregistered.
/// - Reuses the existing schema segment if already registered.
/// - Returns a new empty accumulator.
///
/// Use [`Clone`] to initialise a new empty accumulator for the same [`Schema`][2] and [`Item`](I)
/// type. Users should prefer to clone an existing valid accumulator to bypass schema lookup and
/// [`Type`][3] verification compared to [`Dataset::schema`][4]. Each clone is independent and
/// starts empty.
///
/// 1. Register the [`Schema`][1] once via [`Dataset::schema`][4] to initialise an accumulator.
/// 2. [`Clone`] one accumulator for each worker thread.
/// 3. Each thread accumulates data independently.
/// 4. Commit each accumulator sequentially via [`Dataset::write`][5].
///
/// Refer to the [write-cycle documentation](crate::io) for more details.
///
/// [1]: crate::schema::Schema
/// [2]: crate::Data
/// [3]: crate::schema::Type
/// [4]: crate::Dataset::schema
/// [5]: crate::Dataset::write
pub struct Accumulator<I> {
    /// Type-erased [`Accumulate`] trait object.
    pub data: BoxAcc<I>,
    /// [Name](String) of the corresponding [`Schema`][1] registered in the [`Manifest`][2].
    ///
    /// [1]: crate::Schema
    /// [2]: manifest::Manifest
    pub(crate) name: String,
    /// [`Sector`] of the corresponding [`Schema`](crate::Schema) segment describing the structure
    /// of accumulated data.
    pub schema: Sector,
}

impl<I> Accumulator<I> {
    /// Total length of the data segment header in bytes, including [SIMD alignment](Align) padding.
    ///
    /// Refer to the [`Accumulator`] documentation for more details regarding header layout.
    pub(crate) const HEADER: usize = size_of::<Variant>()
        + size_of::<NonZeroU64>()
        + size_of::<NonZeroU64>()
        + size_of::<NonZeroU64>()
        + Self::ALIGN; // align to 64-bit boundary

    /// Number of trailing zero bytes required to pad the data segment [`header`](Self::HEADER) to
    /// the next 64-bit SIMD [alignment boundary](crate::segment).
    const ALIGN: usize = {
        let n = size_of::<Variant>()
            + size_of::<NonZeroU64>()
            + size_of::<NonZeroU64>()
            + size_of::<NonZeroU64>()
            & 7;
        (8 - n) & 7
    };
}

/// Returns a new empty [`Accumulator`] for the same [`Schema`][1] and [`Item`](I) type.
///
/// Prefer to [`Clone`] an existing valid accumulator to bypass schema lookup and [`Type`][2]
/// verification compared to [`Dataset::schema`][3].
///
/// [1]: manifest::Schema
/// [2]: crate::schema::Type
/// [3]: crate::Dataset::schema
impl<I> Clone for Accumulator<I>
where
    I: 'static,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.boxed(),
            name: self.name.clone(),
            schema: self.schema,
        }
    }
}

impl<I> Debug for Accumulator<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Accumulator")
            .field("item", &std::any::type_name::<I>())
            .field("name", &self.name)
            .field("schema", &self.schema)
            .finish()
    }
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
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
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
/// [`OptBitVec`] encodes [validity](Option) and [value](I) separately for non-niche types:
///
/// 1. A packed [`BitVec`] encodes [`Some`] as `true`.
/// 2. A contiguous data buffer encodes only [`Some`] values.
///
/// [`None`] entries append no data; the validity mask alone records their position. This design
/// improves storage density at the expense of index-based random access. Users are encouraged to
/// [`Stream`][1] data via the [`Query`](crate::Query) interface.
///
/// ### Guidance
///
/// The sibling [`OptInSitu`] type encodes [`Some`] and [`None`] values directly in a single data
/// buffer for supported niche types; no validity mask required. Implementors are advised to use
/// niche-optimised types when possible to improve storage efficiency and random read performance.
///
/// [1]: crate::read::Stream
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct OptBitVec<I>
where
    I: Unfold,
{
    /// Validity mask where `true → `[`Some`] and `false → `[`None`].
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BitVec::is_empty")
    )]
    pub mask: BitVec,
    /// Contiguous payload of [`Some`] items only; [`None`] items append no data.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: I::RawAcc,
}

// NOTE: #[derive(Default)] would impose I: Default trait bound which complicates the proc macro
impl<I> Default for OptBitVec<I>
where
    I: Unfold,
{
    fn default() -> Self {
        Self {
            mask: BitVec::default(),
            data: I::RawAcc::default(),
        }
    }
}

/// Data **accumulator** for [unsized][1] values.
///
/// ### Data Layout
///
/// It is not possible to predetermine the on-disk space required by each instance of an unsized
/// type; there is no guarantee that two [`Vec<I>`] contain the same number of elements.
/// [Clem](crate) therefore unfolds unsized types into:
///
/// 1. Columnar `offsets` region describing boundaries.
/// 2. Contiguous `data` region encoding values.
///
/// This design ensures **O(1) random access** and avoids per-element pointer chasing. Sequential
/// scans across the contained [items](I) remain linear; leveraging columnar optimisations for SIMD
/// and prefetch.
///
/// Each offset records one **zero-based** cumulative end per row, with `0` corresponding to the
/// start of the concatenated `data` region. Item `i` spans `offset[i - 1] → offset[i]` with an
/// implicit leading `0` if not otherwise specified. The offset count therefore equals the item
/// count recorded in the segment header.
///
/// ```text
/// offsets: [3, 6, 6, 8]
/// data:  [a, b, c, d, e, f, g, h]
/// ```
///
/// The serialized on-disk example above (four items) is deserialized into the memory representation
/// below. Implementers can specify which type to use for offset storage based on the number of
/// expected elements.
///
/// ```text
/// Row 0 → data[..3] → "abc"
/// Row 1 → data[3..6] → "def"
/// Row 2 → data[6..6] → "" (empty)
/// Row 3 → data[6..8] → "gh"
/// ```
///
/// Nested unsized types use **multiple offset layers** alongside a **single data region**. This
/// composable design preserves the performance advantages associated with contiguous value storage;
/// namely predictable vectorised traversal. Scanning performance across the contiguous inner `data`
/// region is unaffected by deep nesting. The inner offsets buffer is aligned in memory order of
/// traversal to improve cache locality during nested iteration and reduce TLB misses.
///
/// ```text
/// inner offsets
/// outer offsets
/// data
/// ```
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct Seq<I>
where
    I: Unfold,
{
    /// Cumulative end offsets.
    ///
    /// Offset `n` marks the exclusive end of item `n` and the inclusive start of item `n + 1`.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    // TODO Allow users to specify the offset type based on the number of expected elements.
    pub offsets: Vec<u64>,
    /// Flattened and concatenated [item](I) accumulator.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: I::RawAcc,
}

// NOTE: #[derive(Default)] would impose I: Default trait bound which complicates the API
impl<I> Default for Seq<I>
where
    I: Unfold,
{
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            data: I::RawAcc::default(),
        }
    }
}

/// Data **accumulator** for [optional](Option) [unsized][1] values.
///
/// ### Data Layout
///
/// It is not possible to predetermine the disk space required by each instance of an unsized type;
/// there is no guarantee that two [`Vec<T>`] contain the same number of elements. [Clem](crate)
/// therefore unfolds unsized types into:
///
/// 1. Columnar `offsets` region describing boundaries.
/// 2. Contiguous `data` region encoding values.
///
/// [`OptSeq`] encodes validity in the `offsets` buffer without an auxiliary bitmap. [`None`] items
/// are marked using a [`u64::MAX`] sentinel offset and append no data.
///
/// Refer to the [documentation](Seq) on non-optional unsized type accumulation for more details.
///
/// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[doc(hidden)]
pub struct OptSeq<T>
where
    T: Unfold,
{
    /// Cumulative end offsets.
    ///
    /// Offset `n` marks the exclusive end of item `n` and the inclusive start of item `n + 1`.
    /// [`u64::MAX`] marks a [`None`] item (no data appended).
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub offsets: Vec<u64>,
    /// Flattened and concatenated [item](I) accumulator; only [`Some`] items contribute entries.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Accumulate::is_empty")
    )]
    pub data: T::RawAcc,
}

impl<I> Default for OptSeq<I>
where
    I: Unfold,
{
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            data: I::RawAcc::default(),
        }
    }
}

/// Stateless type-level wrapper that flattens nested types on [`push`](Accumulate::push). All
/// storage lives in the inner accumulator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[doc(hidden)]
pub struct Flatten<I>(#[n(0)] pub I);

/* ----------------------------------------------------------------- Accumulate Trait Definition */

/// An in-memory **data accumulator** that ingests [items](I) of the specified [`Type`][1] and
/// [serializes](Serialize) into an optimised on-disk format.
///
/// [1]: crate::schema::Type
pub trait Accumulate<I>: Serialize {
    /// Returns a new empty instance of [`Self`] boxed as a [`BoxAcc`] trait object.
    // NOTE: Buffer must be a growable Vec; compiler cannot predict the number of accumulated items
    fn boxed(&self) -> BoxAcc<I>;

    /// Append one [`Item`](I) to the [accumulator](Self)
    fn push(&mut self, item: I);

    /// Reinitialise the [accumulator](Self) without writing to disk. All data is permanently lost.
    ///
    /// Note that this method may not affect the allocated capacity of the underlying storage.
    fn discard(&mut self);

    /// Returns `true` if the [accumulator](Self) contains no data.
    fn is_empty(&self) -> bool;

    /// Returns the number of accumulated rows.
    fn count(&self) -> u64;

    /// Returns the minimum accumulated value, or [`None`] if the [`Item`](I) is not meaningfully
    /// [orderable](PartialOrd).
    fn min(&self) -> Option<I> {
        None
    }

    /// Returns the maximum accumulated value, or [`None`] if the [`Item`](I) is not meaningfully
    /// [orderable](PartialOrd).
    fn max(&self) -> Option<I> {
        None
    }

    /// Generates one or more [`Buffer`] instances describing the accumulated data and appends to
    /// the [`Manifest`][1].
    ///
    /// Returns the next available offset for subsequent buffers, or [`Error`] on overflow.
    ///
    /// [1]: manifest::Manifest
    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error>;
}

/* ------------------------------------------------------------- Accumulate Trait Implementation */

impl<I> Accumulate<I> for Accumulator<I> {
    fn boxed(&self) -> BoxAcc<I> {
        self.data.boxed()
    }

    fn push(&mut self, item: I) {
        self.data.push(item);
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

    fn min(&self) -> Option<I> {
        self.data.min()
    }

    fn max(&self) -> Option<I> {
        self.data.max()
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        self.data.buffers(offset, columns)
    }
}

impl Accumulate<bool> for BitVec {
    fn boxed(&self) -> BoxAcc<bool> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: bool) {
        BitVec::push(self, item);
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

    fn min(&self) -> Option<bool> {
        const_assert!(false < true);
        self.iter().min().as_deref().copied()
    }

    fn max(&self) -> Option<bool> {
        const_assert!(false < true);
        self.iter().max().as_deref().copied()
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        let prefix: u64 = manifest::Buffer::HEADER.try_into()?;
        let buf = manifest::Buffer {
            sector: Sector {
                offset: offset.checked_add(prefix).ok_or(Error::Zero)?,
                length: { self.size()?.get() - prefix }.try_into()?,
            },
            count: self.count().try_into()?,
            min: Accumulate::min(self).map(u128::from).unwrap_or(u128::MIN).serialize()?,
            max: Accumulate::max(self).map(u128::from).unwrap_or(u128::MAX).serialize()?,
        };
        let next = buf.sector.next().ok_or(Error::Zero)?.align()?;
        columns.next().map(|column| column.buffers.push(buf));
        Ok(next)
    }
}

impl<I> Accumulate<I> for Vec<I>
where
    I: Serialize + Copy + PartialOrd + 'static,
{
    fn boxed(&self) -> BoxAcc<I> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: I) {
        Vec::push(self, item);
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

    fn min(&self) -> Option<I> {
        let rm_nan = |item: &I| item.partial_cmp(item).is_some();
        self.iter().copied().filter(rm_nan).reduce(|a, b| match a < b {
            true => a,
            false => b,
        })
    }

    fn max(&self) -> Option<I> {
        let rm_nan = |item: &I| item.partial_cmp(item).is_some();
        self.iter().copied().filter(rm_nan).reduce(|a, b| match a > b {
            true => a,
            false => b,
        })
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        let prefix: u64 = manifest::Buffer::HEADER.try_into()?;
        let min = [u8::MIN; B];
        let max = [u8::MAX; B];
        let buf = manifest::Buffer {
            sector: Sector {
                offset: offset.checked_add(prefix).ok_or(Error::Zero)?,
                length: { self.size()?.get() - prefix }.try_into()?,
            },
            count: self.count().try_into()?,
            min: match Accumulate::min(self) {
                Some(v) => min.serialize_push(&v)?,
                None => min,
            },
            max: match Accumulate::max(self) {
                Some(v) => max.serialize_push(&v)?,
                None => max,
            },
        };
        let next = buf.sector.next().ok_or(Error::Zero)?.align()?;
        columns.next().map(|column| column.buffers.push(buf));
        Ok(next)
    }
}

impl<I> Accumulate<Option<I>> for OptInSitu<I>
where
    Option<I>: Serialize,
    I: Copy + PartialOrd + 'static,
{
    fn boxed(&self) -> BoxAcc<Option<I>> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: Option<I>) {
        self.data.push(item);
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

    fn min(&self) -> Option<Option<I>> {
        self.data.min()
    }

    fn max(&self) -> Option<Option<I>> {
        self.data.max()
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        self.data.buffers(offset, columns)
    }
}

impl<I> Accumulate<Option<I>> for OptBitVec<I>
where
    I: Unfold + 'static,
{
    fn boxed(&self) -> BoxAcc<Option<I>> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: Option<I>) {
        if let Some(value) = item {
            self.mask.push(true);
            self.data.push(value);
        } else {
            // NOTE: contiguous payload of Some items only; None items append no data.
            self.mask.push(false);
        }
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

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        let prefix: u64 = manifest::Buffer::HEADER.try_into()?;
        let buf = manifest::Buffer {
            sector: Sector {
                offset: offset.checked_add(prefix).ok_or(Error::Zero)?,
                length: { self.size()?.get() - prefix }.try_into()?,
            },
            count: self.count().try_into()?,
            min: [u8::MIN; B],
            max: [u8::MAX; B],
        };
        let next = buf.sector.next().ok_or(Error::Zero)?.align()?;
        columns.next().map(|column| column.buffers.push(buf));
        Ok(next)
    }
}

impl<I> Accumulate<Vec<I>> for Seq<I>
where
    I: Unfold + 'static,
{
    fn boxed(&self) -> BoxAcc<Vec<I>> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: Vec<I>) {
        let size = item.len() as u64;
        let next = self.offsets.last().copied().unwrap_or(u64::MIN).saturating_add(size);
        item.into_iter().for_each(|i| self.data.push(i));
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

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        let prefix: u64 = manifest::Buffer::HEADER.try_into()?;
        let buf = manifest::Buffer {
            sector: Sector {
                offset: offset.checked_add(prefix).ok_or(Error::Zero)?,
                length: { self.size()?.get() - prefix }.try_into()?,
            },
            count: self.count().try_into()?,
            // NOTE: unsized collections are not meaningfully orderable; min and max are unpopulated
            min: [u8::MIN; B],
            max: [u8::MAX; B],
        };
        let next = buf.sector.next().ok_or(Error::Zero)?.align()?;
        columns.next().map(|column| column.buffers.push(buf));
        Ok(next)
    }
}

impl Accumulate<String> for Seq<u8> {
    fn boxed(&self) -> BoxAcc<String> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: String) {
        let bytes = item.into_bytes();
        self.push(bytes);
    }

    fn discard(&mut self) {
        Accumulate::<Vec<u8>>::discard(self);
    }

    fn is_empty(&self) -> bool {
        Accumulate::<Vec<u8>>::is_empty(self)
    }

    fn count(&self) -> u64 {
        Accumulate::<Vec<u8>>::count(self)
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        Accumulate::<Vec<u8>>::buffers(self, offset, columns)
    }
}

impl<I> Accumulate<Option<Vec<I>>> for OptSeq<I>
where
    I: Unfold + 'static,
{
    fn boxed(&self) -> BoxAcc<Option<Vec<I>>> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: Option<Vec<I>>) {
        if let Some(i) = item {
            let next = self
                .offsets
                .iter()
                .rev()
                .find(|&o| o != &u64::MAX)
                .copied()
                .unwrap_or(u64::MIN)
                .saturating_add(i.len() as u64);
            i.into_iter().for_each(|x| self.data.push(x));
            self.offsets.push(next);
        } else {
            // NOTE: contiguous payload of Some items only; None items append no data.
            self.offsets.push(u64::MAX);
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

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        let prefix: u64 = manifest::Buffer::HEADER.try_into()?;
        let buf = manifest::Buffer {
            sector: Sector {
                offset: offset.checked_add(prefix).ok_or(Error::Zero)?,
                length: { self.size()?.get() - prefix }.try_into()?,
            },
            count: self.count().try_into()?,
            min: [u8::MIN; B],
            max: [u8::MAX; B],
        };
        let next = buf.sector.next().ok_or(Error::Zero)?.align()?;
        columns.next().map(|column| column.buffers.push(buf));
        Ok(next)
    }
}

impl Accumulate<Option<String>> for OptSeq<u8> {
    fn boxed(&self) -> BoxAcc<Option<String>> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: Option<String>) {
        let bytes = item.map(String::into_bytes);
        self.push(bytes);
    }

    fn discard(&mut self) {
        Accumulate::<Option<Vec<u8>>>::discard(self);
    }

    fn is_empty(&self) -> bool {
        Accumulate::<Option<Vec<u8>>>::is_empty(self)
    }

    fn count(&self) -> u64 {
        Accumulate::<Option<Vec<u8>>>::count(self)
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        Accumulate::<Option<Vec<u8>>>::buffers(self, offset, columns)
    }
}

impl<A, B> Accumulate<Option<Option<B>>> for Flatten<A>
where
    A: Accumulate<Option<B>> + Default + Serialize<Buffer = Vec<u8>> + 'static,
{
    fn boxed(&self) -> BoxAcc<Option<Option<B>>> {
        Box::new(Self::default())
    }

    fn push(&mut self, item: Option<Option<B>>) {
        self.0.push(item.flatten());
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

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, Error> {
        self.0.buffers(offset, columns)
    }
}

/* --------------------------------------------------------------------- Buffer Trait Definition */

/// A **buffer** that can hold the serialized byte representation of a value.
///
/// Blanket implementations are provided for stack-allocated byte arrays and heap-allocated byte
/// vectors. This design defines a fixed-size buffer for types with a known size at compile time,
/// while facilitating dynamic buffer sizing for types that require heap allocation.
pub trait Buffer: AsRef<[u8]> + AsMut<[u8]> + Into<Vec<u8>> {
    /// [`Serialize`] the provided [`item`](I) and append into [`self`](Buffer).
    ///
    /// Writing always begins at the **start** of the buffer; chained calls overwrite. Chain
    /// sequential writes through [`Serialize::serialize_into`], which returns the advanced slice.
    fn serialize_push<I: Serialize>(self, item: &I) -> Result<Self, Error>;

    fn serialize_push_aligned<I: Serialize>(self, item: &I) -> Result<Self, Error>;
}

/* ----------------------------------------------------------------- Buffer Trait Implementation */

impl Buffer for &mut [u8] {
    fn serialize_push<I: Serialize>(self, item: &I) -> Result<Self, Error> {
        item.serialize_into(self)
    }

    fn serialize_push_aligned<I: Serialize>(self, item: &I) -> Result<Self, Error> {
        item.serialize_into_aligned(self)
    }
}

impl<const N: usize> Buffer for [u8; N] {
    fn serialize_push<I: Serialize>(mut self, item: &I) -> Result<Self, Error> {
        item.serialize_into(&mut self)?;
        Ok(self)
    }

    fn serialize_push_aligned<I: Serialize>(mut self, item: &I) -> Result<Self, Error> {
        item.serialize_into_aligned(&mut self)?;
        Ok(self)
    }
}

impl Buffer for Vec<u8> {
    fn serialize_push<I: Serialize>(self, item: &I) -> Result<Self, Error> {
        item.extend(self)
    }

    fn serialize_push_aligned<I: Serialize>(mut self, item: &I) -> Result<Self, Error> {
        item.serialize_into_aligned(&mut self)?;
        Ok(self)
    }
}

/* ------------------------------------------------------------------ Serialize Trait Definition */

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
    fn size(&self) -> Result<NonZeroU64, Error>;

    /// Serialize `self` into the provided [`Buffer`].
    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error>;

    /// Serialize `self` and return the encoded bytes in a new [`Buffer`].
    fn serialize(&self) -> Result<Self::Buffer, Error>;

    /// Serialize `self` into the provided [`Vec<u8>`] sink.
    ///
    /// This function provides a unified interface for serializing fixed-size (stack) and
    /// dynamic-size (heap) types into a growable [`Buffer`].
    fn extend(&self, mut sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        let buf = &mut sink;
        self.serialize_into(buf)?;
        Ok(sink)
    }

    /// Serialize `self` into the provided [`Buffer`] and [`Align`] to the next 64-bit boundary.
    fn serialize_into_aligned<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let pad = self.size()?.pad()?;
        let buf = self.serialize_into(buf)?;
        debug_assert!(buf.len() >= pad, "actual size < aligned size");
        buf[..pad].fill(u8::MIN);
        Ok(&mut buf[pad..])
    }
}

/* -------------------------------------------------------------- Serialize Trait Implementation */

impl<const N: usize> Serialize for [u8; N] {
    type Buffer = Self;

    fn size(&self) -> Result<NonZeroU64, Error> {
        { N as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        debug_assert!(buf.len() >= N, "actual size < expected size");
        buf[..N].copy_from_slice(self);
        Ok(&mut buf[N..])
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(*self)
    }
}

impl Serialize for char {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        static_assertions::assert_eq_size!(char, u32);
        u32::from(*self).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        u32::from(*self).serialize()
    }
}

impl Serialize for bool {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        u8::from(*self).serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok([u8::from(*self)])
    }
}

impl Serialize for u8 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok([*self])
    }
}

impl Serialize for u16 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for u32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for u64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for u128 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for i8 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i16 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for i128 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.get().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        self.get().serialize()
    }
}

impl Serialize for f32 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for f64 {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        self.to_le_bytes().serialize_into(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        Ok(self.to_le_bytes())
    }
}

impl Serialize for Option<char> {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        { size_of::<Self>() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        // Recursively sum the sizes of all elements.
        self.iter()
            .try_fold(u64::MIN, |total, element| {
                let size = element.size()?.get();
                total.checked_add(size).ok_or(Error::Zero)
            })
            .map(NonZeroU64::new)
            .transpose()
            .ok_or(Error::Zero)
            .flatten()
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let prefix: u64 = size_of::<NonZeroU64>().try_into()?;
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let buf = self.size()?.get().sub(prefix).to_le_bytes().serialize_into(buf)?;
        self.iter().try_fold(buf, |sink, element| element.serialize_into(sink))
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

impl<K, I> Serialize for BTreeMap<K, I>
where
    K: Serialize + Ord,
    I: Serialize,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        // Recursively sum the sizes of all elements.
        self.iter()
            .try_fold(u64::MIN, |total, entry| {
                let size = entry.0.size()?.get() + entry.1.size()?.get();
                total.checked_add(size).ok_or(Error::Zero)
            })
            .map(NonZeroU64::new)
            .transpose()
            .ok_or(Error::Zero)
            .flatten()
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let prefix: u64 = size_of::<NonZeroU64>().try_into()?;
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let buf = self.size()?.get().sub(prefix).to_le_bytes().serialize_into(buf)?;
        self.iter().try_fold(buf, |sink, entry| {
            let sink = entry.0.serialize_into(sink)?;
            entry.1.serialize_into(sink)
        })
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let prefix: u64 = size_of::<NonZeroU64>().try_into()?;
        // NOTE: Self::size returns Error if BitVec::len overflows u64 (not expected in production)
        let buf = self.size()?.get().sub(prefix).to_le_bytes().serialize_into(buf)?;
        // Intermediate chunks contain 8 bits in Lsb0 order; the final chunk may contain ≤ 8 bits.
        // BitVec::load_le packs each chunk into one u8 in LE order, padding with zeros if the final
        // chunk is shorter than 8 bits. The resulting bytes are pushed into the provided buffer.
        self.chunks(8).try_fold(buf, |sink, bits| bits.load_le::<u8>().serialize_into(sink))
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
    T: Unfold,
    T::RawAcc: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let data = self.data.size()?.align()?;
        let prefix = size_of::<NonZeroU64>().try_into()?; // Length prefix
        self.mask
            .size()?
            .align()?
            .checked_add(data)
            .ok_or(Error::Zero)?
            .checked_add(prefix)
            .and_then(NonZeroU64::new)
            .ok_or(Error::Zero)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let prefix: u64 = size_of::<NonZeroU64>().try_into()?;
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let buf = self.size()?.get().sub(prefix).to_le_bytes().serialize_into(buf)?;
        buf.serialize_push_aligned(&self.mask)?.serialize_push_aligned(&self.data)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

impl<T> Serialize for Seq<T>
where
    T: Unfold,
    T::RawAcc: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let data = self.data.size()?.align()?;
        let prefix = size_of::<NonZeroU64>().try_into()?; // Length prefix
        self.offsets
            .size()?
            .align()?
            .checked_add(data)
            .ok_or(Error::Zero)?
            .checked_add(prefix)
            .and_then(NonZeroU64::new)
            .ok_or(Error::Zero)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let prefix: u64 = size_of::<NonZeroU64>().try_into()?;
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let buf = self.size()?.get().sub(prefix).to_le_bytes().serialize_into(buf)?;
        buf.serialize_push_aligned(&self.offsets)?.serialize_push_aligned(&self.data)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

impl<T> Serialize for OptSeq<T>
where
    T: Unfold,
    T::RawAcc: Serialize<Buffer = Vec<u8>>,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let data = self.data.size()?.align()?;
        let prefix = size_of::<NonZeroU64>().try_into()?; // Length prefix
        self.offsets
            .size()?
            .align()?
            .checked_add(data)
            .ok_or(Error::Zero)?
            .checked_add(prefix)
            .and_then(NonZeroU64::new)
            .ok_or(Error::Zero)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let prefix: u64 = size_of::<NonZeroU64>().try_into()?;
        // NOTE: Self::size returns Error if Σ overflows u64 (not expected in production)
        let buf = self.size()?.get().sub(prefix).to_le_bytes().serialize_into(buf)?;
        buf.serialize_push_aligned(&self.offsets)?.serialize_push_aligned(&self.data)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
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

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
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
        self.data.size()?.align()?.checked_add(header).and_then(NonZeroU64::new).ok_or(Error::Zero)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        // 1. Serialize header fields (see accumulator documentation)
        let buf = { Variant::Data as u8 }.serialize_into(buf)?;
        let buf = self.data.size()?.align()?.serialize_into(buf)?;
        let buf = self.schema.offset.serialize_into(buf)?;
        let buf = self.data.count().serialize_into(buf)?;
        // 2. Align to the next 64-bit boundary
        buf[..Self::ALIGN].fill(u8::MIN);
        // 3. Serialize columnar data buffers
        self.data.serialize_into_aligned(&mut buf[Self::ALIGN..])
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;

    /// [`Accumulate::min`] and [`Accumulate::max`] return [`Some`] for populated [`Vec`].
    #[test]
    fn vec_min_max() {
        let data: Vec<u32> = vec![1, 2, 3];
        assert_eq!(Accumulate::min(&data), Some(1));
        assert_eq!(Accumulate::max(&data), Some(3));
    }

    /// [`Accumulate::min`] and [`Accumulate::max`] return [`Some`] for empty [`Vec`].
    #[test]
    fn vec_min_max_empty() {
        let data: Vec<u32> = Vec::new();
        assert_eq!(Accumulate::min(&data), None);
        assert_eq!(Accumulate::max(&data), None);
    }

    /// [`Accumulate::min`] and [`Accumulate::max`] ignore [`f64::NAN`] values in [`Vec`].
    #[test]
    fn vec_min_max_ignore_nan() {
        let data = vec![1.0, 2.0, 3.0, f64::NAN];
        assert_eq!(Accumulate::min(&data), Some(1.0));
        assert_eq!(Accumulate::max(&data), Some(3.0));
    }

    /// [`Accumulate::discard`] empties the buffer and resets [`Accumulate::count`] to zero.
    #[test]
    fn accumulate_discard_resets_count() {
        let mut data = vec![1, 2, 3];
        assert_eq!(Accumulate::count(&data), 3);
        data.discard();
        assert!(Accumulate::is_empty(&data));
        assert_eq!(Accumulate::count(&data), 0);
    }

    /// [`Accumulate::min`] and [`Accumulate::max`] return [`Some`] for populated [`BitVec`].
    #[test]
    fn bit_vec_min_max() {
        let data: BitVec = [true, false, true].into_iter().collect();
        assert_eq!(Accumulate::min(&data), Some(false));
        assert_eq!(Accumulate::max(&data), Some(true));
    }

    /// [`Accumulate::min`] returns `true` if all bits are `true`.
    /// [`Accumulate::max`] returns `true` if any bit is `true`.
    #[test]
    fn bit_vec_min_max_true() {
        let data: BitVec = [true, true, true].into_iter().collect();
        assert_eq!(Accumulate::min(&data), Some(true));
        assert_eq!(Accumulate::max(&data), Some(true));
    }

    /// [`Accumulate::min`] returns `false` if any bit is `false`.
    /// [`Accumulate::max`] returns `false` if all bits are `false`.
    #[test]
    fn bit_vec_min_max_false() {
        let data: BitVec = [false, false, false].into_iter().collect();
        assert_eq!(Accumulate::min(&data), Some(false));
        assert_eq!(Accumulate::max(&data), Some(false));
    }

    /// [`Accumulate::min`] and [`Accumulate::max`] return [`None`] for empty [`BitVec`].
    #[test]
    fn bit_vec_min_max_empty() {
        let data: BitVec = BitVec::new();
        assert_eq!(Accumulate::min(&data), None);
        assert_eq!(Accumulate::max(&data), None);
    }

    /// [`Align::align`] rounds [`size`](Serialize::size) up ↑ to the next 64-bit boundary.
    #[test]
    fn aligned_rounds_size() {
        let data: Vec<u16> = vec![1, 2, 3]; // 8 byte prefix + 6 byte payload
        let size = data.size().expect("Size failed");
        assert_eq!(size.get(), 14);
        assert_eq!(size.align().expect("Align failed"), 16);
    }

    /// [`Serialize::serialize_into_aligned`] adds zero-bytes up to the next 64-bit boundary.
    #[test]
    fn serialize_into_aligned_pads() {
        let data: Vec<u16> = vec![1, 2, 3];
        let mut buf = [0xFFu8; 16];
        let rest = data.serialize_into_aligned(&mut buf).expect("Align failed");
        assert!(rest.is_empty());
        assert_eq!(buf[..8], 6u64.to_le_bytes()); // Length prefix excludes padding
        assert_eq!(buf[14..], [u8::MIN; 2]); // Trailing bytes are zero-filled
    }

    /// [`OptBitVec`] aligns the value buffer to the boundary following the validity mask and stores
    /// only [`Some`] values in the concatenated payload.
    #[test]
    fn opt_bit_vec_layout() {
        let mut acc: OptBitVec<u32> = OptBitVec::default();
        [Some(1u32), None, Some(3)].into_iter().for_each(|v| acc.push(v));
        let bytes = acc.serialize().expect("Serialize failed");
        assert_eq!(bytes.len(), 40); // [prefix 8][mask 9 → 16][data 16]
        assert_eq!(acc.size().expect("Size failed").get(), 40); // Composite is self-padded
        assert_eq!(bytes[..8], 32u64.to_le_bytes()); // Outer prefix spans padded interior
        assert_eq!(bytes[8..16], 1u64.to_le_bytes()); // Mask length prefix records exact size
        assert_eq!(bytes[16], 0b101); // Mask bits in Lsb0 order
        assert_eq!(bytes[17..24], [u8::MIN; 7]); // Mask padding bytes are zero-filled
        assert_eq!(bytes[24..32], 8u64.to_le_bytes()); // Value length prefix excludes None rows
        assert_eq!(bytes[32..36], 1u32.to_le_bytes()); // Only Some values are stored, contiguously
        assert_eq!(bytes[36..40], 3u32.to_le_bytes());
    }

    /// [`Seq`] offsets terminate on the boundary; data follows without intermediate padding.
    #[test]
    fn seq_layout() {
        let mut acc: Seq<u8> = Seq::default();
        acc.push(vec![97, 98, 99]);
        acc.push(vec![100, 101]);
        let bytes = acc.serialize().expect("Serialize failed");
        assert_eq!(bytes.len(), 48); // [prefix 8][offsets 24][data 13 → 16]
        assert_eq!(bytes[..8], 40u64.to_le_bytes()); // Outer prefix spans padded interior
        assert_eq!(bytes[8..16], 16u64.to_le_bytes()); // Offsets length prefix records exact size
        assert_eq!(bytes[16..24], 3u64.to_le_bytes()); // Zero-based cumulative end of row 0
        assert_eq!(bytes[24..32], 5u64.to_le_bytes()); // Zero-based cumulative end of row 1
        assert_eq!(bytes[32..40], 5u64.to_le_bytes()); // Data length prefix records exact size
        assert_eq!(bytes[40..45], [97, 98, 99, 100, 101]);
        assert_eq!(bytes[45..48], [u8::MIN; 3]); // Data padding bytes are zero-filled
    }

    /// [`Accumulate::buffers`] records exact sector lengths and returns aligned offsets.
    #[test]
    fn buffers_align_offsets() {
        let data: Vec<u16> = vec![1, 2, 3];
        let mut col = Column::from(u16::with_unfolder::<Schema>());
        let next = data.buffers(0, &mut std::iter::once(&mut col)).expect("Buffers failed");
        assert_eq!(next, 16); // Next buffer begins at 64-bit alignment boundary
        assert_eq!(col.buffers[0].sector.offset, 8); // Body starts after the header prefix
        assert_eq!(col.buffers[0].sector.length.get(), 6); // Body excludes the prefix and padding
    }

    /// [`OptBitVec`] records the data buffer at its aligned offset inside the composite region.
    #[test]
    fn opt_bit_vec_buffers_offset() {
        let mut acc: OptBitVec<u32> = OptBitVec::default();
        [Some(1u32), None, Some(3)].into_iter().for_each(|v| acc.push(v));
        let mut col = Column::from(u32::with_unfolder::<Schema>());
        let next = acc.buffers(0, &mut std::iter::once(&mut col)).expect("Buffers failed");
        assert_eq!(next, 40); // Aligned end of the composite region
        assert_eq!(col.buffers[0].sector.offset, 8); // Whole body starts after the header prefix
        assert_eq!(col.buffers[0].sector.length.get(), 32); // Body spans the mask and data regions
    }

    /// [`Seq<u8>`] accumulates a [`String`] into the identical layout as its raw UTF-8 bytes.
    #[test]
    fn seq_string_matches_bytes() {
        let mut text: Seq<u8> = Seq::default();
        text.push(String::from("héllo"));
        text.push(String::from("xyz"));
        let mut bytes: Seq<u8> = Seq::default();
        bytes.push("héllo".as_bytes().to_vec());
        bytes.push(b"xyz".to_vec());
        let text = text.serialize().expect("Serialize failed");
        let bytes = bytes.serialize().expect("Serialize failed");
        assert_eq!(text, bytes);
    }

    /// [`OptSeq<u8>`] accumulates an [`Option<String>`] identically to its raw optional UTF-8 bytes;
    /// the [`u64::MAX`] sentinel marks [`None`] in both.
    #[test]
    fn opt_seq_string_matches_bytes() {
        let mut text: OptSeq<u8> = OptSeq::default();
        text.push(Some(String::from("ab")));
        text.push(None::<String>);
        text.push(Some(String::from("c")));
        let mut bytes: OptSeq<u8> = OptSeq::default();
        bytes.push(Some(b"ab".to_vec()));
        bytes.push(None::<Vec<u8>>);
        bytes.push(Some(b"c".to_vec()));
        let text = text.serialize().expect("Serialize failed");
        let bytes = bytes.serialize().expect("Serialize failed");
        assert_eq!(text, bytes);
    }
}
