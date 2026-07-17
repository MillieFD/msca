/*
Project: msca
GitHub: https://github.com/MillieFD/msca

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
//! - [`Buffer`] → State machine to determine the appropriate encoding strategy.
//!
//! Each accumulator type implements the [`Accumulate`] trait, which defines a shared interface for
//! handling in-memory value accumulation.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::iter;
use std::num::*;
use std::ops::Range;

use bitvec::field::BitField;
use bitvec::vec::BitVec;
use minicbor::{CborLen, Decode, Encode};

use crate::io::{Checksum, Register, SizedBuf, HEADER};
use crate::manifest::{self, Column, Manifest};
use crate::number::Error;
use crate::schema::{self, size_of_opt, BitMatch, Unfold};
use crate::segment::{Align, Header, Segment, Variant};
use crate::{io, Sector};

/// Shorthand type-erased stack-allocated [pointer](Box) to a [`Describe`] trait object backed by a
/// heap-allocated growable [`Buffer`](Serialize::Buffer).
// NOTE: Buffer must be a growable Vec; compiler cannot predict the number of accumulated items
pub type BoxAcc<I> = Box<dyn Describe<I, Buffer = Vec<u8>>>;

/// Shorthand type-erased [`Iterator`] over mutable [`Column`] descriptors.
// NOTE: Deterministic runtime order via BTreeMap; #[derive] ensures identical compile time order.
pub type Columns<'a> = dyn Iterator<Item = &'a mut Column> + 'a;

/// An **in-memory staging buffer** used to build data segments for the specified [`Schema`].
///
/// ### Segment Composition
///
/// Each [msca](crate) file is partitioned into self-describing segments which are immutable once
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
/// │  └─ size: NonZeroU64
/// ├─ metadata
/// │  ├─ schema: NonZeroU64
/// │  ├─ count: NonZeroU64
/// │  └─ alignment padding
/// ├─ 1st buffer
/// ⋮
/// ├─ Nth buffer
/// └─ checksum: u64
/// ```
///
/// The [`Schema`][1] maps each **platform-agnostic** primitive [`Type`][3] to a contiguous buffer;
/// providing essential context for buffer deserialization. Each `Accumulator` holds a [`Sector`]
/// for the corresponding schema which is written to disk within each data segment header. All
/// columns contain an equal number of rows indicated by `count` in the segment header.
///
/// Refer to the [schema module documentation](schema) for more details.
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
/// Refer to the [write-cycle documentation](io) for more details.
///
/// [1]: schema::Schema
/// [2]: crate::Data
/// [3]: schema::Type
/// [4]: crate::Dataset::schema
/// [5]: crate::Dataset::write
pub struct Accumulator<I> {
    /// Type-erased [`Describe`] trait object.
    pub data: BoxAcc<I>,
    /// [Name](String) of the corresponding [`Schema`][1] registered in the [`Manifest`].
    ///
    /// [1]: crate::Schema
    pub(crate) name: String,
    /// [`Sector`] of the corresponding [`Schema`](crate::Schema) segment describing the structure
    /// of accumulated data.
    pub schema: Sector,
}

impl<I> Accumulator<I> {
    /// Size of the data segment [`Header`] and metadata in bytes.
    ///
    /// Refer to the [`Accumulator`] documentation for more details regarding segment layout.
    pub(crate) const HEADER: usize = Header::SIZE
        + size_of::<NonZeroU64>() // corresponding schema segment offset
        + size_of::<NonZeroU64>(); // number of items in this segment (count)

    /// Returns the exact number of bytes required to encode the data segment body for `self`;
    /// including segment metadata and metadata→body [alignment](Align) bytes.
    ///
    /// ### ⚠️ Caution
    ///
    /// This function **cannot** predict the exact on-disk segment size. The number of
    /// header→metadata alignment bytes is determined by the absolute [`Segment`] offset. This
    /// function also excludes the segment [`Header`] and [`Checksum`]. Refer to [`Segment::wrap`]
    /// for an accurate on-disk representation.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`] if the size overflows `u64` or the [`Accumulator`] is empty.
    fn size(&self) -> Result<NonZeroU64, Error> {
        let meta = { size_of::<NonZeroU64>() + size_of::<NonZeroU64>() } as u64;
        self.data.size()?.align()?.checked_add(meta).and_then(NonZeroU64::new).ok_or(Error::Zero)
    }
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

impl<I> OptBitVec<I>
where
    I: Unfold,
{
    /// Byte offset of the `data` sub-buffer within the serialized [`OptBitVec`] body; excludes the
    /// `mask` sub-buffer and data size prefix.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`] if the offset overflows `u64`.
    fn origin(&self) -> Result<u64, Error> {
        let mask = SizedBuf::new(&self.mask).size()?.get();
        mask.checked_add(SizedBuf::<I::RawAcc>::PREFIX).ok_or(Error::Zero)
    }
}

/// Data **accumulator** for [unsized][1] values.
///
/// ### Data Layout
///
/// It is not possible to predetermine the on-disk space required by each instance of an unsized
/// type; there is no guarantee that two [`Vec<I>`] contain the same number of elements.
/// The [msca](crate) engine therefore unfolds unsized types into:
///
/// 1. Columnar `ends` region describing boundaries.
/// 2. Contiguous `data` region encoding items.
///
/// This design ensures **O(1) random access** and avoids per-element pointer chasing. Sequential
/// scans across the contained [items](I) remain linear; leveraging columnar optimisations for SIMD
/// and prefetch.
///
/// Each offset records one **zero-based** cumulative end per row, with `0` corresponding to the
/// start of the concatenated `data` region. Item `i` spans `ends[i - 1] → ends[i]` with an
/// implicit leading `0` if not otherwise specified. The offset count therefore equals the item
/// count recorded in the segment header.
///
/// ```text
/// ends: [3, 6, 6, 8]
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
/// composable design preserves the performance advantages associated with contiguous item storage;
/// namely predictable vectorised traversal. Scanning performance across the contiguous inner `data`
/// region is unaffected by deep nesting. The inner ends buffer is aligned in memory order of
/// traversal to improve cache locality during nested iteration and reduce TLB misses.
///
/// ```text
/// inner ends
/// outer ends
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
    /// Cumulative ends.
    ///
    /// Offset `n` marks the exclusive end of item `n` and the inclusive start of item `n + 1`.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    // TODO Allow users to specify the offset type based on the number of expected elements.
    pub ends: Vec<u64>,
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
            ends: Vec::new(),
            data: I::RawAcc::default(),
        }
    }
}

impl<I> Seq<I>
where
    I: Unfold + Clone + 'static,
{
    /// Returns an [`Iterator`] over [`Range`] instances that each describe the location of one item
    /// within the concatenated `data` sub-buffer, where item `n` spans `ends[n - 1] → ends[n]` with
    /// an implicit leading `0`.
    ///
    /// Refer to the [unsized accumulator documentation](Seq) for more details.
    #[allow(unused)]
    fn bounds(&self) -> impl Iterator<Item = Range<u64>> {
        let ubs = self.ends.iter().copied();
        let lbs = ubs.clone();
        iter::once(u64::MIN).chain(lbs).zip(ubs).map(|b| b.0..b.1)
    }
}

/// Data **accumulator** for [optional](Option) [unsized][1] items.
///
/// ### Data Layout
///
/// It is not possible to predetermine the disk space required by each instance of an unsized type;
/// there is no guarantee that two [`Vec<T>`] contain the same number of elements. The [msca](crate)
/// engine therefore unfolds unsized types into:
///
/// 1. Columnar `ends` region describing boundaries.
/// 2. Contiguous `data` region encoding items.
///
/// [`OptSeq`] encodes validity in the `ends` buffer without an auxiliary bitmap. [`None`] items are
/// marked using a [`u64::MAX`] sentinel offset and append no data.
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
    pub ends: Vec<u64>,
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
            ends: Vec::new(),
            data: I::RawAcc::default(),
        }
    }
}

impl<I> OptSeq<I>
where
    I: Unfold,
{
    /// Returns an [`Iterator`] over [`Range`] instances that each describe the location of one item
    /// within the concatenated `data` sub-buffer, where item `n` spans `ends[n - 1] → ends[n]` with
    /// an implicit leading `0`.
    ///
    /// Refer to the [optional unsized accumulator](OptSeq) documentation for more details.
    #[allow(unused)]
    fn bounds(&self) -> impl Iterator<Item = Range<u64>> {
        let ubs = self.ends.iter().copied().filter(|&end| end != u64::MAX);
        let lbs = ubs.clone();
        iter::once(u64::MIN).chain(lbs).zip(ubs).map(|b| b.0..b.1)
    }
}

/// Stateless type-level wrapper that flattens nested types on [`push`](Accumulate::push). All
/// storage lives in the inner accumulator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[doc(hidden)]
pub struct Flatten<I>(#[n(0)] pub I);

/// A **state machine** used to build [data buffers](manifest::Buffer) for the specified [`Column`].
///
/// ### Buffer Composition
///
/// Real-world applications often require the inclusion of columns with infrequently altered items.
/// It is possible for a column to contain only **one** repeated item across an entire data segment.
/// Instead of repeatedly encoding identical items, [msca](crate) defaults to a **compact buffer**
/// representation to improve storage density.
// TODO → Add link to on-disk-format.md for more information.
///
/// ##### 1. Empty
///
/// Each column begins in the [`Empty`](Buffer::Empty) state, which is never written to disk. If an
/// empty [`Buffer`] is encountered during the [write-cycle](crate::io), the entire data segment is
/// discarded. This behaviour may change in future releases; using absent buffers to encode a
/// type-dependent [`Default`].
///
/// ##### 2. Compact
///
/// The column transitions to the [`Compact`](Buffer::Compact) state when the first [`item`](I) is
/// [pushed](Accumulate::push). This state holds the item directly and tracks the number of
/// accumulated repetitions to coalesce homogenous runs. All subsequently [accumulated](Accumulate)
/// items are compared against this value.
///
/// ##### 3. Many
///
/// The column transitions to the [`Many`](Buffer::Many) state when a [pushed](Accumulate::push)
/// item is not [bit-identical](BitMatch::eq) to the [accumulated](Accumulate) item; materialising
/// the required [accumulator](`I::RawAcc`) with the specified number of repeated items.
///
/// Buffer `Empty → Compact → Many` variant escalation is unidirectional; a `Compact` buffer can
/// never return to the `Empty` state.
///
/// ### Guidance
///
/// Implementers are encouraged to use a [`bin`] segment for genuinely constant data that never
/// changes across the entire file lifetime. This improves storage efficiency by eliminating an
/// unnecessary column from the schema.
// TODO → add doc link to binary segment
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum Buffer<I>
where
    I: Unfold,
{
    /// A buffer which does not contain any data; never written to disk.
    Empty,
    /// A buffer containing a single [`item`](I) repeated `count` times; used to coalesce homogenous
    /// runs into an efficient on-disk representation to improve storage density.
    Compact {
        /// The single repeated item.
        item: I,
        /// The number of accumulated repetitions.
        count: u64,
    },
    /// A buffer containing many different [items](I) collected into an [accumulator](I::RawAcc).
    Many(I::RawAcc),
}

impl<I> Buffer<I>
where
    I: Unfold + Clone,
{
    /// Constructor for [`Buffer::Many`] that materialises the required [accumulator](`I::RawAcc`)
    /// containing the specified number of [repeated](iter::repeat_n) identical [items](I) followed
    /// by the one new item.
    fn upgrade(item: &I, count: &u64, new: I) -> Self {
        let one = iter::once(new);
        let acc = iter::repeat_n(item.clone(), *count as usize).chain(one).collect();
        Self::Many(acc)
    }
}

/// Constructor for [`Buffer::Empty`].
impl<I> Default for Buffer<I>
where
    I: Unfold,
{
    fn default() -> Self {
        Self::Empty
    }
}

/// Constructor for [`Buffer::Compact`] wrapping the specified [`item`](I) with a count of one.
impl<I> From<I> for Buffer<I>
where
    I: Unfold,
{
    fn from(item: I) -> Self {
        Self::Compact { item, count: 1 }
    }
}

/* ----------------------------------------------------------------- Accumulate Trait Definition */

/// An in-memory **data accumulator** that ingests [items](I) of the specified [`Type`][1] and
/// [serializes](Serialize) into an optimised on-disk format.
///
/// [1]: schema::Type
pub trait Accumulate<I> {
    /// Append one [`Item`](I) to the [accumulator](Self)
    fn push(&mut self, item: I);

    /// Reinitialise the [accumulator](Self) without writing to disk. All data is permanently lost.
    ///
    /// Note that this method may not affect the allocated capacity of the underlying storage.
    fn discard(&mut self);

    /// Returns `true` if the [accumulator](Self) contains no data.
    fn is_empty(&self) -> bool;

    /// Returns the number of accumulated [`items`](I).
    fn count(&self) -> u64;
}

    /// Returns `true` if **any** accumulated [`item`](I) is **bit-identical** to the provided item.
    fn contains(&self, item: &I) -> bool;

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
}

/* ------------------------------------------------------------- Accumulate Trait Implementation */

impl<I> Accumulate<I> for Accumulator<I> {
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
}

impl Accumulate<bool> for BitVec {
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
}

impl<I> Accumulate<I> for Vec<I>
where
    I: BitMatch + Copy + PartialOrd + Serialize + Unfold + 'static,
{
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
}

impl<I> Accumulate<Option<I>> for OptInSitu<I>
where
    Option<I>: Serialize,
    I: BitMatch + Copy + PartialOrd + Unfold + 'static,
{
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
}

impl<I> Accumulate<Option<I>> for OptBitVec<I>
where
    I: Unfold + 'static,
{
    fn push(&mut self, item: Option<I>) {
        if let Some(i) = item {
            self.mask.push(true);
            self.data.push(i);
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
}

impl<I> Accumulate<Vec<I>> for Seq<I>
where
    I: Unfold + 'static,
{
    fn push(&mut self, item: Vec<I>) {
        let size = item.len() as u64;
        let next = self.ends.last().copied().unwrap_or(u64::MIN).saturating_add(size);
        item.into_iter().for_each(|i| self.data.push(i));
        self.ends.push(next);
    }

    fn discard(&mut self) {
        self.ends.discard();
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.ends.is_empty() && self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.ends.len() as u64
    }
}

impl Accumulate<String> for Seq<u8> {
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
}

impl<I> Accumulate<Option<Vec<I>>> for OptSeq<I>
where
    I: Unfold,
{
    fn push(&mut self, item: Option<Vec<I>>) {
        if let Some(i) = item {
            let next = self
                .ends
                .iter()
                .rev()
                .find(|&o| o != &u64::MAX)
                .copied()
                .unwrap_or(u64::MIN)
                .saturating_add(i.len() as u64);
            i.into_iter().for_each(|x| self.data.push(x));
            self.ends.push(next);
        } else {
            // NOTE: contiguous payload of Some items only; None items append no data.
            self.ends.push(u64::MAX);
        }
    }

    fn discard(&mut self) {
        self.ends.discard();
        self.data.discard();
    }

    fn is_empty(&self) -> bool {
        self.ends.is_empty() && self.data.is_empty()
    }

    fn count(&self) -> u64 {
        self.ends.len() as u64
    }
}

impl Accumulate<Option<String>> for OptSeq<u8> {
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
}

impl<A, B> Accumulate<Option<Option<B>>> for Flatten<A>
where
    A: Accumulate<Option<B>> + Default + Serialize<Buffer = Vec<u8>> + 'static,
{
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
}

impl<I> Accumulate<I> for Buffer<I>
where
    I: BitMatch + Clone + Unfold,
{
    fn push(&mut self, new: I) {
        match self {
            Self::Empty => *self = new.into(),
            Self::Compact { item, count } if new.eq(item) => *count += 1,
            Self::Compact { item, count } => *self = Self::upgrade(item, count, new),
            Self::Many(acc) => acc.push(new),
        };
    }

    fn discard(&mut self) {
        *self = Self::default();
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Compact { .. } => false,
            Self::Many(acc) => acc.is_empty(),
        }
    }

    fn count(&self) -> u64 {
        match self {
            Self::Empty => u64::MIN,
            Self::Compact { count, .. } => *count,
            Self::Many(acc) => acc.count(),
        }
    }
}

/* ------------------------------------------------------------- Descriptor Trait Implementation */

impl<I> Descriptor for Vec<I>
where
    I: PartialOrd + Copy,
{
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        self.detail(buffer, count)
    }
}

impl Descriptor for BitVec {
    /// [`Detailed`][1] descriptor statistics are meaningless for `bool`: [`Buffer::Basic`][2]
    /// implies that both `true`/`max` and `false`/`min` items were accumulated.
    ///
    /// Refer to the [trait documentation](Descriptor::describe) for more information.
    ///
    /// [1]: manifest::Buffer::Detailed
    /// [2]: manifest::Buffer::Basic
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        Ok(manifest::Buffer::Basic { buffer, count })
    }
}

impl<I> Descriptor for OptInSitu<I>
where
    I: PartialOrd + Copy,
{
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        self.detail(buffer, count)
    }
}

impl<I> Descriptor for OptBitVec<I>
where
    I: Unfold,
    I::RawAcc: Extreme,
{
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        self.detail(buffer, count)
    }
}

impl Descriptor for OptBitVec<bool> {
    /// [`Detailed`][1] descriptor statistics are meaningless for `bool`: [`Buffer::Basic`][2]
    /// implies that both `true`/`max` and `false`/`min` items were accumulated.
    ///
    /// Refer to the [trait documentation](Descriptor::describe) for more information.
    ///
    /// [1]: manifest::Buffer::Detailed
    /// [2]: manifest::Buffer::Basic
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        Ok(manifest::Buffer::Basic { buffer, count })
    }
}

impl<I> Descriptor for Seq<I>
where
    I: Unfold,
{
    /// [Unsized][1] items do not currently support [`Detailed`][2] descriptor statistics.
    ///
    /// Refer to the [trait documentation](Descriptor::describe) for more information.
    ///
    /// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
    /// [2]: manifest::Buffer::Detailed
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        Ok(manifest::Buffer::Basic { buffer, count })
    }
}

impl<I> Descriptor for OptSeq<I>
where
    I: Unfold,
{
    /// [Unsized][1] items do not currently support [`Detailed`][2] descriptor statistics.
    ///
    /// Refer to the [trait documentation](Descriptor::describe) for more information.
    ///
    /// [1]: https://doc.rust-lang.org/reference/dynamically-sized-types.html
    /// [2]: manifest::Buffer::Detailed
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        Ok(manifest::Buffer::Basic { buffer, count })
    }
}

impl<A> Descriptor for Flatten<A>
where
    A: Descriptor,
{
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        self.0.describe(buffer, count)
    }
}

impl<I> Descriptor for Buffer<I>
where
    I: Unfold,
{
    /// Maps each state onto its descriptor variant.
    ///
    /// Refer to the [trait documentation](Descriptor::describe) for more information.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`] for an [`Empty`](Buffer::Empty) accumulator; empty buffers are never
    /// written to disk and must be caught before registration.
    fn describe(&self, buffer: Sector, count: NonZeroU64) -> Result<manifest::Buffer, Error> {
        match self {
            Buffer::Empty => Error::Zero.into(),
            Buffer::Compact { .. } => Ok(manifest::Buffer::Compact { buffer, count }),
            Buffer::Many(acc) => acc.describe(buffer, count),
        }
    }
}

/* --------------------------------------------------------------- Describe Trait Implementation */

impl<I> Describe<I> for Buffer<I>
where
    I: BitMatch + Clone + Unfold + 'static,
{
    fn boxed(&self) -> BoxAcc<I> {
        let buf = Self::default();
        Box::new(buf)
    }

    fn buffers(&self, offset: u64, columns: &mut Columns) -> Result<u64, schema::Error> {
        if let Some(column) = columns.next() {
            let sector = Sector {
                offset: offset.checked_add(SizedBuf::<I>::PREFIX).ok_or(Error::Zero)?,
                size: self.size()?,
            };
            let count = self.count().try_into().map_err(Error::from)?;
            let buf = self.describe(sector, count)?;
            column.buffers.push(buf);
            sector.next().ok_or(Error::Zero)?.align().map_err(Into::into)
        } else {
            schema::Error::NotFound.into() // expected column is not present
        }
    }
}

/* ----------------------------------------------------------- FromIterator Trait Implementation */

// NOTE: iterator collect builds a byte-identical accumulator to sequential Accumulate::push calls

impl<I> FromIterator<Vec<I>> for Seq<I>
where
    I: Unfold + 'static,
{
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = Vec<I>>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

impl FromIterator<String> for Seq<u8> {
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = String>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

impl<I> FromIterator<Option<I>> for OptBitVec<I>
where
    I: Unfold + 'static,
{
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = Option<I>>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

impl<I> FromIterator<Option<I>> for OptInSitu<I>
where
    Option<I>: Serialize,
    I: BitMatch + Copy + PartialOrd + Unfold + 'static,
{
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = Option<I>>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

impl<I> FromIterator<Option<Vec<I>>> for OptSeq<I>
where
    I: Unfold,
{
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = Option<Vec<I>>>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

impl FromIterator<Option<String>> for OptSeq<u8> {
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = Option<String>>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

impl<A, B> FromIterator<Option<Option<B>>> for Flatten<A>
where
    A: Accumulate<Option<B>> + Default + Serialize<Buffer = Vec<u8>> + 'static,
{
    fn from_iter<S>(src: S) -> Self
    where
        S: IntoIterator<Item = Option<Option<B>>>,
    {
        let mut acc = Self::default();
        src.into_iter().for_each(|item| acc.push(item));
        acc
    }
}

/* ------------------------------------------------------------------ Serialize Trait Definition */

/// A **type** that can be serialized into a canonical [`msca`](crate) binary representation for
/// on-disk storage.
#[doc(hidden)]
pub trait Serialize {
    /// The [`Buffer`] type returned by [`Self::serialize`].
    ///
    /// Fixed-size types can specify an appropriate array to leverage stack allocation. Unsized
    /// types should specify a heap-allocated buffer to accommodate dynamic sizing at runtime.
    type Buffer: io::Buffer;

    /// Returns the exact number of bytes required to encode `self`.
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
}

/* -------------------------------------------------------------- Serialize Trait Implementation */

/// Blanket [`&I`](I) implementation delegates to the referenced [`item`](I); allowing [`SizedBuf`]
/// to add a **length prefix** and **zero-filled alignment padding** without an intermediate copy.
impl<I> Serialize for &I
where
    I: Serialize + ?Sized,
{
    type Buffer = I::Buffer;

    fn size(&self) -> Result<NonZeroU64, Error> {
        I::size(self)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        I::serialize_into(self, buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        I::serialize(self)
    }

    fn extend(&self, sink: Vec<u8>) -> Result<Vec<u8>, Error> {
        I::extend(self, sink)
    }
}

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

impl Serialize for [u8] {
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        { self.len() as u64 }.try_into().map_err(Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let size = self.len();
        debug_assert!(buf.len() >= size, "actual size < buffer size");
        buf[..size].copy_from_slice(self);
        Ok(&mut buf[size..])
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        let buf = self.to_vec();
        Ok(buf)
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
        self.iter().try_fold(buf, |sink, element| element.serialize_into(sink))
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        use io::Buffer;
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
        self.iter().try_fold(buf, |sink, entry| {
            let sink = entry.0.serialize_into(sink)?;
            entry.1.serialize_into(sink)
        })
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        use io::Buffer;
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
        self.chunks(8).len().try_into().map(NonZeroU64::new)?.ok_or(Error::Zero)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        // Intermediate chunks contain 8 bits in Lsb0 order; the final chunk may contain ≤ 8 bits.
        // BitVec::load_le packs each chunk into one u8 in LE order, padding with zeros if the final
        // chunk is shorter than 8 bits. The resulting bytes are pushed into the provided buffer.
        self.chunks(8).try_fold(buf, |sink, bits| bits.load_le::<u8>().serialize_into(sink))
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        use io::Buffer;
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
        let mask = SizedBuf::new(&self.mask).size()?;
        match self.data.is_empty() {
            true => Ok(mask),
            false => SizedBuf::new(&self.data).size()?.checked_add(mask.get()).ok_or(Error::Zero),
        }
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let buf = SizedBuf::new(&self.mask).serialize_into(buf)?;
        match self.data.is_empty() {
            true => Ok(buf),
            false => SizedBuf::new(&self.data).serialize_into(buf),
        }
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        use io::Buffer;
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
        let ends = SizedBuf::new(&self.ends).size()?;
        match self.data.is_empty() {
            true => Ok(ends),
            false => SizedBuf::new(&self.data).size()?.checked_add(ends.get()).ok_or(Error::Zero),
        }
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let buf = SizedBuf::new(&self.ends).serialize_into(buf)?;
        match self.data.is_empty() {
            true => Ok(buf),
            false => SizedBuf::new(&self.data).serialize_into(buf),
        }
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        use io::Buffer;
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
        let ends = SizedBuf::new(&self.ends).size()?;
        match self.data.is_empty() {
            true => Ok(ends),
            false => SizedBuf::new(&self.data).size()?.checked_add(ends.get()).ok_or(Error::Zero),
        }
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let buf = SizedBuf::new(&self.ends).serialize_into(buf)?;
        match self.data.is_empty() {
            true => Ok(buf),
            false => SizedBuf::new(&self.data).serialize_into(buf),
        }
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        use io::Buffer;
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

impl<I> Serialize for Buffer<I>
where
    I: Unfold + Clone,
{
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        match self {
            Self::Empty => Error::Zero.into(),
            Self::Compact { item, .. } => I::once(item).size(),
            Self::Many(acc) => acc.size(),
        }
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        match self {
            Self::Empty => Err(Error::Zero),
            Self::Compact { item, .. } => I::once(item).serialize_into(buf),
            Self::Many(acc) => acc.serialize_into(buf),
        }
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        match self {
            Self::Empty => Err(Error::Zero),
            Self::Compact { item, .. } => I::once(item).serialize(),
            Self::Many(acc) => acc.serialize(),
        }
    }
}

impl<I> Segment for Accumulator<I> {
    const VARIANT: Variant = Variant::Data;

    fn wrap(&self, offset: u64) -> Result<Vec<u8>, Error> {
        use io::Buffer;
        const ADD: u64 = { Header::SIZE + size_of::<u64>() } as u64;
        let pad = offset.checked_add(Self::HEADER as u64).ok_or(Error::Zero)?.pad()?;
        let size = self.size()?.get().checked_add(pad as u64).ok_or(Error::Zero)?;
        let full = size.checked_add(ADD).ok_or(Error::Zero)?.try_into()?;
        let mut buf = vec![u8::MIN; full];
        let rem = buf
            .as_mut_slice()
            .serialize_push(&{ Self::VARIANT as u8 })?
            .serialize_push(&size)?
            .serialize_push(&self.schema.offset)?
            .serialize_push(&self.data.count())?;
        rem[..pad].fill(u8::MIN);
        self.data.serialize_into(&mut rem[pad..])?;
        Self::checksum(&mut buf)?;
        Ok(buf)
    }
}

impl<I> Checksum for Accumulator<I> {}

impl<I> Register for Accumulator<I> {
    type Error = schema::Error;

    fn register<'a>(self, s: &'a Sector, m: &mut Manifest) -> Result<&'a Sector, schema::Error> {
        let mut columns = m
            .schemas
            .get_mut(&self.name)
            // NOTE: Dataset::schema registers the schema before producing an Accumulator
            .ok_or(schema::Error::NotFound)?
            .columns
            .values_mut();
        let offset = s
            .offset
            .checked_add(Self::HEADER as u64)
            .ok_or(Error::Zero)?
            .align()?
            .checked_sub(HEADER as u64)
            .ok_or(Error::Zero)?;
        self.data.buffers(offset, &mut columns)?;
        Ok(s)
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
        let data: Vec<u16> = vec![1, 2, 3]; // 6 byte payload; excludes the length prefix
        let size = data.size().expect("Size failed");
        assert_eq!(size.get(), 6);
        assert_eq!(size.align().expect("Align failed"), 8);
    }

    /// [`SizedBuf`] frames the payload behind its length prefix and adds zero-bytes up to the
    /// next 64-bit boundary; an empty payload is a genuine error rather than a zero prefix.
    #[test]
    fn sized_buf_frames_payload() {
        let data: Vec<u16> = vec![1, 2, 3];
        let framed = SizedBuf::new(&data);
        assert_eq!(framed.size().expect("Size failed").get(), 16); // 8 prefix + 6 payload → 8
        let bytes = framed.serialize().expect("Serialize failed");
        assert_eq!(bytes[..8], 6u64.to_le_bytes()); // Length prefix excludes padding
        assert_eq!(bytes[14..], [u8::MIN; 2]); // Trailing bytes are zero-filled
        let none: Vec<u16> = Vec::new();
        assert!(SizedBuf::new(&none).size().is_err()); // Empty regions are omitted, never framed
    }

    /// [`OptBitVec`] aligns the value buffer to the boundary following the validity mask and stores
    /// only [`Some`] values in the concatenated payload.
    #[test]
    fn opt_bit_vec_layout() {
        let mut acc: OptBitVec<u32> = OptBitVec::default();
        [Some(1u32), None, Some(3)].into_iter().for_each(|v| acc.push(v));
        let bytes = acc.serialize().expect("Serialize failed");
        assert_eq!(bytes.len(), 32); // [mask 9 → 16][data 16]
        assert_eq!(acc.size().expect("Size failed").get(), 32); // Body excludes the outer prefix
        assert_eq!(SizedBuf::new(&acc).size().expect("Size failed").get(), 40); // Framed body
        assert_eq!(bytes[..8], 1u64.to_le_bytes()); // Mask length prefix records exact size
        assert_eq!(bytes[8], 0b101); // Mask bits in Lsb0 order
        assert_eq!(bytes[9..16], [u8::MIN; 7]); // Mask padding bytes are zero-filled
        assert_eq!(bytes[16..24], 8u64.to_le_bytes()); // Value length prefix excludes None rows
        assert_eq!(bytes[24..28], 1u32.to_le_bytes()); // Only Some values are stored, contiguously
        assert_eq!(bytes[28..32], 3u32.to_le_bytes());
    }

    /// An all-[`None`] [`OptBitVec`] omits the empty value sub-buffer entirely: the body carries
    /// only the framed validity mask and zero-length regions never reach the disk.
    #[test]
    fn opt_bit_vec_layout_all_none() {
        let mut acc: OptBitVec<u32> = OptBitVec::default();
        [None, None, None::<u32>].into_iter().for_each(|v| acc.push(v));
        let bytes = acc.serialize().expect("Serialize failed");
        assert_eq!(bytes.len(), 16); // [mask 9 → 16]; the empty data sub-buffer is omitted
        assert_eq!(bytes[..8], 1u64.to_le_bytes()); // Mask length prefix records exact size
        assert_eq!(bytes[8], 0b000); // Every row is None
        assert_eq!(bytes[9..], [u8::MIN; 7]); // Mask padding bytes are zero-filled
    }

    /// [`Seq`] offsets terminate on the boundary; data follows without intermediate padding.
    #[test]
    fn seq_layout() {
        let mut acc: Seq<u8> = Seq::default();
        acc.push(vec![97, 98, 99]);
        acc.push(vec![100, 101]);
        let bytes = acc.serialize().expect("Serialize failed");
        assert_eq!(bytes.len(), 40); // [offsets 24][data 13 → 16]
        assert_eq!(SizedBuf::new(&acc).size().expect("Size failed").get(), 48); // Framed body
        assert_eq!(bytes[..8], 16u64.to_le_bytes()); // Offsets length prefix records exact size
        assert_eq!(bytes[8..16], 3u64.to_le_bytes()); // Zero-based cumulative end of row 0
        assert_eq!(bytes[16..24], 5u64.to_le_bytes()); // Zero-based cumulative end of row 1
        assert_eq!(bytes[24..32], 5u64.to_le_bytes()); // Data length prefix records exact size
        assert_eq!(bytes[32..37], [97, 98, 99, 100, 101]);
        assert_eq!(bytes[37..40], [u8::MIN; 3]); // Data padding bytes are zero-filled
    }

    /// [`Accumulate::buffers`] records exact sector lengths and returns aligned offsets.
    #[test]
    fn buffers_align_offsets() {
        let data: Vec<u16> = vec![1, 2, 3];
        let mut col = Column::from(u16::with_unfolder::<Schema>());
        let next = data.buffers(0, &mut std::iter::once(&mut col)).expect("Buffers failed");
        assert_eq!(next, 16); // Next buffer begins at 64-bit alignment boundary
        let manifest::Buffer::Full { sector, .. } = &col.buffers[0] else {
            panic!("Buffer descriptor is not Full")
        };
        assert_eq!(sector.offset, 8); // Body starts after the header prefix
        assert_eq!(sector.length.get(), 6); // Body excludes the prefix and padding
    }

    /// [`OptBitVec`] records the data buffer at its aligned offset inside the composite region.
    #[test]
    fn opt_bit_vec_buffers_offset() {
        let mut acc: OptBitVec<u32> = OptBitVec::default();
        [Some(1u32), None, Some(3)].into_iter().for_each(|v| acc.push(v));
        let mut col = Column::from(u32::with_unfolder::<Schema>());
        let next = acc.buffers(0, &mut std::iter::once(&mut col)).expect("Buffers failed");
        assert_eq!(next, 40); // Aligned end of the composite region
        let manifest::Buffer::Full { sector, .. } = &col.buffers[0] else {
            panic!("Buffer descriptor is not Full")
        };
        assert_eq!(sector.offset, 8); // Whole body starts after the header prefix
        assert_eq!(sector.length.get(), 32); // Body spans the mask and data regions
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

    /// [`Compact`] counts repetitions of one value in place without materialising the inner
    /// accumulator.
    #[test]
    fn compact_counts_repetitions() {
        let mut acc: Compact<u32> = Compact::default();
        assert!(acc.is_empty());
        [5, 5, 5].into_iter().for_each(|v| acc.push(v));
        assert!(matches!(acc, Compact::Lite { count: 3, .. }));
        assert!(!acc.is_empty());
        assert_eq!(acc.count(), 3);
    }

    /// A [`Lite`](Compact::Lite) column serializes as a **one-row** compact body regardless of the
    /// repetition count.
    #[test]
    fn compact_lite_serializes_one_row() {
        let mut acc: Compact<u32> = Compact::default();
        [5, 5, 5].into_iter().for_each(|v| acc.push(v));
        let one = vec![5u32].serialize().expect("Serialize failed");
        assert_eq!(acc.size().expect("Size failed").get(), 4);
        assert_eq!(acc.serialize().expect("Serialize failed"), one);
    }

    /// The first differing push collects the repeated run into a materialised
    /// [`Full`](Compact::Full) state that is byte-identical to a hand-built inner accumulator.
    #[test]
    fn compact_materialises_full() {
        let mut acc: Compact<u32> = Compact::default();
        [5, 5, 5, 7].into_iter().for_each(|v| acc.push(v));
        assert!(matches!(acc, Compact::Full(..)));
        assert_eq!(acc.count(), 4);
        let full = vec![5u32, 5, 5, 7].serialize().expect("Serialize failed");
        assert_eq!(acc.serialize().expect("Serialize failed"), full);
    }

    /// An all-[`None`] optional column stays [`Lite`](Compact::Lite) and serializes as a one-row
    /// mask-only body; the empty data sub-buffer is omitted entirely.
    #[test]
    fn compact_all_none_lite_body() {
        let mut acc: Compact<Option<u32>> = Compact::default();
        [None, None, None::<u32>].into_iter().for_each(|v| acc.push(v));
        assert!(matches!(acc, Compact::Lite { count: 3, .. }));
        let bytes = acc.serialize().expect("Serialize failed");
        assert_eq!(bytes.len(), 16); // [mask 9 → 16]; one None row, data omitted
        assert_eq!(bytes[..8], 1u64.to_le_bytes()); // Mask length prefix records exact size
        assert_eq!(bytes[8], 0b0); // The single row is None
    }

    /// [`Unfold::same`] compares the exact bit pattern: a repeated [`f64::NAN`] niche column stays
    /// [`Lite`](Compact::Lite), while a differing bit pattern materialises
    /// [`Full`](Compact::Full).
    #[test]
    fn compact_float_bits_drive_state() {
        let mut nan: Compact<f64> = Compact::default();
        [f64::NAN, f64::NAN].into_iter().for_each(|v| nan.push(v));
        assert!(matches!(nan, Compact::Lite { count: 2, .. }));
        let mut inf: Compact<f64> = Compact::default();
        [f64::INFINITY, f64::INFINITY].into_iter().for_each(|v| inf.push(v));
        assert!(matches!(inf, Compact::Lite { count: 2, .. }));
        inf.push(f64::NEG_INFINITY);
        assert!(matches!(inf, Compact::Full(..)));
    }

    /// [`Accumulate::discard`] returns the column to the [`Empty`](Compact::Empty) state.
    #[test]
    fn compact_discard_resets_empty() {
        let mut acc: Compact<u32> = Compact::default();
        [5, 7].into_iter().for_each(|v| acc.push(v));
        assert!(matches!(acc, Compact::Full(..)));
        acc.discard();
        assert!(matches!(acc, Compact::Empty));
        assert!(acc.is_empty());
    }

    /// The single [`Lite`](Compact::Lite) value serves as both the minimum and maximum statistic.
    #[test]
    fn compact_min_max_lite() {
        let mut acc: Compact<u32> = Compact::default();
        [5, 5].into_iter().for_each(|v| acc.push(v));
        assert_eq!(Accumulate::min(&acc), Some(5));
        assert_eq!(Accumulate::max(&acc), Some(5));
    }

    /// [`Compact::buffers`] registers a [`Buffer::Lite`](manifest::Buffer::Lite) descriptor whose
    /// sector spans the one-row body; materialising emits [`Buffer::Full`](manifest::Buffer::Full)
    /// instead.
    #[test]
    fn compact_buffers_emit_lite() {
        let mut acc: Compact<u16> = Compact::default();
        [7, 7, 7].into_iter().for_each(|v| acc.push(v));
        let mut col = Column::from(u16::with_unfolder::<Schema>());
        let next = acc.buffers(0, &mut std::iter::once(&mut col)).expect("Buffers failed");
        assert_eq!(next, 16); // Aligned end of the one-row compact body
        let manifest::Buffer::Lite { sector, count } = &col.buffers[0] else {
            panic!("Buffer descriptor is not Lite")
        };
        assert_eq!(sector.offset, 8); // Body starts after the header prefix
        assert_eq!(sector.length.get(), 2); // Body spans exactly one serialized u16
        assert_eq!(count.get(), 3); // Repetition count spans every accumulated row
        acc.push(9); // Materialise the inner accumulator
        let mut col = Column::from(u16::with_unfolder::<Schema>());
        acc.buffers(0, &mut std::iter::once(&mut col)).expect("Buffers failed");
        assert!(matches!(col.buffers[0], manifest::Buffer::Full { .. }));
    }
}
