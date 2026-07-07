/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

#![doc = include_str!("../../doc/manifest.md")]

use std::collections::{BTreeMap, Bound};
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use minicbor::{CborLen, Decode, Encode};
use smol::io::{AsyncRead, AsyncReadExt, AsyncSeek};

use crate::io::{Deserializer, Header, Write};
use crate::schema::number::Error;
use crate::schema::Type;
use crate::{io, Deserialize, Sector, Serialize};

/* ------------------------------------------------------------------------------ Public Exports */

/// Size of each serialized [`Buffer`] statistic in bytes; determined by the largest supported type.
pub(crate) const B: usize = size_of::<u128>();

/// Manifest of file segments and accompanying metadata for random access and predicate pruning.
/// See the [module-level documentation](self) for details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[cbor(tag(100))]
pub(crate) struct Manifest {
    /// [`Schema`] segments keyed by [`name`](String).
    #[cbor(n(0), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub schemas: BTreeMap<String, Schema>,
    /// [`Dictionary`] segments keyed by [`name`](String).
    #[cfg(feature = "dictionary")]
    #[cbor(n(1), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub dictionaries: BTreeMap<String, Dictionary>,
    /// [`Index`] segments keyed by [`name`](String).
    #[cfg(feature = "index")]
    #[cbor(n(2), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub indexes: BTreeMap<String, Index>,
    /// Implementers can use the optional free-form `metadata.toml` to attach file-level
    /// domain-specific information such as:
    ///
    /// - Date and time
    /// - Experimental parameters
    /// - Provenance
    ///
    /// If a metadata section is included in the file, a corresponding `length` and `offset` are
    /// described in the `manifest`. The core library includes a read and write surface, but
    /// implementers must include their own metadata parsing and validation logic.
    #[cfg(feature = "metadata")]
    #[cbor(n(3), skip_if = "Option::is_none")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub metadata: Option<Sector>,
}

impl Manifest {
    /// [`Deserialize`] a file [`Manifest`] from the provided [`File`](AsyncRead) at the specified
    /// [`Sector`], verifying the segment framing recorded by the [write-cycle](io).
    ///
    /// ### Errors
    ///
    /// - [`Error::Truncated`][1] if the sector length is too small to contain a segment [`Header`].
    /// - [`Error::Checksum`][2] if computed checksum does not match the on-disk checksum suffix.
    /// - [`Error::Decode`][3] from the underlying manifest [`CBOR`](minicbor) decode operation.
    /// - [`Error::Io`][4] from the underlying [`seek`][5] and [`read`][6] operations.
    ///
    /// [1]: io::Error::Truncated
    /// [2]: io::Error::Checksum
    /// [3]: io::Error::Decode
    /// [4]: io::Error::Io
    /// [5]: Sector::seek_to_start
    /// [6]: AsyncReadExt::read_exact
    pub async fn from_file<F>(file: &mut F, sector: Sector) -> Result<Self, io::Error>
    where
        F: AsyncRead + AsyncSeek + Unpin + ?Sized,
    {
        let size = sector.length.get().try_into()?;
        let mut buf = vec![0u8; size];
        sector.seek_to_start(file).await?;
        file.read_exact(&mut buf).await?;
        Manifest::verify(&buf)?
            .get(Header::SIZE..)
            .ok_or_else(|| io::Error::Truncated {
                expected: Header::SIZE,
                actual: buf.len(),
            })?
            .deserialize_into()
    }

    /// Reconstruct a [`Manifest`] by walking the self-describing segment region.
    ///
    /// Used to recover a corrupt or truncated manifest by replaying intact segments. Each segment
    /// header is decoded sequentially and re-registered in a fresh [`Manifest`].
    pub fn rebuild(data: &[u8], tail: NonZeroU64) -> Self {
        unimplemented!("Manifest::rebuild is not yet implemented")
    }
}

/// [`Write`] [`Context`](Write::Ctx) for the [`Manifest`]; carries the file [`Header`] and
/// [`size`][1] of the incoming [`Segment`][2].
///
/// The new manifest is written prior to the incoming segment at an offset that preserves sufficient
/// space without overwriting the existing on-disk manifest.
///
/// ```text
///                                      Incoming Segment Sector
///                                     ├───────────────────────┤
/// [Header] [Segment 0] ... [Segment N] ... [Prev Manifest] ... [New Manifest]
///                                tail ↑   ↑ manifest.offset
/// ```
///
/// Refer to the [write-cycle](io) documentation for more details.
///
/// [1]: crate::segment::Segment::size
/// [2]: crate::segment::Segment
pub(crate) struct Pending<'a> {
    /// File [`Header`] reference used to read the current `tail` and `manifest` sectors.
    pub header: &'a Header,
    /// Total [`size`][1] of the incoming [`Segment`][2] in bytes.
    ///
    /// [1]: crate::segment::Segment::size
    /// [2]: crate::segment::Segment
    pub size: NonZeroU64,
}

impl Write for Manifest {
    type Ctx<'a> = Pending<'a>;

    /// Returns a suitable [`Sector`] to write the updated [`Manifest`].
    ///
    /// 1. Reserves space for the incoming [`Segment`]
    /// 2. Does not overwrite the existing manifest
    ///
    /// This function is purely predictive; no file IO is executed.
    ///
    /// ```text
    /// [Header] [Segment 0] ... [Segment N] [New Segment] ... [New Manifest]
    ///                                tail ↑                 ↑ manifest.offset
    /// ```
    ///
    /// Refer to the [write-cycle](io) documentation for more details.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`](Error::Zero) if a `u64` overflow occurs while calculating
    /// [`size`](NonZeroU64) or [`offset`](NonZeroU64) for the relevant file regions.
    fn sector(&self, pending: Pending) -> Result<Sector, Error> {
        let offset = match pending.header.manifest.length < pending.size {
            true => pending.header.tail.checked_add(pending.size.get()),
            false => pending.header.manifest.next(),
        }
        .ok_or(Error::Zero)?
        .get();
        Ok(Sector { offset, length: self.size()? })
    }
}

impl Serialize for Manifest {
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, Error> {
        let size: u64 = minicbor::len(self).try_into()?;
        size.try_into().map_err(Error::Convert)
    }

    fn serialize_into<'a>(&self, mut buf: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        // SAFETY: minicbor::encode is infallible when writing to &mut [u8]
        minicbor::encode(self, &mut buf).expect("Infallible manifest CBOR encode failed");
        Ok(buf)
    }

    fn serialize(&self) -> Result<Self::Buffer, Error> {
        // NOTE: Scoped trait import avoids namespace conflict with Buffer struct (below)
        use crate::accumulate::Buffer;
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

impl<'de> Deserialize<'de> for Manifest {
    type Ok = Self;

    fn deserialize(src: &mut &'de [u8]) -> Result<Self, io::Error> {
        // NOTE: one-shot decode from a pre-sized CBOR buffer; the slice is not advanced.
        minicbor::decode(src).map_err(io::Error::Decode)
    }
}

/// A minimal schema segment **descriptor** that specifies:
///
/// 1. [`Sector`] where the schema segment is located on disk.
/// 2. [`BTreeMap`] of [`Column`] descriptors keyed by name.
///
/// This type does **not** contain the actual schema definition or columnar data buffers; it is a
/// lightweight descriptor for segment discovery and access without holding buffer contents in
/// memory. An on-disk schema segment encodes the schema definition (column names and types) while
/// on-disk data segments contain the columnar buffers.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub struct Schema {
    /// Location of the [`Schema`] segment.
    #[n(0)]
    pub sector: Sector,
    /// [`Column`] descriptors keyed by name.
    ///
    /// The [`BTreeMap`] guarantees a stable deterministic column order for consistent binary
    /// encoding and schema comparison.
    #[cbor(n(1), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
}

/// A minimal column **descriptor** wrapping a collection of [`Buffer`] descriptors.
///
/// This type does **not** contain the actual buffer data; it is a lightweight descriptor for column
/// discovery and access without holding buffer contents in memory. Data is stored via one or more
/// on-disk data segments, each of which contains a buffer for this column.
///
/// [`Vec`] order in-memory is **not** guaranteed to reflect [`Sector`] order on-disk.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[doc(hidden)] // Reachable through Accumulate::buffers for the #[derive(Data)] macro.
pub struct Column {
    /// The [`Type`] of values contained within this column.
    #[n(0)]
    pub ty: Type,
    /// List of [`Buffer`] descriptors for this column across all data segments.
    #[cbor(n(1), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub buffers: Vec<Buffer>,
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.ty == other.ty
    }
}

impl From<Type> for Column {
    fn from(ty: Type) -> Self {
        Column { ty, buffers: Vec::new() }
    }
}

/// A minimal columnar buffer **descriptor** that specifies:
///
/// 1. [`Sector`] where the buffer is located on disk.
/// 2. Number of data entries e.g. for index arithmetic.
/// 3. Buffer statistics such as `min` and `max` for predicate pruning.
///
/// This type does **not** contain the actual buffer data; it is a lightweight descriptor for buffer
/// discovery and access without holding buffer contents in memory. Data is stored via contiguous
/// buffers distributed across one or more on-disk data segments.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[doc(hidden)] // Reachable through Accumulate::buffers for the #[derive(Data)] macro.
pub enum Buffer {
    /// A buffer containing **more than one** distinct item.
    #[n(0)]
    Full {
        /// Location of the [`Buffer`] on disk.
        ///
        /// Sector `offset` is calculated relative to the immutable segment region, excluding the
        /// [file](io::File) [header](io::Header). Refer to the [write-cycle](self) documentation
        /// for more details.
        #[n(0)]
        sector: Sector,
        /// Number of data entries.
        ///
        /// Empty buffers are never written to disk; this invariant is enforced by [`NonZeroU64`].
        #[n(1)]
        count: NonZeroU64,
        /// Minimum value recorded in this buffer; used for segment-level predicate pruning.
        ///
        /// [Serialized](Serialize) LE bytes into a fixed-size array with trailing zeros.
        /// [Deserialize] according to the [`Type`] specified by the [`Schema`]. Defaults to unset
        /// bits if no orderable statistic is available e.g. for non-orderable types.
        #[cbor(n(2), with = "minicbor::bytes")]
        min: [u8; B],
        /// Maximum value recorded in this buffer; used for segment-level predicate pruning.
        ///
        /// [Serialized](Serialize) LE bytes into a fixed-size array with trailing zeros.
        /// [Deserialize] according to the [`Type`] specified by the [`Schema`]. Defaults to set
        /// bits if no orderable statistic is available e.g. for non-orderable types.
        #[cbor(n(3), with = "minicbor::bytes")]
        max: [u8; B],
    },
    /// A compact buffer containing exactly **one** item repeated `count` times.
    #[n(1)]
    Lite {
        /// Location of the [`Buffer`] on disk.
        ///
        /// Sector `offset` is calculated relative to the immutable segment region, excluding the
        /// [file](io::File) [header](io::Header). Refer to the [write-cycle](self) documentation
        /// for more details.
        #[n(0)]
        sector: Sector,
        /// Logical number of repetitions of the single [Serialized](Serialize) item.
        ///
        /// Empty buffers are never written to disk; this invariant is enforced by [`NonZeroU64`].
        #[n(1)]
        count: NonZeroU64,
    },
}

impl Buffer {
    /// Returns `true` if [`self`](Buffer) is provably disjoint from the specified [`Range`].
    ///
    /// ### ⚠️ Safety
    ///
    /// This function is marked as [unsafe][1] due to the potential for undefined behaviour if the
    /// requested type [`I`] does not match the actual [`Column`](Column) [`Type`].
    ///
    /// [1]: https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html
    pub(crate) unsafe fn disjoint<I, B>(&self, bounds: &B) -> Result<bool, io::Error>
    where
        B: RangeBounds<I>,
        I: for<'de> Deserialize<'de, Ok = I> + PartialOrd,
    {
        let min: I = self.min.as_slice().deserialize_into()?;
        let max: I = self.max.as_slice().deserialize_into()?;
        let above = match bounds.end_bound() {
            Bound::Included(v) => &min > v,
            Bound::Excluded(v) => &min >= v,
            Bound::Unbounded => false,
        };
        let below = match bounds.start_bound() {
            Bound::Included(v) => &max < v,
            Bound::Excluded(v) => &max <= v,
            Bound::Unbounded => false,
        };
        Ok(above || below)
    }
}

/// A minimal dictionary **descriptor** that specifies:
///
/// 1. [`Sector`] of the corresponding [`Schema`] segment.
/// 2. [`BTreeMap`] of [`Column`] descriptors keyed by [`name`](String).
///
/// This type does **not** contain the actual schema definition or columnar data buffers; it is a
/// lightweight descriptor for segment discovery and access without holding buffer contents in
/// memory. An on-disk schema segment encodes the schema definition (column names and types) while
/// on-disk data segments contain the columnar buffers.
#[cfg(feature = "dictionary")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Dictionary {
    /// Location of the [`Schema`] segment.
    #[n(0)]
    pub schema: Sector,
    /// [`Column`] descriptors keyed by [`name`](String).
    #[cbor(n(1), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
}

#[cfg(feature = "dictionary")]
impl Dictionary {
    /// Returns a reference to the [`key`](String) [`Column`] for this dictionary.
    pub fn key(&self) -> &Column {
        // SAFETY: Dictionaries are guaranteed to contain a "key" column:
        // 1. Serializer enforces a key-value layout during dictionary initialisation.
        // 2. Deserializer rejects schemas that do not contain a "key" column.
        self.columns.get("key").expect("Dictionary does not contain a 'key' column")
    }
}

/// A minimal dictionary index **descriptor** that specifies:
///
/// 1. Underlying [`Dictionary`] descriptor.
/// 2. Next available `key` for appending new entries to the dictionary.
///
/// This type does **not** contain the actual dictionary entries; it is a lightweight descriptor for
/// index discovery and access without holding buffer contents in memory. An on-disk schema segment
/// encodes the schema definition (column names and types) while on-disk data segments contain the
/// columnar buffers.
#[cfg(feature = "index")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Index {
    /// Underlying [`Dictionary`] descriptor.
    #[n(0)]
    pub dictionary: Dictionary,
    /// Next available key.
    ///
    /// Data is stored via an arbitrary-length [`Vec`] containing raw bytes encoded in
    /// platform-native endianness. Decode according to the `Key` type described by the schema.
    #[n(1)]
    pub next: Vec<u8>,
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    /// Both [`Buffer`] variants round-trip through their tagged CBOR representation.
    #[test]
    fn buffer_cbor_round_trips() {
        let sector = Sector::new(8u64, 16u64).expect("Sector::new failed");
        let count = NonZeroU64::new(3).expect("Count is zero");
        let full = Buffer::Full {
            sector,
            count,
            min: [u8::MIN; B],
            max: [u8::MAX; B],
        };
        let lite = Buffer::Lite { sector, count };
        for buf in [full, lite] {
            let mut bytes = vec![u8::MIN; minicbor::len(&buf)];
            let mut sink = bytes.as_mut_slice();
            // SAFETY: minicbor::encode is infallible when writing to &mut [u8]
            minicbor::encode(&buf, &mut sink).expect("Infallible buffer CBOR encode failed");
            let out: Buffer = minicbor::decode(&bytes).expect("Buffer CBOR decode failed");
            assert_eq!(out, buf);
        }
    }

    /// [`Lite`](Buffer::Lite) descriptors carry no statistics and are never provably disjoint.
    #[test]
    fn lite_never_disjoint() {
        let sector = Sector::new(8u64, 16u64).expect("Sector::new failed");
        let count = NonZeroU64::new(3).expect("Count is zero");
        let lite = Buffer::Lite { sector, count };
        // SAFETY: Lite descriptors return before any type-dependent statistic is deserialized
        let disjoint = unsafe { lite.disjoint(&(10u32..20)) }.expect("Disjoint failed");
        assert!(!disjoint);
    }
}
