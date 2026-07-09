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

use crate::io::{Checksum, Deserializer};
use crate::schema::number::Error;
use crate::schema::Type;
use crate::segment::{Header, Segment, Variant};
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
    #[cbor(n(1), skip_if = "Option::is_none")]
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

impl Segment for Manifest {
    const VARIANT: Variant = Variant::Manifest;
}

impl Checksum for Manifest {}

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

impl Schema {
    /// Returns the total number of items across every [`Segment`] for this [`Schema`].
    ///
    /// Calculated from the [`Manifest`] via the summation (Σ) of [`Buffer::count`] for one
    /// [`Column`] – since all columns in a single segment contain the same number of logical items.
    pub(crate) fn count(&self) -> u64 {
        self.columns
            .values()
            .next()
            .into_iter()
            .flat_map(|column| &column.buffers)
            .map(Buffer::count)
            .sum()
    }
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
/// 2. Logical number of data entries e.g. for index arithmetic.
/// 3. Statistics such as `min` and `max` for predicate pruning.
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
    /// Returns the logical number of items recorded in [`self`](Buffer).
    pub(crate) const fn count(&self) -> u64 {
        match self {
            Buffer::Full { count, .. } | Buffer::Lite { count, .. } => count.get(),
        }
    }

    /// Returns `true` if [`self`](Buffer) is provably disjoint from the specified [`Range`].
    ///
    /// - [`Buffer::Full`] are evaluated using `min` and `max` statistics.
    /// - [`Buffer::Lite`] do not carry statistics and therefore never provably disjoint
    ///
    /// Compact buffers always return `false` from this function; the repeated value is instead
    /// [evaluated][2] **exactly once** during [`Stream`](crate::Stream) initialisation.
    ///
    /// ### ⚠️ Safety
    ///
    /// This function is marked as [unsafe][1] due to the potential for undefined behaviour if the
    /// requested type [`I`] does not match the actual [`Column`](Column) [`Type`].
    ///
    /// [1]: https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html
    /// [2]: crate::query::Evaluate
    pub(crate) unsafe fn disjoint<I, B>(&self, bounds: &B) -> Result<bool, io::Error>
    where
        B: RangeBounds<I>,
        I: for<'de> Deserialize<'de, Ok = I> + PartialOrd,
    {
        let (min, max) = match self {
            Buffer::Full { min, max, .. } => (min, max),
            Buffer::Lite { .. } => return Ok(false), // no statistics to evaluate
        };
        let min: I = min.as_slice().deserialize_into()?;
        let max: I = max.as_slice().deserialize_into()?;
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

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    /// A manifest segment round-trips: [`frame`](Segment::wrap) then [`verify`](Checksum::verify)
    /// the checksum and [`deserialize`](Deserialize::deserialize) the payload to recover the
    /// original [`Manifest`].
    #[test]
    fn manifest_segment_round_trips() {
        let manifest = Manifest::default();
        let bytes = manifest.wrap(0).expect("Frame failed");
        let region = Manifest::verify(&bytes).expect("Checksum failed");
        let out = Manifest::deserialize(&mut &region[Header::SIZE..]).expect("Deserialize failed");
        assert_eq!(out, manifest);
    }

    /// Corrupting any framed byte is detected by [`verify`](Checksum::verify) as
    /// [`io::Error::Checksum`].
    #[test]
    fn manifest_checksum_detects_corruption() {
        let mut bytes = Manifest::default().wrap(0).expect("Frame failed");
        bytes[Header::SIZE] ^= u8::MAX; // Flip the first payload byte
        let err = Manifest::verify(&bytes).expect_err("Corruption undetected");
        assert!(matches!(err, io::Error::Checksum));
    }

    /// A region shorter than one trailing checksum is rejected with [`io::Error::Truncated`].
    #[test]
    fn manifest_verify_rejects_short_region() {
        let err = Manifest::verify([u8::MIN; 4].as_slice()).expect_err("Short region accepted");
        assert!(matches!(err, io::Error::Truncated { .. }));
    }

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
