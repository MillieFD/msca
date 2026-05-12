/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Low-level IO for reading and writing files.
//!
//! ---
//!
//! [`clem`](crate) maximises IO performance by separating the data lifecycle into two phases:
//!
//! 1. **In-memory** accumulator optimised for high-throughput ingestion.
//! 2. **On-disk** columnar buffers optimised for range-based querying across arbitrary dimensions.
//!
//! This module coordinates the transition between memory and disk phases to ensure data durability
//! and efficient access patterns suitable for edge deployment on resource-constrained hardware.
//! The on-disk layout minimises contention for multiple-producer multiple-consumer workflows.
//!
//! ### Segment Composition
//!
//! Each file is partitioned into self-describing [segments](Segment) which are immutable once
//! written. Each segment begins with a minimal header consisting of a [`variant`][1] identifier
//! and [`length`](NonZeroU64).
//!
//! - [`Schema`] segments describe the structure of encoded data.
//! - `Data` segments carry columnar buffers for a specified schema instance.
//!
//! Multimodality and schema evolution are realised by appending additional schema segments. Data
//! storage and file extensibility are realised by appending additional data segments. Format
//! extensibility may be achieved via the introduction of new segment variants in future releases.
//!
//! ### Lazy Partial Reads
//!
//! On-disk data is represented using a [`Sector`] instance prior to file IO. This design ensures:
//!
//! - **O(1) Random Access:** Readers `seek` directly to the relevant file region.
//! - **Efficient:** Readers `take` the required number of bytes instead of loading the entire file.
//!
//! Passing a small `Sector` instance can reduce overhead compared to passing an owned data buffer.
//! Sectors enforce the immutability of underlying on-disk data; implementers must [`Copy`] into an
//! owned type when mutability is required e.g. for downstream data processing.
//!
//! ### Manifest
//!
//! A [`Manifest`] footer lists file segments by type. Data segments are grouped by [`Schema`]
//! alongside segment-level statistics e.g. min and max values. The manifest acts like the index of
//! a book to enhance segment discovery and random access.
//!
//! The manifest is encoded as **CBOR** and written after the final data segment. A [`BTreeMap`][2]
//! sorted in lexicographic order ensures the layout is deterministic regardless of insertion order.
//!
//! ### Metadata
//!
//! An optional free-form `metadata` [`Sector`] may be written after the [`Manifest`] where
//! implementers can attach file-level domain-specific information such as:
//!
//! - Date and time
//! - Experimental parameters
//! - Provenance
//!
//! The [`Manifest`] may include an optional `metadata` field which points to this [`Sector`]. The
//! file IO mechanisms defined in this module will always preserve metadata and update the
//! [`Manifest`] metadata field during the write-cycle if present, but will only provide a read and
//! write surface if the corresponding metadata feature is enabled. Implementers must include their
//! own metadata parsing and validation logic.
//!
//! ### File Header
//!
//! The file header begins with a magic byte sequence used to identify the file type. The file IO
//! mechanisms defined in this module will reject incorrect magic byte sequences. Implementers may
//! prepend their own file header – e.g. to indicate a specific file type built atop `clem` with a
//! canonical schema – but must remove the prepended data before passing to the underlying reader.
//!
//! ```text
//! File
//! ├─ Header
//! │  ├─ magic: [u8; 4] // b"clem"
//! │  ├─ version: u8
//! │  ├─ tail: NonZeroU64
//! │  └─ manifest: Sector
//! ├─ Segment 0
//! ⋮
//! ├─ Segment N
//! ├─ Empty (optional)
//! ├─ Manifest
//! └─ Metadata (optional)
//! ```
//!
//! A major version number is embedded in the file header to indicate breaking changes in the format
//! specification. Forwards and backwards compatibility across version numbers is not guaranteed.
//! Implementers must reject any file with an unrecognised version number.
//!
//! ```text
//! [Header] [Segment 0] ... [Segment N] ... [Manifest] [Metadata]
//!                                tail ↑   ↑ manifest.offset
//! ```
//!
//! The [`tail`](NonZeroU64) field records the byte offset immediately following the final committed
//! segment. New segments are always appended from `tail`, not from EOF. An empty region may exist
//! between `tail` and the start of the manifest when appending segments that are shorter than the
//! combined manifest and metadata. This empty region is filled during the next write-cycle.
//!
//! [1]: crate::segment::Variant
//! [2]: std::collections::BTreeMap

#![doc = include_str!("../docs/write-cycle.md")]
#![doc = include_str!("../docs/read-cycle.md")]

use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;
use std::io::SeekFrom;
use std::num::{NonZeroU64, NonZeroUsize, TryFromIntError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::{Mmap, MmapOptions};
use minicbor::{CborLen, Decode, Encode};
use smol::fs::OpenOptions;
use smol::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use smol::lock::RwLock;

use crate::manifest::Manifest;
use crate::segment::Segment;
use crate::{Record, Serialize, accumulate, manifest, schema};

/* ------------------------------------------------------------------------------ Public Exports */

type BufWriter = smol::io::BufWriter<smol::fs::File>;

/// Magic byte sequence used to identify a valid [`clem`](crate) file.
const MAGIC: [u8; 4] = *b"clem";

/// Current [`clem`](crate) major version number which is embedded in the file header to indicate
/// breaking changes in the format specification. Forwards and backwards compatibility across
/// version numbers is not guaranteed. Implementers must reject any unrecognised version number.
const VERSION: u8 = 1;

/// Creates a read-only [memory map](Mmap) backed by the specified [clem](crate) file.
///
/// ### Errors
///
/// [`Error::Zero`] if the [`Header`](HEADER) size exceeds [`u64::MAX`].
// todo → Static assert HEADER size as u64, remove try_into runtime checks, use faster unchecked fn.
///
/// [`Error::Io`] if the underlying system call fails. This can occur for a variety of reasons,
/// such as the file is no longer accessible, or the platform does not support memory mapping.
///
/// ### Safety
///
/// This function is marked as [unsafe][1] because of the potential for undefined behaviour if the
/// underlying file region is subsequently modified, in or out of process. Implementers are strongly
/// advised to take appropriate precautions and ensure the mapped region is not accessed or modified
/// concurrently in a way that causes undefined behaviour.
///
/// [`Segments`](Segment) are immutable once written. The [`Mmap`] is tightly scoped to reduce the
/// risk of undefined behaviour:
///
/// - [`offset`](MmapOptions::offset) excludes the mutable [`Header`]
/// - [`length`](MmapOptions::len) excludes the mutable [`Manifest`]
/// - Only the immutable segment region is mapped
///
/// Appending a new segment updates the [`Arc`]`<`[`Mmap`](Mmap)`>` after the [write-cycle](self) is
/// complete. New readers must await a [read lock](RwLock) on the [file state](File) before cloning
/// the [`Arc`]. Existing mmaps are released only when their reference count drops to zero.
/// In-flight reader mmaps remain valid (existing segments unaltered).
///
/// Refer to the [memmap](memmap2) crate for more details.
///
/// [1]: https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html
unsafe fn mmap(file: &smol::fs::File, length: usize) -> Result<Mmap, Error> {
    let offset: u64 = Header::SECTOR.offset.get();
    // SAFETY: Undefined behaviour if mapped file is modified (refer to function documentation).
    unsafe { MmapOptions::new().offset(offset).len(length).map(file).map_err(Error::Io) }
}

/// A contiguous byte region within the [`clem`](crate) file.
///
/// Implementers must [`Copy`] into an owned type when mutability is required e.g. for downstream
/// data processing.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Hash, Encode, Decode, CborLen)]
pub struct Sector {
    /// Byte offset to the start of the sector.
    #[n(0)]
    pub offset: NonZeroU64,
    /// Total length of the sector in bytes.
    #[n(1)]
    pub length: NonZeroU64,
}

impl Sector {
    pub fn new<A, B>(offset: A, length: B) -> Result<Self, Error>
    where
        A: TryInto<NonZeroU64>,
        B: TryInto<NonZeroU64>,
        Error: From<A::Error> + From<B::Error>,
    {
        Ok(Self {
            offset: offset.try_into()?,
            length: length.try_into()?,
        })
    }

    /// Returns the offset immediately following [`self`](Sector), or [`None`] on `u64` overflow.
    pub fn next(&self) -> Option<NonZeroU64> {
        let length = self.length.get();
        self.offset.checked_add(length)
    }
}

impl Ord for Sector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl Serialize for Sector {
    type Buffer = [u8; size_of::<Self>()];

    fn serialize_into(&self, buf: &mut [u8]) {
        buf[..size_of::<NonZeroU64>()].copy_from_slice(self.offset.get().to_be_bytes().as_ref());
        buf[size_of::<NonZeroU64>()..].copy_from_slice(self.length.get().to_be_bytes().as_ref());
    }

    fn serialize(&self) -> Result<Self::Buffer, accumulate::Error> {
        let mut buf = [u8::MIN; size_of::<Self>()];
        self.serialize_into(&mut buf);
        Ok(buf)
    }
}

/// Mutable region of the file header.
///
/// Excludes immutable header elements such as the [magic bytes][1] and [version number][2]. See the
/// [module documentation](self) for a detailed description of the file header layout.
///
/// [1]: MAGIC
/// [2]: VERSION
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Header {
    /// Byte offset immediately following the last committed [`Segment`].
    #[n(0)]
    pub tail: NonZeroU64,
    /// On-disk location of the encoded [`Manifest`].
    #[n(1)]
    pub manifest: Sector,
}

impl Header {
    /// Total length of the file header in bytes. Includes the [magic bytes][1] and [version number][2].
    ///
    /// [1]: MAGIC
    /// [2]: VERSION
    pub const SIZE: NonZeroUsize = {
        let size = size_of_val(&MAGIC) + size_of_val(&VERSION) + size_of::<Header>();
        // SAFETY: Const fn can panic at compile time. Value is guaranteed at runtime.
        NonZeroUsize::new(size).expect("Header size is zero")
    };

    /// todo → const doc comment
    pub const SECTOR: Sector = {
        let offset = { size_of_val(&MAGIC) + size_of_val(&VERSION) } as u64;
        let length = Self::SIZE.get() as u64;
        Sector {
            offset: NonZeroU64::new(offset).expect("Header offset is zero"),
            length: NonZeroU64::new(length).expect("Header length is zero"),
        }
    };

    /// Create a new [clem](crate) file [`Header`] pointing to the provided manifest [`Sector`].
    ///
    /// ```text
    /// [Header] [Manifest]
    ///         ↑ tail & manifest.offset
    /// ```
    ///
    /// The `tail` and `manifest.offset` pointers are guaranteed to align exactly.
    fn new(manifest: Sector) -> Self {
        Self { tail: manifest.offset, manifest }
    }

    /// [`Deserialize`] the file [`Header`] using the provided file [`Reader`](AsyncRead).
    ///
    /// ### Error
    ///
    /// Returns [`Error::Io`] if the underlying [read operation][1] fails or the reader encounters
    /// an unexpected end of file.
    ///
    /// [1]: AsyncReadExt::poll_read
    async fn from_file<F>(file: &mut F) -> Result<Self, Error>
    where
        F: AsyncRead + Unpin + ?Sized,
    {
        let mut buf = [0u8; Self::SIZE.get()];
        file.read_exact(&mut buf).await?;
        Header::deserialize(&buf)
    }

    /// Returns a suitable [`Sector`] to write the specified [`Segment`]. This function is purely
    /// predictive; no file IO is executed.
    ///
    /// New segments are appended from the [`tail`](NonZeroU64) position, overwriting the previous
    /// manifest and any empty regions if present.
    ///
    /// ```text
    /// [Header] [Segment 0] ... [Segment N] [New Segment] ... [New Manifest]
    ///                                tail ↑                 ↑ manifest.offset
    /// ```
    ///
    /// Refer to the [write-cycle](self) documentation for more details.
    fn segment<S: Segment>(&self, src: &S) -> Result<Sector, Error> {
        Ok(Sector { offset: self.tail, length: src.size()? })
    }

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
    /// Refer to the [write-cycle](self) documentation for more details.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating [`size`](NonZeroU64)
    /// or [`offset`](NonZeroU64) for the relevant file regions.
    async fn manifest<S: Segment>(&self, manifest: &Manifest, seg: &S) -> Result<Sector, Error> {
        let length = seg.size()?;
        let offset = match manifest.size()? < length {
            true => self.tail.checked_add(length.get()),
            false => self.manifest.next(),
        }
        .ok_or(Error::Zero)?;
        Ok(Sector { offset, length })
    }

    /// todo → fn doc comment
    async fn write<F>(&self, file: &mut F) -> Result<(), Error>
    where
        F: AsyncSeek + AsyncWrite + Unpin + ?Sized,
    {
        Self::SECTOR.seek_to_start(file).await?;
        file.write_all(&self.serialize()?).await.map_err(Error::from)
    }

    /// todo → fn doc comment
    fn update(&mut self) {}
}

impl Serialize for Header {
    type Buffer = [u8; size_of::<Self>()];

    fn serialize_into(&self, buf: &mut [u8]) {
        self.tail.serialize_into(buf);
        self.manifest.serialize_into(buf);
    }

    fn serialize(&self) -> Result<Self::Buffer, accumulate::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        self.serialize_into(&mut buf);
        Ok(buf)
    }
}

impl Deserialize for Header {
    type Error = Error;

    fn deserialize(src: &[u8]) -> Result<Self, Self::Error> {
        let buf: [u8; Self::SIZE.get()] = match src {
            s if !s.starts_with(&MAGIC) => Err(Error::Magic),
            s if s[4] != VERSION => Err(Error::Version(s[4])),
            s => s.try_into().map_err(Error::Slice),
        }?;
        let tail = NonZeroU64::deserialize(&buf[5..13])?;
        let offset = NonZeroU64::deserialize(&buf[13..21])?;
        let length = NonZeroU64::deserialize(&buf[21..29])?;
        let manifest = Sector { offset, length };
        Ok(Self { tail, manifest })
    }
}

/// An exclusive owned file handle for an open [`clem`](crate) dataset.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub(crate) struct File {
    pub writer: RwLock<Writer>,
    pub mmap: Arc<Mmap>,
    pub path: PathBuf,
}

impl File {
    /// Create a new [clem](crate) file with read and write permissions at the specified [path][1].
    ///
    /// The file is initialised in a valid empty state with a default [`Manifest`] and no
    /// [`Segments`](Segment) or [`Metadata`][2]. The tail and manifest offset pointers are
    /// guaranteed to align exactly.
    ///
    /// ```text
    /// [Header] [Manifest]
    ///         ↑ tail & manifest.offset
    /// ```
    ///
    /// Implementors must ensure that the provided `path` remains valid and accessible for the
    /// entire duration of the operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`Path`](P)
    /// - The current process lacks read and write permissions
    /// - The platform does not support [memory mapping](memmap2)
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    ///
    /// [1]: PathBuf
    // [2]: todo → link to metadata struct or feature documentation
    pub(crate) async fn create<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();
        let manifest = Manifest::default();
        let sector = Sector {
            offset: Header::SECTOR.offset, // Manifest directly after header (no segments)
            length: manifest.size()?,
        };
        let header = Header::new(sector);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .truncate(false)
            .open(&path)
            .await?;
        file.write_all(&header.serialize()?).await?;
        file.write_all(&manifest.serialize()?).await?;
        file.flush().await?;
        // SAFETY: Undefined behaviour if mapped file is modified (refer to function documentation).
        let mmap = unsafe { mmap(&file, 0)? }.into();
        let file = BufWriter::new(file);
        let writer = Writer { file, header, manifest }.into();
        Ok(Self { writer, mmap, path })
    }

    /// Open an existing [clem](crate) file with read and write permissions at the specified
    /// [path](PathBuf).
    ///
    /// The [magic bytes](MAGIC) and [version number](VERSION) are validated immediately on open. A
    /// [`Mmap`] is scoped to the immutable [`Segment`] file region. Implementors must ensure that
    /// the provided `path` remains valid and accessible for the entire duration of the operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`Path`](P)
    /// - The current process lacks read and write permissions
    /// - Unexpected `EOF` while parsing the [`Header`] or [`Manifest`]
    /// - The platform does not support [memory mapping](memmap2)
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    pub(crate) async fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(false) // Explicitly disallow file creation. Use File::create instead.
            .truncate(false)
            .open(&path)
            .await?;
        let header = Header::from_file(&mut file).await?;
        // SAFETY: Undefined behaviour if mapped file is modified (refer to function documentation).
        let mmap = unsafe { mmap(&file, header.tail.get() as usize)? }.into();
        let manifest = Manifest::from_file(&mut file, header.manifest).await?;
        let file = BufWriter::new(file);
        let writer = Writer { file, header, manifest }.into();
        Ok(Self { writer, mmap, path })
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub(crate) struct Writer {
    pub file: BufWriter,
    pub header: Header,
    pub manifest: Manifest,
}

impl Writer {
    }

    /// Returns a suitable [`Sector`] to write the updated [`Manifest`].
    ///
    /// 1. Reserves space for the incoming [`Segment`]
    /// 2. Does not overwrite the existing manifest.
    ///
    /// This function is purely predictive; no file IO is executed.
    ///
    /// ```text
    /// [Header] [Segment 0] ... [Segment N] [New Segment] ... [New Manifest]
    ///                                tail ↑                 ↑ manifest.offset
    /// ```
    ///
    /// Refer to the [module documentation](self) documentation for more details.
    ///


impl TryFrom<&Manifest> for Sector {
    type Error = Error;

    fn try_from(manifest: &Manifest) -> Result<Self, Self::Error> {
        let offset = HEADER.try_into()?;
        let length = manifest.size().map_err(Error::from)?;
        Ok(Self { offset, length })
    }
}

/* ------------------------------------------------------------------------------ Specific Error */

/// Errors returned by [`File`] IO.
///
/// Enum variants cover various granular error cases that may arise when working with the underlying
/// file. Users should consider handling errors explicitly wherever possible to provide meaningful
/// error messages and recovery actions.
///
/// ### Implementation
///
/// This enum is `#[non_exhaustive]` meaning additional variants may be added in future versions.
/// Implementers are advised to include a wildcard arm `_` to account for potential additions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
#[non_exhaustive] // To accommodate potential future error cases.
pub enum Error {
    /// Underlying [`TryFromIntError`] from a checked conversion between two types.
    Convert(TryFromIntError),
    /// CBOR decoding failure for a manifest or schema payload.
    Decode(minicbor::decode::Error),
    /// Underlying [`std::io::Error`] from the file backing the [`Dataset`](crate::Dataset).
    Io(std::io::Error),
    /// File magic bytes did not match the expected `clem` signature.
    Magic,
    /// Underlying [`manifest::Error`] from a file manifest operation.
    Manifest(manifest::Error),
    /// Underlying [`TryFromSliceError`] while parsing a slice into a fixed-size array.
    Slice(TryFromSliceError),
    /// A read operation attempted to access bytes beyond the end of the input slice.
    Truncated {
        /// Expected length of the input slice.
        expected: usize,
        /// Actual length of the input slice.
        actual: usize,
    },
    /// File version number is not recognised by this build of [`clem`](crate).
    Version(u8),
    /// Attempted to decode a zero value into a [`NonZero`](core::num::NonZero) field.
    Zero,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Convert(e) => write!(f, "Integer type conversion error → {e}"),
            Self::Decode(e) => write!(f, "CBOR decode error → {e}"),
            Self::Io(e) => write!(f, "File IO error → {e}"),
            Self::Magic => f.write_str("File is not a valid clem dataset"),
            Self::Manifest(e) => write!(f, "Manifest error → {e}"),
            Self::Slice(e) => write!(f, "Try from slice error → {e}"),
            Self::Truncated { .. } => write!(f, "Read was truncated → {self:?}"),
            Self::Version(v) => write!(f, "Unrecognised clem version → {v}"),
            Self::Zero => write!(f, "Expected non-zero value was zero"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<TryFromSliceError> for Error {
    fn from(e: TryFromSliceError) -> Self {
        Self::Slice(e)
    }
}

impl From<TryFromIntError> for Error {
    fn from(e: TryFromIntError) -> Self {
        Self::Convert(e)
    }
}

impl From<minicbor::decode::Error> for Error {
    fn from(e: minicbor::decode::Error) -> Self {
        Self::Decode(e)
    }
}

impl From<accumulate::Error> for Error {
    fn from(e: accumulate::Error) -> Self {
        match e {
            accumulate::Error::Convert(inner) => Self::Convert(inner),
            accumulate::Error::Zero => Self::Zero,
        }
    }
}

impl From<manifest::Error> for Error {
    fn from(e: manifest::Error) -> Self {
        Self::Manifest(e)
    }
}

//noinspection DuplicatedCode → Conversion is implemented for error types across different modules.
impl<T, E> From<Error> for Result<T, E>
where
    E: From<Error>,
{
    fn from(error: Error) -> Self {
        Err(E::from(error))
    }
}

/* ---------------------------------------------------------------- Deserialize Trait Definition */

/// A **type** that can be deserialized from a canonical [`clem`](crate) binary representation.
pub trait Deserialize {
    /// The error type returned by [`deserialize`](Self::deserialize) on failure.
    type Error;

    /// Deserialize `self` from the provided source byte slice.
    #[rustfmt::skip] // Single line where clause improves readability
    fn deserialize(src: &[u8]) -> Result<Self, Self::Error> where Self: Sized;
}

/* ------------------------------------------------------------ Deserialize Trait Implementation */

impl Deserialize for NonZeroU64 {
    type Error = Error;

    fn deserialize(src: &[u8]) -> Result<Self, Self::Error> {
        let buf = src
            .get(0..size_of::<Self>())
            .ok_or(Error::Truncated {
                expected: size_of::<Self>(),
                actual: src.len(),
            })?
            .try_into()?;
        u64::from_le_bytes(buf).try_into().map_err(Error::Convert)
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sector_ord() {
        let hi = Sector::new(200, 16).expect("Sector::new failed for hi");
        let lo = Sector::new(100, 16).expect("Sector::new failed for lo");
        assert!(hi > lo);
        assert!(lo < hi);
    }

    #[test]
    fn sector_eq() {
        let short = Sector::new(100, 16).expect("Sector::new failed for short");
        let long = Sector::new(100, 32).expect("Sector::new failed for long");
        assert_ne!(short, long);
    }

    #[test]
    fn sector_copy() {
        let a = Sector::new(10, 5).expect("Sector::new failed");
        let b = a;
        assert_eq!(a, b);
    }
}
