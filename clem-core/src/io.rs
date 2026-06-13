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
//! [clem](crate) maximises IO performance by separating the data lifecycle into two phases:
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
//! A self-describing **CBOR** file [`Manifest`] is included after the immutable segment region and
//! lists all file segments by type. The manifest acts like the index of a book to enhance segment
//! discovery and enable O(1) random access. Refer to the [manifest documentation][2] for details.
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
//! [1]: crate::segment::Variant

#![doc = include_str!("../../doc/write-cycle.md")]
#![doc = include_str!("../../doc/read-cycle.md")]

use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::convert::{Infallible, TryInto};
use std::fmt;
use std::io::SeekFrom;
use std::num::{self, NonZeroU64, TryFromIntError};
use std::ops::Add;
use std::path::{Path, PathBuf};

use memmap2::{Mmap, MmapOptions};
use minicbor::{CborLen, Decode, Encode};
use smol::fs::{self, OpenOptions};
use smol::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::accumulate::{Accumulator, Buffer};
use crate::manifest::{self, Manifest, Pending};
use crate::{number, schema, Serialize};

/* ------------------------------------------------------------------------------ Public Exports */

/// Magic byte sequence used to identify a valid [clem](crate) file.
const MAGIC: [u8; 4] = *b"clem";

/// Current [clem](crate) major version number which is embedded in the file header to indicate
/// breaking changes in the format specification. Forwards and backwards compatibility across
/// version numbers is not guaranteed. Implementers must reject any unrecognised version number.
const VERSION: u8 = 1;

/// Total length of the file header in bytes. Includes the [magic][1] bytes, [version][2] number,
/// and [SIMD alignment][3] bytes.
///
/// [1]: MAGIC
/// [2]: VERSION
/// [3]: crate::Align
pub const HEADER: usize = size_of_val(&MAGIC) + size_of_val(&VERSION) + size_of::<Header>() + ALIGN;

/// Number of trailing zero bytes required to pad the [`File`](File) [`Header`] to the next 64-bit
/// SIMD [alignment boundary](crate::segment).
const ALIGN: usize = {
    let n = size_of_val(&MAGIC) + size_of_val(&VERSION) + size_of::<Header>() & 7;
    (8 - n) & 7
};

/// A contiguous byte region within the [clem](crate) file.
///
/// Implementers must [`Copy`] into an owned type when mutability is required e.g. for downstream
/// data processing.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Hash, Encode, Decode, CborLen)]
pub struct Sector {
    /// Byte offset to the start of the sector.
    #[n(0)]
    pub offset: u64,
    /// Total length of the sector in bytes.
    #[n(1)]
    pub length: NonZeroU64,
}

impl Sector {
    pub fn new<A, B>(offset: A, length: B) -> Result<Self, Error>
    where
        A: TryInto<u64>,
        B: TryInto<NonZeroU64>,
        Error: From<A::Error> + From<B::Error>,
    {
        Ok(Self {
            offset: offset.try_into()?,
            length: length.try_into()?,
        })
    }

    /// [`Seek`](AsyncSeek::poll_seek) the provided bytes stream to the start of [`self`](Sector)
    /// and return the new position.
    ///
    /// Returns [`Error::Io`] if the underlying seek operation fails.
    pub async fn seek_to_start<F>(&self, stream: &mut F) -> Result<u64, Error>
    where
        F: AsyncSeek + Unpin + ?Sized,
    {
        stream.seek(SeekFrom::Start(self.offset)).await.map_err(Error::from)
    }

    /// Returns the offset immediately following [`self`](Sector), or [`None`] on `u64` overflow.
    pub const fn next(&self) -> Option<NonZeroU64> {
        self.length.checked_add(self.offset)
    }

    /// Read the byte [slice][1] defined by [`self`](Sector) from the provided [`Mmap`].
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Truncated`] if the sector extends beyond the end of the [`Mmap`], or
    /// [`Error::Number`] if numeric conversion overflow occurs.
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    pub fn slice<'a>(&self, mmap: &'a Mmap) -> Result<&'a [u8], Error> {
        let start = self.offset.try_into()?;
        let end = self.next().ok_or(number::Error::Zero)?.get().try_into()?;
        mmap.get(start..end).ok_or(Error::Truncated {
            expected: end - start,
            actual: mmap.len().saturating_sub(start),
        })
    }
}

impl Add for Sector {
    type Output = Option<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        let offset = self.offset.min(rhs.offset);
        let length = self.length.checked_add(rhs.length.get())?;
        Some(Self { offset, length })
    }
}

impl Ord for Sector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl Serialize for Sector {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, number::Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(number::Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], number::Error> {
        buf.serialize_push(&self.offset)?.serialize_push(&self.length)
    }

    fn serialize(&self) -> Result<Self::Buffer, number::Error> {
        [u8::MIN; size_of::<Self>()].serialize_push(self)
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
    /// Byte offset immediately following the last committed [`Segment`]; calculated relative to
    /// the immutable segment region excluding the file [`Header`].
    #[n(0)]
    pub tail: NonZeroU64,
    /// On-disk location of the encoded [`Manifest`].
    #[n(1)]
    pub manifest: Sector,
}

impl Header {
    /// [`Sector`] containing the mutable region of the file [`Header`]. Excludes the immutable
    /// [magic bytes][1] and [version number][2].
    ///
    /// [1]: MAGIC
    /// [2]: VERSION
    const SECTOR: Sector = Sector {
        offset: { size_of_val(&MAGIC) + size_of_val(&VERSION) } as u64,
        length: NonZeroU64::new(size_of::<Self>() as u64).expect("Length is zero"),
    };

    /// Create a new [clem](crate) file [`Header`] pointing to the provided manifest [`Sector`].
    ///
    /// ```text
    /// [Header] [Manifest]
    ///         ↑ tail & manifest.offset
    /// ```
    ///
    /// The `tail` and `manifest.offset` pointers are guaranteed to align exactly.
    fn new(manifest: Sector) -> Result<Self, Error> {
        Ok(Self {
            tail: manifest.offset.try_into()?,
            manifest,
        })
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
        let mut buf = [0u8; HEADER];
        file.read_exact(&mut buf).await?;
        Header::deserialize(&buf)
    }
}

impl Serialize for Header {
    type Buffer = [u8; size_of::<Self>()];

    fn size(&self) -> Result<NonZeroU64, number::Error> {
        { size_of::<Self>() as u64 }.try_into().map_err(number::Error::from)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], number::Error> {
        buf.serialize_push(&self.tail)?.serialize_push(&self.manifest)
    }

    fn serialize(&self) -> Result<Self::Buffer, number::Error> {
        [0u8; size_of::<Self>()].serialize_push(self)
    }
}

impl Deserialize for Header {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf: [u8; HEADER] = match src {
            s if !s.starts_with(&MAGIC) => Err(Error::Magic),
            s if s[4] != VERSION => Err(Error::Version(s[4])),
            s => s.try_into().map_err(Error::Slice),
        }?;
        let tail = NonZeroU64::deserialize(&buf[5..13])?;
        let offset = u64::deserialize(&buf[13..21])?;
        let length = NonZeroU64::deserialize(&buf[21..29])?;
        let manifest = Sector { offset, length };
        Ok(Self { tail, manifest })
    }
}

/// An exclusive owned file handle for an open [clem](crate) dataset.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub(crate) struct File {
    /// todo → field doc comment
    pub file: fs::File,
    /// todo → field doc comment
    pub header: Header,
    /// todo → field doc comment
    pub manifest: Manifest,
    /// todo → field doc comment
    pub path: PathBuf,
}

impl File {
    /// Create a new [clem](crate) file with read and write permissions at the specified
    /// [`path`](P).
    ///
    /// The file is initialised in a valid empty state with a default [`Manifest`] and no
    /// [`Segments`](Segment) or [`Metadata`][1]. The tail and manifest offset pointers are
    /// guaranteed to align exactly.
    ///
    /// ```text
    /// [Header] [Manifest]
    ///         ↑ tail & manifest.offset
    /// ```
    ///
    /// Implementors must ensure that the provided [`path`](P) remains valid and accessible for the
    /// entire duration of the operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`path`](P)
    /// - The current process lacks read and write permissions
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    // [1]: todo → link to metadata struct or feature documentation
    pub(crate) async fn create<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();
        let manifest = Manifest::default();
        let sector = Sector {
            offset: HEADER as u64, // Manifest directly after header (no segments)
            length: manifest.size()?,
        };
        let header = Header::new(sector)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .truncate(false)
            .open(&path)
            .await?;
        file.write_all(&MAGIC).await?;
        file.write_all(&[VERSION]).await?;
        file.write_all(&header.serialize()?).await?;
        file.write_all(&manifest.serialize()?).await?;
        file.flush().await?;
        Ok(Self { file, header, manifest, path })
    }

    /// Open an existing [clem](crate) file with read and write permissions at the specified
    /// [`path`](P).
    ///
    /// The [magic bytes](MAGIC) and [version number](VERSION) are validated immediately on open.
    /// Implementors must ensure that the provided `path` remains valid and accessible for the
    /// entire duration of the operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`path`](P)
    /// - The current process lacks read and write permissions
    /// - Unexpected `EOF` while parsing the [`Header`] or [`Manifest`]
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
        let manifest = Manifest::from_file(&mut file, header.manifest).await?;
        Ok(Self { file, header, manifest, path })
    }

    /// Create a read-only [memory map](Mmap) backed by the [clem](crate) file.
    ///
    /// ### Errors
    ///
    /// [`Error::Zero`] if the [`Header`](HEADER) size exceeds [`u64::MAX`].
    // todo → Static assert HEADER size as u64, remove try_into runtime checks, use faster unchecked fn.
    ///
    /// [`Error::Io`] if the underlying system call fails. This can occur for a variety of reasons,
    /// such as the file is no longer accessible, or the platform does not support memory mapping.
    ///
    /// ### ⚠️ Safety
    ///
    /// This function is marked as [unsafe][1] because of the potential for undefined behaviour if
    /// the underlying file region is subsequently modified, in or out of process. Implementers are
    /// strongly advised to take appropriate precautions and ensure the mapped region is not
    /// accessed or modified concurrently in a way that causes undefined behaviour.
    ///
    /// [`Segments`](Segment) are immutable once written. The [`Mmap`] is tightly scoped to reduce
    /// the risk of undefined behaviour:
    ///
    /// - [`offset`](MmapOptions::offset) excludes the mutable [`Header`]
    /// - [`length`](MmapOptions::len) excludes the mutable [`Manifest`]
    /// - Only the immutable segment region is mapped
    ///
    /// The [`Arc`][2]`<`[`Mmap`](Mmap)`>` is updated after each [write-cycle](self) to include the
    /// newly appended segment. New readers must await a [read lock](RwLock) on the [dataset][3]
    /// before cloning the [`Arc`][2]. Existing mmaps are released only when their reference count
    /// drops to zero. In-flight reader mmaps remain valid because existing segments are unaltered.
    /// Buffer [`Sector`] offsets are recorded relative to the immutable segment region and index
    /// the [`Mmap`] directly; no runtime offset arithmetic.
    ///
    /// Refer to the [memmap](memmap2) crate for more details.
    ///
    /// [1]: https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html
    /// [2]: std::sync::Arc
    /// [3]: crate::dataset::Dataset
    pub(crate) unsafe fn mmap(&self, tail: NonZeroU64) -> Result<Mmap, Error> {
        let offset: u64 = HEADER.try_into()?;
        let length: usize = { tail.get() - offset }.try_into()?;
        // SAFETY: Undefined behaviour if mapped region is modified (refer to mmap documentation)
        unsafe { MmapOptions::new().offset(offset).len(length).map(&self.file).map_err(Error::Io) }
    }

    /// Append a new [`Segment`] to the file according to the [write-cycle](self).
    ///
    /// Returns a read-only [`Mmap`] covering the immutable segment region.
    // TODO → Add error list & mmap safety section to fn doc comment
    pub(crate) async fn write<S>(&mut self, seg: S) -> Result<Mmap, Error>
    where
        S: Push + for<'a> Write<Ctx<'a> = &'a Header>,
    {
        // Phase 1: Update the in-memory manifest
        let sector = seg.sector(&self.header)?;
        self.manifest.push(&seg, sector)?;
        // Phase 2: Append the new manifest
        let pending = Pending { header: &self.header, size: seg.size()? };
        self.header.manifest = self.manifest.write_to_file(&mut self.file, pending).await?;
        // Phase 3: Overwrite the file header manifest sector
        self.header.write_to_file(&mut self.file, ()).await?;
        // Phase 4: Append the new segment
        self.header.tail = seg
            .write_to_file(&mut self.file, &self.header)
            .await?
            .next()
            .ok_or(number::Error::Zero)?;
        // Phase 5: Overwrite the file header tail pointer
        self.header.write_to_file(&mut self.file, ()).await?;
        self.file.flush().await?;
        // SAFETY: Undefined behaviour if mapped region is modified (refer to mmap documentation)
        unsafe { self.mmap(self.header.tail) }
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
    /// CBOR decoding failure for a manifest or schema payload.
    Decode(minicbor::decode::Error),
    /// Underlying [`std::io::Error`] from the file backing the [`Dataset`](crate::Dataset).
    Io(std::io::Error),
    /// File magic bytes did not match the expected `clem` signature.
    Magic,
    /// Underlying [`Error`](number::Error) from a numerical operation or conversion.
    Number(number::Error),
    /// Underlying [`TryFromSliceError`] while parsing a slice into a fixed-size array.
    Slice(TryFromSliceError),
    /// A read operation attempted to access bytes beyond the end of the input slice.
    Truncated {
        /// Expected length of the input slice.
        expected: usize,
        /// Actual length of the input slice.
        actual: usize,
    },
    /// The specified `u32` is not a valid [Unicode scalar value][1].
    ///
    /// [1]: https://www.unicode.org/glossary/#unicode_scalar_value
    Utf8(u32),
    /// File version number is not recognised by this build of [clem](crate).
    Version(u8),
}

impl Error {
    /// Constructor for [`Error::Truncated`] wrapping the [`expected`](A) and [`actual`](B) lengths.
    pub(crate) fn truncated<A, B, E>(expected: A, actual: B) -> Self
    where
        usize: TryFrom<A, Error = E> + TryFrom<B, Error = E>,
        Self: From<E>,
    {
        let expected = match usize::try_from(expected) {
            Ok(value) => value,
            Err(error) => return Self::from(error),
        };
        let actual = match usize::try_from(actual) {
            Ok(value) => value,
            Err(error) => return Self::from(error),
        };
        Self::Truncated { expected, actual }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(e) => write!(f, "CBOR decode error → {e}"),
            Self::Io(e) => write!(f, "File IO error → {e}"),
            Self::Magic => f.write_str("File is not a valid clem dataset"),
            Self::Number(e) => write!(f, "Number error → {e}"),
            Self::Slice(e) => write!(f, "Try from slice error → {e}"),
            Self::Truncated { .. } => write!(f, "Read was truncated → {self:?}"),
            Self::Utf8(e) => write!(f, "Invalid UTF8 scalar value → {e}"),
            Self::Version(v) => write!(f, "Unrecognised clem version → {v}"),
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
        number::Error::from(e).into()
    }
}

impl From<minicbor::decode::Error> for Error {
    fn from(e: minicbor::decode::Error) -> Self {
        Self::Decode(e)
    }
}

impl From<number::Error> for Error {
    fn from(e: number::Error) -> Self {
        Self::Number(e)
    }
}

impl From<Infallible> for Error {
    fn from(value: Infallible) -> Self {
        match value {}
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

/// A **type** that can be deserialized from a canonical [clem](crate) binary representation.
pub trait Deserialize {
    /// Byte source from which values of [`Self`] are deserialized.
    type Src<'a>;

    /// Pull the exact number of bytes required to [deserialize](Self::deserialize) one instance
    /// of [`Self`] from `src`.
    ///
    /// Returns a read-only [slice][1] over the extracted bytes; splitting the source without an
    /// intermediate copy and advancing `src` by the number of bytes read.
    ///
    /// ### Guidance
    ///
    /// The default implementation leverages [`size_of`]`::<Self>()` for fixed-size types. Unsized
    /// types must override this default implementation with type-specific size determination logic
    /// such as reading an on-disk [`length`](NonZeroU64) prefix.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Truncated`] if `src` contains fewer than the requested number of bytes.
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    fn take(mut src: &[u8]) -> Result<&[u8], Error>
    where
        Self: Sized,
    {
        let expected = size_of::<Self>();
        let actual = src.len();
        src.get(..expected)
            .map(|data| {
                src = &src[expected..];
                Ok(data)
            })
            .unwrap_or_else(|| {
                src = &[];
                Error::Truncated { expected, actual }.into()
            })
    }

    /// Deserialize [`Self`] from the provided [source](Self::Src).
    // TODO → Remove Sized trait bound on Self to support unsized types; could try return Box<Self>
    #[rustfmt::skip] // Single line where clause improves readability
    fn deserialize(src: Self::Src<'_>) -> Result<Self, Error> where Self: Sized;
}

/* ------------------------------------------------------------ Deserialize Trait Implementation */

impl Deserialize for u8 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for u16 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for u32 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for u64 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for u128 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for num::NonZeroU8 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        u8::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroU16 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        u16::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroU32 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        u32::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for NonZeroU64 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        u64::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroU128 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        u128::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for i8 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for i16 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for i32 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for i64 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for i128 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for num::NonZeroI8 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        i8::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroI16 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        i16::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroI32 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        i32::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroI64 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        i64::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for num::NonZeroI128 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        i128::deserialize(src)?.try_into().map_err(Into::into)
    }
}

impl Deserialize for f32 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for f64 {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let buf = Self::take(src)?.try_into()?;
        let num = Self::from_le_bytes(buf);
        Ok(num)
    }
}

impl Deserialize for char {
    type Src<'a> = &'a [u8];

    fn deserialize(src: &[u8]) -> Result<Self, Error> {
        let utf8 = u32::deserialize(src)?;
        char::from_u32(utf8).ok_or(Error::Utf8(utf8))
    }
}

/* --------------------------------------------------------------- Deserializer Trait Definition */

/// A **source** that can be deserialized into a [supported data type](Deserialize).
pub trait Deserializer<'a, I>
where
    I: Deserialize<Src<'a> = Self>,
{
    /// Deserialize [`Self`] into an instance of the target type [`I`].
    fn deserialize_into(self) -> Result<I, Error>
    where
        Self: Sized,
    {
        I::deserialize(self)
    }
}

/* ----------------------------------------------------------- Deserializer Trait Implementation */

impl<'a, I, S> Deserializer<'a, I> for S where I: Deserialize<Src<'a> = S> {}

/* ---------------------------------------------------------------------- Write Trait Definition */

/// A **data type** that is written to the [clem](crate) file at a specific [location](Sector).
pub(crate) trait Write: Serialize {
    /// Additional context required to determine the target [`Sector`].
    type Ctx<'a>;

    /// Returns a suitable [`Sector`] to write [`Self`].
    ///
    /// This function is purely predictive; no file IO is executed. Implementing types can leverage
    /// the associated [`Context`](Self::Ctx) type for dynamic sector identification at runtime.
    ///
    /// Sector `offset` is calculated relative to the immutable segment region; excludes the file
    /// header. Refer to the [write-cycle](self) documentation for more details regarding the
    /// [clem](crate) file layout.
    fn sector(&self, ctx: Self::Ctx<'_>) -> Result<Sector, number::Error>;

    /// Write [`Self`] to the file at the [`Sector`](Self::sector) computed from [`Ctx`](Self::Ctx).
    ///
    /// Returns the written [`Sector`] for subsequent function chaining.
    async fn write_to_file<F>(&self, file: &mut F, ctx: Self::Ctx<'_>) -> Result<Sector, Error>
    where
        F: AsyncSeek + AsyncWrite + Unpin + ?Sized,
    {
        let sector = self.sector(ctx)?;
        sector.seek_to_start(file).await?;
        file.write_all(self.serialize()?.as_ref()).await?;
        Ok(sector)
    }
}

/* ------------------------------------------------------------------ Write Trait Implementation */

impl Write for Header {
    type Ctx<'a> = (); // No context required. Header sector is known at compile time.

    fn sector(&self, _: ()) -> Result<Sector, number::Error> {
        Ok(Header::SECTOR)
    }
}

impl Write for schema::Schema {
    type Ctx<'a> = &'a Header;

    fn sector(&self, ctx: &Header) -> Result<Sector, number::Error> {
        Ok(Sector {
            offset: ctx.tail.get(),
            length: self.size()?,
        })
    }
}

impl<I> Write for Accumulator<I> {
    type Ctx<'a> = &'a Header;

    fn sector(&self, ctx: &Header) -> Result<Sector, number::Error> {
        Ok(Sector {
            offset: ctx.tail.get(),
            length: self.size()?,
        })
    }
}

/* --------------------------------------------------------------------- Ingest Trait Definition */

/// A **data source** that is appended to the [`Manifest`] before writing to disk.
pub(crate) trait Push {
    /// The **lightweight record** type used to describe values of [`Self`] in the [`Manifest`].
    type Record;

    /// Build a [`Descriptor`](Self::Record) for [`Self`] and append to the [`Manifest`].
    fn push_to_manifest(&self, man: &mut Manifest, sec: Sector) -> Result<Sector, number::Error>;
}

/* ----------------------------------------------------------------- Ingest Trait Implementation */

impl<I> Push for Accumulator<I> {
    type Record = manifest::Buffer;

    fn push_to_manifest(&self, man: &mut Manifest, sec: Sector) -> Result<Sector, number::Error> {
        let mut columns = man
            .schemas
            .get_mut(&self.name)
            // SAFETY: Dataset::schema registers the schema before producing an Accumulator
            .expect("Schema missing from manifest")
            .columns
            .values_mut();
        // NOTE: Buffer offset is relative to the immutable region; excludes the file header.
        let offset = sec
            .offset
            .checked_add(Accumulator::<I>::HEADER as u64)
            .ok_or(number::Error::Zero)?
            .checked_sub(HEADER as u64)
            .ok_or(number::Error::Zero)?;
        self.data.buffers(offset, &mut columns)?;
        Ok(sec)
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_prefixes_src() {
        let bytes = [1u8, 0, 2, 0];
        assert_eq!(u16::take(&bytes).expect("Take failed"), &[1, 0]);
        assert_eq!(bytes, [1, 0, 2, 0]); // Source is borrowed, never consumed.
    }

    #[test]
    fn take_truncated_errors() {
        let bytes = [1u8]; // One byte cannot encode u16.
        assert!(matches!(u16::take(&bytes), Err(Error::Truncated { .. })));
    }

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
