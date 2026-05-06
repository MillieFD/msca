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
//!                                tail ↑   ↑ offset
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

/// Magic byte sequence used to identify a valid [`clem`](crate) file.
const MAGIC: [u8; 4] = *b"clem";

/// Current [`clem`](crate) major version number which is embedded in the file header to indicate
/// breaking changes in the format specification. Forwards and backwards compatibility across
/// version numbers is not guaranteed. Implementers must reject any unrecognised version number.
const VERSION: u8 = 1;

/// Total length of the file header in bytes. Includes the [magic bytes][1] and [version number][2].
///
/// [1]: MAGIC
/// [2]: VERSION
const HEADER: usize = size_of_val(&MAGIC) + size_of_val(&VERSION) + size_of::<Header>();

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

impl Serialize for Header {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.tail.get().to_le_bytes());
        self.manifest.serialize_into(buf);
    }
}


