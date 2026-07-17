/*
Project: msca
GitHub: https://github.com/MillieFD/msca

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Immutable named binary segments carrying free-form bytes in any user-defined format.
//!
//! ---
//!
//! ### Opaque Payloads
//!
//! A binary segment stores one named sequence of bytes that can be read but never altered once
//! written. The payload is opaque to [msca](crate): implementers choose the encoding – TOML, JSON,
//! CBOR, packed numeric arrays, or anything else – and carry the full burden of parsing and
//! validation. Typical uses include file-level metadata, configuration snapshots, provenance
//! records, and genuinely constant items that would otherwise waste a schema column.
//!
//! ### Write Surface
//!
//! [`Binary`] is a **byte accumulator** mirroring the [`Accumulator`](crate::Accumulator) design:
//! bytes are pushed in one or more chunks and committed through [`Dataset::write`][1] as a single
//! immutable segment. Binary segments are standalone; no prior schema registration is required.
//!
//! ```rust,ignore
//! let mut bin = msca::Binary::new("calibration");
//! bin.push(bytes);
//! dataset.write(bin).await?;
//! ```
//!
//! An empty accumulator performs no file IO. The manifest entry is reserved **before** the
//! write-cycle begins, so a name collision – reported as [`Error::Collision`][2] – never mutates
//! the file.
//!
//! ### Read Surface
//!
//! [`Dataset::binary`][3] returns the payload as a **zero-copy** byte slice borrowed directly from
//! the underlying memory map. The manifest sector points at exactly the payload bytes, so readers
//! route fearlessly with no per-access framing or checksum work.
//!
//! ```rust,ignore
//! let data: &[u8] = dataset.binary("calibration")?;
//! ```
//!
//! ### Alignment
//!
//! The first payload byte is guaranteed to begin at an **absolute** 64-bit boundary, measured
//! relative to the page-aligned memory map. Up to seven zero-filled gap bytes are inserted after
//! the segment header to achieve this, allowing zero-copy reinterpretation of packed numeric data.
//!
//! The payload is an opaque byte sequence, so **only** the first byte is aligned. Maintaining any
//! interior alignment is the responsibility of the implementer who chose the encoding.
//!
//! ### Immutability
//!
//! Immutability is enforced structurally; no runtime mutability checks exist because no mutable
//! surface exists:
//!
//! - The segment region is **append-only**; existing segments are never rewritten.
//! - Registration only ever fills a **vacant** manifest entry; duplicate names are rejected before
//!   any file IO occurs.
//! - Reads borrow from a **read-only** memory map as shared `&[u8]` slices.
//!
//! Refer to the [on-disk format specification](crate::io) for the complete file anatomy.
//!
//! [1]: crate::Dataset::write
//! [2]: manifest::Error::Collision
//! [3]: crate::Dataset::binary

use std::collections::btree_map::{Entry, VacantEntry};
use std::num::NonZeroU64;

use crate::io::{self, Buffer, Checksum, HEADER, Register};
use crate::manifest::{self, Manifest};
use crate::schema::number;
use crate::segment::{Align, Header as Head, Segment, Variant};
use crate::{Accumulate, Sector, Serialize};

/* ------------------------------------------------------------------------------ Public Exports */

/// An in-memory **byte accumulator** for one named immutable binary segment.
///
/// Bytes are [pushed](Accumulate::push) in one or more chunks and committed through
/// [`Dataset::write`](crate::Dataset::write) as a single immutable [`Binary`](Variant::Binary)
/// segment. The payload is opaque to [msca](crate); implementers choose the encoding and carry the
/// full burden of parsing and validation.
///
/// See the [module level documentation](self) for more details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Binary {
    /// Name for retrieval via the [`Manifest`] `bins` map.
    name: String,
    /// Accumulated payload bytes.
    data: Vec<u8>,
}

impl Binary {
    /// Initialises a new empty [`Binary`] accumulator with the specified `name`.
    pub fn new<N>(name: N) -> Self
    where
        String: From<N>,
    {
        Self {
            name: String::from(name),
            data: Vec::new(),
        }
    }

    /// Initialises a new empty [`Binary`] accumulator with the specified `name`, reserving space
    /// for at least `size` payload bytes.
    ///
    /// ### Guidance
    ///
    /// Prefer this constructor when the payload size is known ahead of time; the accumulator then
    /// grows without reallocating as chunks are [pushed](Accumulate::push).
    pub fn with_capacity<N>(name: N, size: usize) -> Self
    where
        String: From<N>,
    {
        Self {
            name: String::from(name),
            data: Vec::with_capacity(size),
        }
    }

    /// Number of zero-filled **gap** bytes inserted after the segment [`Header`](Head) at the
    /// specified segment `offset`, aligning the payload – written after the eight-byte size prefix
    /// – to an absolute 64-bit boundary.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Zero`](number::Error::Zero) on `u64` overflow.
    fn gap(offset: u64) -> Result<usize, number::Error> {
        offset.checked_add(Head::SIZE as u64).ok_or(number::Error::Zero)?.pad()
    }
}

impl<N> From<N> for Binary
where
    String: From<N>,
{
    fn from(name: N) -> Self {
        Self::new(name)
    }
}

impl<'d> Accumulate<&'d [u8]> for Binary {
    /// Append one chunk of payload bytes to the [accumulator](Binary).
    fn push(&mut self, item: &'d [u8]) {
        self.data.extend_from_slice(item);
    }

    fn discard(&mut self) {
        self.data.clear();
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the number of accumulated payload **bytes**.
    fn count(&self) -> u64 {
        self.data.len() as u64
    }
}

impl Segment for Binary {
    const VARIANT: Variant = Variant::Binary;

    fn wrap(&self, offset: u64) -> Result<Vec<u8>, number::Error> {
        const ADDS: u64 = { Head::SIZE + size_of::<u64>() } as u64;
        const LEAD: u64 = size_of::<NonZeroU64>() as u64;
        let plen = NonZeroU64::new(self.data.len() as u64).ok_or(number::Error::Zero)?;
        let gap = Self::gap(offset)?;
        let size = { gap as u64 }
            .checked_add(LEAD)
            .ok_or(number::Error::Zero)?
            .checked_add(plen.get())
            .ok_or(number::Error::Zero)?;
        let full = size.checked_add(ADDS).ok_or(number::Error::Zero)?.try_into()?;
        let mut buf = vec![u8::MIN; full];
        let rem =
            buf.as_mut_slice().serialize_push(&{ Self::VARIANT as u8 })?.serialize_push(&size)?;
        rem[..gap].fill(u8::MIN);
        self.data.serialize_into(plen.serialize_into(&mut rem[gap..])?)?;
        Self::checksum(&mut buf)?;
        Ok(buf)
    }
}

impl Checksum for Binary {}

impl Register for Binary {
    type Error = io::Error;
    type Entry<'m> = VacantEntry<'m, String, Sector>;

    fn entry<'m>(&self, m: &'m mut Manifest) -> Result<Self::Entry<'m>, io::Error> {
        match m.bins.entry(self.name.clone()) {
            Entry::Occupied(e) => manifest::Error::Collision { name: e.key().clone() }.into(),
            Entry::Vacant(e) => Ok(e),
        }
    }

    fn register<'a, 'm>(self, s: &'a Sector, e: Self::Entry<'m>) -> Result<&'a Sector, io::Error> {
        let start = s
            .offset
            .checked_add(Head::SIZE as u64)
            .ok_or(number::Error::Zero)?
            .align()?
            .checked_add(size_of::<NonZeroU64>() as u64)
            .ok_or(number::Error::Zero)?
            .checked_sub(HEADER as u64)
            .ok_or(number::Error::Zero)?;
        e.insert(Sector::new(start, self.data.len() as u64)?);
        Ok(s)
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::io::File;

    /// Unique scratch path for a layout test, cleared before use.
    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("msca-{name}.msca"));
        std::fs::remove_file(&path).ok();
        path
    }

    /// [`Binary::gap`] aligns the post-header position to the next 64-bit boundary.
    #[test]
    fn gap_aligns_prefix() {
        for offset in 0..16u64 {
            let gap = Binary::gap(offset).expect("Gap failed") as u64;
            assert_eq!({ offset + Head::SIZE as u64 + gap } % 8, 0);
        }
    }

    /// [`Binary::with_capacity`] reserves payload space without accumulating any bytes.
    #[test]
    fn with_capacity_reserves() {
        let bin = Binary::with_capacity("b", 64);
        assert!(bin.is_empty());
        assert_eq!(bin.count(), 0);
        assert!(bin.data.capacity() >= 64);
    }

    /// [`Segment::wrap`] frames `[variant][size][gap][prefix][payload][checksum]` densely: the
    /// size field spans gap + prefix + payload with no trailing padding, and the checksum covers
    /// every preceding byte.
    #[test]
    fn frame_layout() {
        let mut bin = Binary::new("b");
        bin.push([1u8, 2, 3].as_slice());
        let offset = 3; // Deliberately misaligned segment start
        let bytes = bin.wrap(offset).expect("Wrap failed");
        assert_eq!(bytes[0], Variant::Binary as u8);
        let head = Head::SIZE;
        let size = u64::from_le_bytes(bytes[1..head].try_into().expect("Size is 8 bytes"));
        let gap = Binary::gap(offset).expect("Gap failed");
        assert_eq!(size as usize, gap + size_of::<NonZeroU64>() + 3); // Dense: no trailing pad
        assert_eq!(bytes.len(), head + size as usize + size_of::<u64>());
        Binary::verify(&bytes).expect("Checksum failed");
        let data = head + gap + size_of::<NonZeroU64>();
        assert_eq!(&bytes[data..data + 3], &[1, 2, 3]);
    }

    /// A duplicate name is rejected while reserving the manifest entry – before any file IO – so
    /// the on-disk file remains intact and reopens cleanly afterwards.
    #[test]
    fn duplicate_rejected_before_io() {
        smol::block_on(async {
            let path = scratch("bin-entry");
            let mut file = File::create(&path).await.expect("Create failed");
            let mut bin = Binary::new("cal");
            bin.push([9u8; 4].as_slice());
            file.write(bin).await.expect("Write failed");
            let mut twin = Binary::new("cal");
            twin.push([7u8; 2].as_slice());
            let err = file.write(twin).await.expect_err("Duplicate accepted");
            assert!(matches!(
                err,
                io::Error::Manifest(manifest::Error::Collision { .. })
            ));
            drop(file);
            File::open(&path).await.expect("Reopen failed"); // Manifest intact
            std::fs::remove_file(&path).ok();
        });
    }

    /// The registered `bins` sector spans exactly the payload at an absolute 64-bit boundary,
    /// relative to the immutable segment region.
    #[test]
    fn sector_spans_payload() {
        smol::block_on(async {
            let path = scratch("bin-sector");
            let mut file = File::create(&path).await.expect("Create failed");
            let mut bin = Binary::new("cal");
            bin.push([1u8, 2, 3, 4, 5].as_slice());
            file.write(bin).await.expect("Write failed");
            let sect = *file.manifest.bins.get("cal").expect("Bin missing");
            let bytes = std::fs::read(&path).expect("Read failed");
            std::fs::remove_file(&path).ok();
            assert_eq!(sect.size.get(), 5); // Payload bytes only
            assert_eq!(sect.offset % 8, 0); // Absolute 64-bit alignment
            let abs = sect.offset as usize + HEADER;
            assert_eq!(&bytes[abs..abs + 5], &[1, 2, 3, 4, 5]);
        });
    }
}
