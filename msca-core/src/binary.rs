/*
Project: msca
GitHub: https://github.com/MillieFD/msca

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Immutable named segments containing free-form bytes in any format.
//!
//! ---
//!
//! Each binary segment is recorded in the [`Manifest`] under a unique [`name`](String). The
//! [`Dataset`][1] provides a basic read and write surface, leaving implementers free to choose any
//! interpretation strategy e.g. packed numeric arrays, [TOML][2] for human-readable configuration,
//! or [CBOR][3] for schema-free object encoding.
//!
//! ### Write Surface
//!
//! [`Bin`] is an in-memory [byte accumulator](Accumulate) that ingests raw bytes. Pass to
//! [`Dataset::write`][4] to write the accumulated bytes to disk as a single immutable [`Segment`].
//! Binary segments are standalone; no prior schema registration is required.
//!
//! ```rust,ignore
//! let mut bin = msca::Binary::new("calibration");
//! bin.push(bytes);
//! dataset.write(bin).await?;
//! ```
//!
//! Empty accumulators are ignored. The [`Manifest`] entry is reserved before file [`IO`][5]; a
//! rejected [`Segment`] leaves the file untouched.
//!
//! ### Read Surface
//!
//! [`Dataset::binary`][3] returns the binary segment body as a **zero-copy** byte [slice][6]
//! borrowed directly from the underlying [memory map](memmap2::Mmap). The [segment Header](Header)
//! is excluded from the [`Sector`] recorded in the manifest; the optimised random-access read path
//! routes fearlessly to the relevant segment body without boundary checks or variant verification.
//!
//! ```rust,ignore
//! let data: &[u8] = dataset.binary("calibration")?;
//! ```
//!
//! Segment immutability is structural; a mutable surface is deliberately omitted to enforce this
//! invariant.
//!
//! ### Alignment
//!
//! The segment body is guaranteed to begin at an **absolute** 64-bit boundary relative to the
//! page-aligned memory map. Up to seven zero-filled alignment bytes are inserted directly after the
//! segment header. Maintaining interior alignment – if required – is the responsibility of the
//! implementer.
//!
//! Refer to the [on-disk format specification][8] for more details.
//!
//! [1]: crate::dataset::Dataset
//! [2]: https://toml.io/en/
//! [3]: https://cbor.io
//! [4]: crate::dataset::Dataset::write
//! [5]: io::File::write
//! [6]: https://doc.rust-lang.org/std/primitive.slice.html
// [7]: TODO → link to on-disk-format.md

use std::collections::btree_map::{Entry, VacantEntry};
use std::num::NonZeroU64;

use minicbor::{CborLen, Decode, Encode};

use crate::io::{self, Buffer, Checksum, Register, HEADER};
use crate::manifest::{self, Manifest};
use crate::schema::number::Error;
use crate::segment::{Align, Header, Segment, Variant};
use crate::{Accumulate, Sector, Serialize};

/* ------------------------------------------------------------------------------ Public Exports */

/// An in-memory **byte accumulator** for one named binary segment.
///
/// Refer to the [module level documentation](self) for more details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Encode, Decode, CborLen)]
pub struct Bin {
    /// [Name](String) of the corresponding [`Sector`] registered in the [`Manifest`].
    #[cbor(n(0), skip_if = "String::is_empty")]
    pub name: String,
    /// Contiguous growable array of heap-allocated accumulated bytes.
    #[cbor(n(1), skip_if = "Vec::is_empty")]
    pub data: Vec<u8>,
}

impl Bin {
    /// Initialise a new empty [`Bin`] accumulator with the specified [`name`](N).
    ///
    /// The [accumulator](Accumulate) will not allocate until bytes are [pushed](Accumulate::push).
    /// Prefer [`Bin::with_capacity`] to pre-allocate capacity if the number of bytes is known
    /// ahead of time.
    pub fn new<N>(name: N) -> Self
    where
        String: From<N>,
    {
        Self {
            name: String::from(name),
            data: Vec::new(),
        }
    }

    /// Initialise a new empty [`Bin`] accumulator with the specified [`name`](N) and capacity for
    /// at least `size` bytes without reallocating.
    ///
    /// ### Guidance
    ///
    /// Prefer `with_capacity` instead of [`new`](Self::new) when the number of bytes is known ahead
    /// of time; the accumulator can grow without intermediate reallocation overhead as chunks are
    /// [pushed](Accumulate::push).
    ///
    /// ### ⚠️ Panics
    ///
    /// Panics if the requested capacity exceeds [`isize::MAX`] bytes.
    ///
    /// Refer to the [`Vec::with_capacity`] documentation for more details.
    pub fn with_capacity<N>(name: N, size: usize) -> Self
    where
        String: From<N>,
    {
        Self {
            name: String::from(name),
            data: Vec::with_capacity(size),
        }
    }

    /// [Iterate](Iterator) over the provided `&[u8]` slice, [copying](Copy) each byte and appending
    /// to [`self`](Self).
    ///
    /// ### ⚠️ Panics
    ///
    /// Panics if the requested capacity exceeds [`isize::MAX`] bytes.
    ///
    /// Refer to the [`Vec::extend_from_slice`] documentation for more details.
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.data.extend_from_slice(slice);
    }
}

impl<N> From<N> for Bin
where
    String: From<N>,
{
    fn from(name: N) -> Self {
        Self::new(name)
    }
}

impl<'d> Accumulate<&'d [u8]> for Bin {
    /// Append one byte [chunk][1] to the [accumulator](Bin).
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    fn push(&mut self, item: &'d [u8]) {
        self.data.extend_from_slice(item);
    }

    fn discard(&mut self) {
        self.data.clear();
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the number of accumulated **bytes**.
    fn count(&self) -> u64 {
        self.data.len() as u64
    }
}

impl Extend<u8> for Bin {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = u8>,
    {
        Extend::extend(&mut self.data, iter);
    }
}

impl Segment for Bin {
    const VARIANT: Variant = Variant::Binary;

    fn wrap(&self, offset: u64) -> Result<Vec<u8>, Error> {
        const PREFIX: u64 = { Header::SIZE + size_of::<u64>() } as u64;
        let pad = offset.checked_add(Header::SIZE as u64).ok_or(Error::Zero)?.pad()?;
        let size = self.data.size()?.get().checked_add(pad as u64).ok_or(Error::Zero)?;
        let full = size.checked_add(PREFIX).ok_or(Error::Zero)?.try_into()?;
        let mut buf = vec![u8::MIN; full];
        let rem = buf
            .as_mut_slice()
            .serialize_push(&{ Self::VARIANT as u8 })?
            .serialize_push(&size)?
            .serialize_push(&self.data.count())?;
        rem[..pad].fill(u8::MIN);
        self.data.serialize_into(&mut rem[pad..])?;
        Self::checksum(&mut buf)?;
        Ok(buf)
    }
}

impl Checksum for Bin {}

impl Register for Bin {
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
            .checked_add(Header::SIZE as u64)
            .ok_or(Error::Zero)?
            .align()?
            .checked_add(size_of::<NonZeroU64>() as u64)
            .ok_or(Error::Zero)?
            .checked_sub(HEADER as u64)
            .ok_or(Error::Zero)?;
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

    /// [`Bin::with_capacity`] reserves payload space without accumulating any bytes.
    #[test]
    fn with_capacity_reserves() {
        let bin = Bin::with_capacity("b", 64);
        assert!(bin.is_empty());
        assert_eq!(bin.count(), 0);
        assert!(bin.data.capacity() >= 64);
    }

    /// A duplicate name is rejected while reserving the manifest entry – before any file IO – so
    /// the on-disk file remains intact and reopens cleanly afterwards.
    #[test]
    fn duplicate_rejected_before_io() {
        smol::block_on(async {
            let path = scratch("bin-entry");
            let mut file = File::create(&path).await.expect("Create failed");
            let mut bin = Bin::new("cal");
            bin.push([9u8; 4].as_slice());
            file.write(bin).await.expect("Write failed");
            let mut twin = Bin::new("cal");
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
            let mut bin = Bin::new("cal");
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

    /// [`Extend`] moves bytes into the accumulator from any [`IntoIterator`] that yields [`u8`],
    /// covering both a bare iterator and an owned collection.
    #[test]
    fn extend_moves_bytes() {
        let mut bin = Bin::new("b");
        bin.extend(1u8..=3); // Bare iterator
        bin.extend([4u8, 5, 6]); // Owned IntoIterator
        assert_eq!(bin.data, [1, 2, 3, 4, 5, 6]);
        assert_eq!(bin.count(), 6);
    }

    /// Every binary segment body begins on an absolute 64-bit boundary, even when a preceding
    /// segment leaves an odd byte count that must be padded before the next header.
    #[test]
    fn body_starts_aligned() {
        smol::block_on(async {
            let path = scratch("bin-align");
            let mut file = File::create(&path).await.expect("Create failed");
            for (name, size) in [("a", 1usize), ("b", 3), ("c", 7), ("d", 8)] {
                let mut bin = Bin::new(name);
                bin.push(vec![u8::MAX; size].as_slice());
                file.write(bin).await.expect("Write failed");
            }
            for name in ["a", "b", "c", "d"] {
                let sect = file.manifest.bins.get(name).expect("Bin missing");
                assert_eq!(sect.offset % 8, 0, "{name} body misaligned");
            }
            drop(file);
            std::fs::remove_file(&path).ok();
        });
    }
}
