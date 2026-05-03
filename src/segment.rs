//! Segment header encode/decode and alignment helpers — implemented in Phase 4.
/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Binary encoding and decoding for on-disk segments.
//!
//! ---
//!
//! ### Segment Composition
//!
//! A [`clem`](crate) file is partitioned into self-describing **segments** which are immutable
//! once written. Each segment begins with a minimal header consisting of a [`variant: u8`](Variant)
//! identifier and [`length: NonZeroU64`](NonZeroU64).
//!
//! - [`Schema`] segments describe the structure of encoded data.
//! - [`Data`] segments carry columnar buffers for a specified schema instance.
//!
//! Multimodality and schema evolution are realised by appending additional schema segments. Data
//! storage and file extensibility are realised by appending additional data segments. Format
//! extensibility may be achieved via the introduction of new segment variants in future releases.
//!
//! ### Module Boundary
//!
//! This module performs **only** in-memory ⇄ byte-buffer transformations. File I/O is the
//! responsibility of the [`crate::io`] module.

use minicbor::{Decode, Encode};
use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;

/* ------------------------------------------------------------------------------ Public Exports */

/// On-disk **variant** identifier carried in the first byte of every segment header.
///
/// Format extensibility may be achieved via the introduction of new segment variants in future
/// releases. Existing variants are guaranteed to retain their discriminant values for binary
/// compatibility with existing files.
///
/// See the [module level documentation](self) for more details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode)]
#[non_exhaustive] // To accommodate future segment variants.
#[repr(u8)] // To map discriminant values directly ⇄ variant byte in the segment header.
pub enum Variant {
    /// A [`Schema`] segment describing the [structure](crate::schema::Schema) of encoded data.
    #[n(0)]
    Schema = 0x01, // DO NOT alter discriminant value (breaking change)
    /// A [`Data`] segment encoding columnar buffers for a specified schema instance.
    #[n(1)]
    Data = 0x02, // DO NOT alter discriminant value (breaking change)
}

impl TryFrom<u8> for Variant {
    type Error = Error;

    fn try_from(byte: u8) -> Result<Self, Error> {
        match byte {
            x if x == Self::Schema as u8 => Ok(Self::Schema),
            x if x == Self::Data as u8 => Ok(Self::Data),
            other => Error::Variant {
                expected: None,
                found: other,
            }
            .into(),
        }
    }
}

/* --------------------------------------------------------------------------- Alignment Helpers */

/// Round `n` up to the next multiple of eight; the unit of [critical-field alignment](self).
pub(crate) const fn align(n: usize) -> usize {
    (n + 7) & !7
}

/// Errors returned by [`Segment`] encoding and decoding.
///
/// Enum variants cover various granular error cases that may arise when working with segments.
/// Users should consider handling errors explicitly wherever possible to provide meaningful error
/// messages and recovery actions.
///
/// ### Implementation
///
/// This enum is `#[non_exhaustive]` meaning additional variants may be added in future versions.
/// Implementers are advised to include a wildcard arm `_` to account for potential additions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode)]
#[non_exhaustive] // To accommodate potential future error cases.
pub enum Error {
}
