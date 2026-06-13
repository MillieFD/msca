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
//! A [clem](crate) file is partitioned into self-describing **segments** which are immutable once
//! written. Each segment begins with a minimal header, consisting of a [`variant: u8`](Variant)
//! identifier and [`length: NonZeroU64`](NonZeroU64), followed by a variant-specific payload.
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
//! This module performs in-memory ⇄ byte-buffer transformations **only**. See the
//! [IO module](crate::io) for interaction with the underlying [`File`][1].
//!
//! [1]: crate::io::File

#![doc = include_str!("../../doc/simd-alignment.md")]

use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::num::NonZeroU64;

use minicbor::{CborLen, Decode, Encode};

use crate::accumulate::Buffer;
use crate::schema::{number, Schema};
use crate::Serialize;

/* ------------------------------------------------------------------------------ Public Exports */

#[rustfmt::skip] // pub exports grouped separately from private imports
pub use self::variant::Variant;

/// A minimal segment header containing:
///
/// 1. A [`Variant`] byte to identify the segment type and payload structure.
/// 2. LE [`NonZeroU64`] encoding the size of the segment payload in bytes.
///
/// See the [module level documentation](self) for more details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
struct Header {
    /// Segment [`Variant`] identifier carried in the first byte of every segment header.
    #[n(0)]
    variant: Variant,
    /// LE [`NonZeroU64`] encoding the size of the segment payload in bytes. Excludes the header.
    #[n(1)]
    length: NonZeroU64,
}

impl Header {
    /// Packed [size](size_of) of the segment [`Header`] in bytes.
    ///
    /// The rust compiler may add padding bytes to the **in-memory** header struct to improve field
    /// alignment and access efficiency. However, the **on-disk** segment header requires a strict
    /// compact layout without padding to minimise storage overhead.
    const SIZE: usize = size_of::<u8>() + size_of::<NonZeroU64>();
}

impl Serialize for Header {
    type Buffer = [u8; Self::SIZE];

    fn size(&self) -> Result<NonZeroU64, number::Error>
    where
        Self: Sized,
    {
        { Self::SIZE as u64 }.try_into().map_err(number::Error::Convert)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], number::Error> {
        // SAFETY: Header::Buffer size is Σ of fixed-size fields; guaranteed to fit all data.
        buf.serialize_push(&self.variant)?.serialize_push(&self.length)
    }

    fn serialize(&self) -> Result<Self::Buffer, number::Error> {
        [u8::MIN; Self::SIZE].serialize_push(self)
    }
}

mod variant {
    //! This module defines the segment [`Variant`] identifier and associated parsing [`Error`].
    //!
    //! A [clem](crate) file is partitioned into self-describing **segments** which are immutable
    //! once written. Each segment begins with a single [`Variant`] byte to identify the segment
    //! type and payload structure. Readers dispatch on the variant byte to specific decoders.

    use std::fmt::{Display, Formatter};
    use std::num::NonZeroU64;

    use minicbor::{CborLen, Decode, Encode};

    use crate::accumulate::Buffer;
    use crate::schema::number;
    use crate::Serialize;

    /* -------------------------------------------------------------------------- Public Exports */

    /// On-disk **variant** identifier carried in the first byte of every segment header.
    ///
    /// Format extensibility may be achieved via the introduction of new segment variants in future
    /// releases. Existing variants are guaranteed to retain their discriminant values for binary
    /// compatibility with existing files.
    ///
    /// See the [module level documentation](self) for more details.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
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

    /* ------------------------------------------------------------------- Trait Implementations */

    impl Display for Variant {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Schema => write!(f, "Schema"),
                Self::Data => write!(f, "Data"),
            }
        }
    }

    impl TryFrom<u8> for Variant {
        type Error = Error;

        fn try_from(byte: u8) -> Result<Self, Error> {
            match byte {
                x if x == Self::Schema as u8 => Ok(Self::Schema),
                x if x == Self::Data as u8 => Ok(Self::Data),
                other => Error::Unknown { found: other }.into(),
            }
        }
    }

    impl Serialize for Variant {
        type Buffer = [u8; size_of::<u8>()];

        fn size(&self) -> Result<NonZeroU64, number::Error> {
            { *self as u8 }.size()
        }

        fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], number::Error> {
            { *self as u8 }.serialize_into(buf)
        }

        fn serialize(&self) -> Result<Self::Buffer, number::Error> {
            [u8::MIN; size_of::<u8>()].serialize_push(self)
        }
    }

    /* -------------------------------------------------------------------------- Specific Error */

    /// Errors returned by [`Variant`] parsing.
    ///
    /// Enum variants cover various granular error cases that may arise when working with segments.
    /// Users should consider handling errors explicitly wherever possible to provide meaningful
    /// error messages and recovery actions.
    ///
    /// ### Implementation
    ///
    /// This enum is `#[non_exhaustive]` meaning additional variants may be added in future versions.
    /// Implementers are advised to include a wildcard arm `_` to account for potential additions.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
    #[non_exhaustive] // To accommodate potential future error cases.
    pub enum Error {
        /// The actual variant byte did not match the [`Variant`] expected by the caller.
        #[n(0)]
        Unexpected {
            /// The [`Variant`] byte expected by the caller.
            #[n(0)]
            expected: u8,
            /// The actual [`Variant`] byte encountered by the caller.
            #[n(1)]
            found: u8,
        },
        /// The actual variant byte did not map to any known [`Variant`].
        #[n(1)]
        Unknown {
            /// The actual [`Variant`] byte encountered by the caller.
            #[n(0)]
            found: u8,
        },
    }

    impl Display for Error {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Error::Unexpected { found, .. } => write!(f, "Unexpected variant → 0x{found:02X}"),
                Error::Unknown { found } => write!(f, "Unknown variant → 0x{found:02X}"),
            }
        }
    }

    impl std::error::Error for Error {}

    impl From<u8> for Error {
        fn from(value: u8) -> Self {
            Self::Unknown { found: value }
        }
    }

    //noinspection DuplicatedCode → Conversion is implemented for error types in different modules.
    impl<T, E> From<Error> for Result<T, E>
    where
        E: From<Error>,
    {
        fn from(error: Error) -> Self {
            Err(E::from(error))
        }
    }
}

/* -------------------------------------------------------------------- Segment Trait Definition */

/// A self-describing on-disk **segment** prefixed by a [`Variant`] discriminant and an LE [`u64`]
/// size field, followed by a variant-specific payload. See the [module level documentation](self).
#[deprecated(note = "Segment trait is currently unneeded and available for repurposing.")]
pub trait Segment: Serialize {
    /// On-disk variant identifier for [`Self`]. Stored in the first byte of the segment header.
    const VARIANT: Variant;
}

/* ---------------------------------------------------------------- Segment Trait Implementation */

impl Serialize for Schema {
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, number::Error> {
        let size: u64 = { Header::SIZE + minicbor::len(self) }.try_into()?;
        size.try_into().map_err(number::Error::Convert)
    }

    fn serialize_into<'a>(&self, buf: &'a mut [u8]) -> Result<&'a mut [u8], number::Error> {
        let pad = { Header::SIZE + minicbor::len(self) }.pad()?;
        let buf = buf.serialize_push(&{ Variant::Schema as u8 })?;
        // NOTE: Self::size returns Error if usize overflows u64 (not expected in production)
        let mut buf = self.size()?.get().to_le_bytes().serialize_into(buf)?;
        // SAFETY: minicbor::encode is infallible when writing to Vec<u8>
        minicbor::encode(self, &mut buf).expect("Infallible encode failed");
        buf[..pad].fill(u8::MIN);
        Ok(&mut buf[pad..])
    }

    fn serialize(&self) -> Result<Self::Buffer, number::Error> {
        let size = self.size()?.get().try_into()?;
        let buf = vec![0u8; size].serialize_push(self)?;
        // NOTE: cannot use static assertion as size is dependent on runtime data accumulation.
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

/* --------------------------------------------------------------------------- Alignment Helpers */

/// A **numeric type** that can be rounded up to the next 64-bit SIMD [alignment boundary](self).
#[doc(hidden)] // Reachable through Serialize::serialize_into_aligned
pub trait Align {
    /// Round up `self` to the next 64-bit SIMD [alignment boundary](self).
    ///
    /// ### Errors
    ///
    /// Returns [`number::Error`] if `self` overflows `u64` or conversion into `u64` fails. Refer to
    /// each implementation for specific error cases.
    // NOTE: all on-disk sizes and offsets use u64; usize would truncate on 32-bit targets.
    fn align(self) -> Result<u64, number::Error>;

    /// Calculate the number of trailing zero bytes required to align `self` to the next 64-bit SIMD
    /// [alignment boundary](self).
    ///
    /// ### Errors
    ///
    /// Returns [`number::Error`] if `self` overflows `u64` or conversion into `u64` fails. Refer to
    /// each implementation for specific error cases.
    fn pad(self) -> Result<usize, number::Error>;
}

/// Blanket implementation covering all types that fallibly convert to [`u64`], enabling chained
/// alignment arithmetic such as `sector.next().ok_or(Error::Zero)?.align()?`.
impl<I> Align for I
where
    I: TryInto<u64>,
    number::Error: From<I::Error>,
{
    fn align(self) -> Result<u64, number::Error> {
        let n: u64 = self.try_into()? + 7;
        Ok(n & !7)
    }

    fn pad(self) -> Result<usize, number::Error> {
        let n: u64 = self.try_into()? & 7;
        // NOTE: pad ≤ seven bytes; conversion to usize is infallible in practice.
        { (8 - n) & 7 }.try_into().map_err(number::Error::Convert)
    }
}

/* ------------------------------------------------------------------------------ Specific Error */

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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[non_exhaustive] // To accommodate potential future error cases.
pub enum Error {
    /// Underlying [`variant::Error`] from a failed [`Variant`] parsing operation.
    #[n(0)]
    Variant(#[n(0)] variant::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Variant(error) => write!(f, "Segment variant ID error → {error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<variant::Error> for Error {
    fn from(error: variant::Error) -> Self {
        Self::Variant(error)
    }
}

impl From<Infallible> for Error {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

//noinspection DuplicatedCode → Conversion is implemented for error types in different modules.
impl<T, E> From<Error> for Result<T, E>
where
    E: From<Error>,
{
    fn from(error: Error) -> Self {
        Err(E::from(error))
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {}
