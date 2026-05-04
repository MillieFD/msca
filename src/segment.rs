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

use crate::Sector;
pub use data::{Buffer, Data, buffer};
pub use schema::Schema;

mod variant {
    //! todo module doc comment

    use minicbor::{CborLen, Decode, Encode};
    use std::fmt::{Display, Formatter};

    /* -------------------------------------------------------------------------- Public Exports */

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

    /* -------------------------------------------------------------------------- Specific Error */

    /// Errors returned by [`Variant`] parsing.
    ///
    /// Enum variants cover various granular error cases that may arise when working with segments.
    /// Users should consider handling errors explicitly wherever possible to provide meaningful
    /// error messages and recovery actions.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
    pub enum Error {
        /// The actual variant byte did not match the [`Variant`] expected by the caller.
        #[n(0)]
        Unexpected {
            /// The [`Variant`] expected by the caller.
            #[n(0)]
            expected: Variant,
            /// The actual variant byte encountered by the caller.
            #[n(1)]
            found: Variant,
        },
        /// The actual variant byte did not map to any known [`Variant`].
        #[n(1)]
        Unknown {
            /// The actual variant byte encountered by the caller.
            #[n(0)]
            found: u8,
        },
    }

    impl Display for Error {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Error::Unexpected { expected, found } => write!(
                    f,
                    "Unexpected segment variant:\n\t\
                    Expected {expected:?}\n\t\
                    Found {found:?}"
                ),
                Error::Unknown { found } => write!(f, "Unknown segment variant → 0x{found:02X}"),
            }
        }
    }

    impl std::error::Error for Error {}

    impl From<u8> for Error {
        fn from(value: u8) -> Self {
            Self::Unknown { found: value }
        }
    }
}

/* --------------------------------------------------------------------------- Alignment Helpers */

/// Round `n` up to the next multiple of eight; the unit of [critical-field alignment](self).
pub(crate) const fn align(n: usize) -> usize {
    (n + 7) & !7
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
    /// A read operation attempted to access bytes beyond the end of the input slice.
    #[n(0)]
    Truncated(#[n(0)] Sector),
    /// Underlying [`variant::Error`] from a failed [`Variant`] parsing operation.
    #[n(1)]
    Variant(#[n(0)] variant::Error),
    /// Attempted to decode a zero value into a [`NonZero`](num::NonZero) field.
    #[n(2)]
    Zero,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated(sector) => write!(f, "Read from {sector:?} was truncated"),
            Self::Variant(error) => write!(f, "Segment variant ID error → {error}"),
            Self::Zero => write!(f, "Expected non-zero value was zero"),
            other => write!(f, "Unexpected segment error → {other:?}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<variant::Error> for Error {
    fn from(error: variant::Error) -> Self {
        Self::Variant(error)
    }
}

//noinspection DuplicatedCode
impl<T, E> From<Error> for Result<T, E>
where
    E: From<Error>,
{
    fn from(error: Error) -> Self {
        Err(E::from(error))
    }
}

/* ------------------------------------------------------------------ Serialize Trait Definition */

/// A **type** that can be serialized into a canonical [`clem`](crate) byte representation for
/// on-disk storage.
pub trait Serialize {
    /// Returns the number of bytes required to encode `self`.
    #[rustfmt::skip] // Single line default fn body improves readability.
    fn size(&self) -> usize { size_of::<Self>() }

    /// [`Serialize`](Self::serialize) `self` and append the encoded bytes to the provided buffer.
    fn serialize_into(&self, buf: &mut Vec<u8>);

    /// Serialize `self` and return the encoded bytes as a tight [`Box<[u8]>`].
    fn serialize(&self) -> Box<[u8]> {
        let size = self.size();
        let mut buf = Vec::with_capacity(size);
        self.serialize_into(&mut buf);
        debug_assert_eq!(buf.len(), size, "Actual size ≠ predicted size");
        buf.into_boxed_slice()
    }
}

/* -------------------------------------------------------------- Serialize Trait Implementation */

impl Serialize for char {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        debug_assert_eq!(size_of::<Self>(), size_of::<u32>(), "char is not 4 bytes");
        u32::from(*self).serialize_into(buf);
    }
}

impl Serialize for u8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }
}

impl Serialize for u16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for u32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for u64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for u128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for num::NonZeroU8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroU128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for i8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for i128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for num::NonZeroI8 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI16 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for num::NonZeroI128 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.get().serialize_into(buf);
    }
}

impl Serialize for f32 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for f64 {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        let bytes = self.to_le_bytes();
        buf.extend_from_slice(&bytes);
    }
}

impl Serialize for Option<char> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        // None writes u32::MAX (outside the valid scalar range).
        self.map_or(u32::MAX, u32::from).serialize_into(buf);
    }
}

impl Serialize for Option<num::NonZeroU8> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u8.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU16> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u16.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU32> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u32.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU64> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u64.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroU128> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0u128.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI8> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i8.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI16> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i16.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI32> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i32.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI64> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i64.serialize_into(buf),
        }
    }
}

impl Serialize for Option<num::NonZeroI128> {
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            Some(n) => n.serialize_into(buf),
            None => 0i128.serialize_into(buf),
        }
    }
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    fn size(&self) -> usize {
        self.iter().map(T::size).sum()
    }
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        self.iter().for_each(|v| v.serialize_into(buf))
    }
}
