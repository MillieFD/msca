/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Core library for the [`clem`] storage engine.

mod accumulate;
mod dataset;
mod error;
mod io;
mod manifest;
mod query;
mod schema;
mod segment;

/* ----------------------------------------------------------------------------- Private Imports */

use std::num::{NonZeroU128, NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8};

use self::accumulate::Serialize;
use self::io::Deserialize;
use self::schema::number;

/* ------------------------------------------------------------------------------ Public Exports */

pub use self::error::Error;
pub use self::io::Sector;

/* --------------------------------------------------------------------- Record Trait Definition */

/// A user-defined type that describes its own schema for storage in a [`clem`](crate)
/// [`Dataset`].
///
/// Implementations are typically generated automatically by a procedural derive macro on user
/// structs and enums; manual implementations are also supported for advanced use cases.
// todo → rename trait
pub trait Record {
    /// Constructs a [`Schema`](schema::Schema) describing the layout of [`Self`].
    fn schema() -> schema::Schema;
}

/* ---------------------------------------------------------------- NonZeroUInt Trait Definition */

/// Marker trait for unsigned [`non-zero`](core::num::nonzero::NonZero) integer types.
pub trait NonZeroUInt: Copy + Ord + Sized {
    /// A constant representing the multiplicative identity element for the implementing type.
    ///
    /// Multiplication by this constant should leave any compatible instance of the type unchanged.
    ///
    /// ### Example
    ///
    /// ```rust,ignore
    /// fn noop<T: NonZeroUInt>(value: T) {
    ///     assert_eq!(T::ONE * value, value);
    ///     assert_eq!(value * T::ONE, value);
    /// }
    /// ```
    ///
    /// [`Self::ONE`] represents the minimum permissible value of the implementing type.
    const ONE: Self;
}

/* ------------------------------------------------------------ NonZeroUInt Trait Implementation */

impl NonZeroUInt for NonZeroU8 {
    const ONE: Self = Self::MIN;
}

impl NonZeroUInt for NonZeroU16 {
    const ONE: Self = Self::MIN;
}

impl NonZeroUInt for NonZeroU32 {
    const ONE: Self = Self::MIN;
}

impl NonZeroUInt for NonZeroU64 {
    const ONE: Self = Self::MIN;
}

impl NonZeroUInt for NonZeroU128 {
    const ONE: Self = Self::MIN;
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_zero_uint_ord() {
        assert!(NonZeroU8::MIN < NonZeroU8::MAX);
        assert!(NonZeroU16::MIN < NonZeroU16::MAX);
        assert!(NonZeroU32::MIN < NonZeroU32::MAX);
        assert!(NonZeroU64::MIN < NonZeroU64::MAX);
        assert!(NonZeroU128::MIN < NonZeroU128::MAX);
    }

    #[test]
    fn non_zero_uint_one() {
        assert_eq!(NonZeroU8::ONE.get() * NonZeroU8::new(2).unwrap().get(), 2);
        assert_eq!(NonZeroU16::ONE.get() * NonZeroU16::new(2).unwrap().get(), 2);
        assert_eq!(NonZeroU32::ONE.get() * NonZeroU32::new(2).unwrap().get(), 2);
        assert_eq!(NonZeroU64::ONE.get() * NonZeroU64::new(2).unwrap().get(), 2);
    }

    #[test]
    fn niche_optimisation() {
        assert_eq!(size_of::<Option<NonZeroU8>>(), size_of::<NonZeroU8>());
        assert_eq!(size_of::<Option<NonZeroU16>>(), size_of::<NonZeroU16>());
        assert_eq!(size_of::<Option<NonZeroU32>>(), size_of::<NonZeroU32>());
        assert_eq!(size_of::<Option<NonZeroU64>>(), size_of::<NonZeroU64>());
        assert_eq!(size_of::<Option<NonZeroU128>>(), size_of::<NonZeroU128>());
    }
}
