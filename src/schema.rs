/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Type unfolding logic for schema generation and validation.
//!
//! ---
//!
//! ### Unfolding Arbitrary Types
//!
//! `clem` understands **platform-agnostic** primitive types such as `u32` or `f64` out of the box.
//! Platform-dependent types such as `usize` are deliberately omitted to ensure file portability.
//! Arbitrary user-defined algebraic data types (structs and enums) are [unfolded](Unfold) into
//! their primitive [components](Type).
//!
//! - **Leaf nodes** map to contiguous columnar data buffers by name.
//! - **Internal nodes** exist purely for navigation and reconstruction.
//!
//! ### Unsized Types
//!
//! It is not possible to predetermine the disk space required by each instance of an unsized type;
//! there is no guarantee that two [`Vec<T>`] contain the same number of elements. [`Clem`](crate)
//! therefore unfolds unsized types into:
//!
//! 1. Columnar `offsets` bufffer describing boundaries.
//! 2. Contiguous `data` buffer encoding values.
//!
//! This design ensures **O(1) random access** and avoids per-element pointer chasing. Sequential
//! scans across the contained elements remain linear; leveraging columnar optimisations for SIMD
//! and prefetch.
//!
//! ```text
//! offsets: [3, 6, 6]
//! values:  [a, b, c, d, e, f, g, h]
//! ```
//!
//! The serialized on-disk example above is deserialized into the memory representation below.
//! Implementers must specify which type to use for offset storage based on the number of expected
//! elements.
//!
//! ```text
//! Row 0 → values[..3] → "abc"
//! Row 1 → values[3..6] → "def"
//! Row 2 → values[6..6] → "" (empty)
//! Row 3 → values[6..] → "gh"
//! ```
//!
//! Nested unsized types use **multiple offset layers** alongside a **single data buffer**. This
//! composable design preserves the performance advantages associated with contiguous value storage;
//! namely predictable vectorised traversal. Scanning performance across the contiguous inner
//! `values` buffer is unaffected by deep nesting. The inner offsets buffer is aligned in memory
//! order of traversal to improve cache locality during nested iteration and reduce TLB misses.
//!
//! ```text
//! inner offsets
//! outer offsets
//! values
//! ```
//!
//! Readers can directly query data from a named field – without reconstructing the full type – by
//! reading only the required columnar data buffer. Each schema segment encodes **one** schema, and
//! each `clem` file requires at least **one** schema segment. Multimodality and schema evolution
//! are achieved by appending additional schema segments.

use std::collections::btree_map::{BTreeMap, Entry, OccupiedEntry, VacantEntry};
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::num;

use bitvec::vec::BitVec;
use minicbor::{CborLen, Decode, Encode};

use self::number::Number;
use crate::accumulate::{Accumulate, Flatten, OptBitVec, OptInSitu, OptSeq, Seq};

/// Shorthand [`OccupiedEntry`] for a [`Column`] that already exists in the [`Schema`].
type Occupied<'a> = OccupiedEntry<'a, String, Column>;

/// Shorthand [`VacantEntry`] for a [`Column`] that does not yet exist in the [`Schema`].
type Vacant<'a> = VacantEntry<'a, String, Column>;

/* ------------------------------------------------------------------------------ Public Exports */

/// A minimal schema **builder** wrapping a [`BTreeMap`] of [`Column`] descriptors keyed by name.
///
/// This type does **not** contain the actual schema definition or columnar data buffers; it is a
/// lightweight descriptor for segment initialisation without holding buffer contents in memory. An
/// on-disk schema segment encodes the schema definition (column names and types) while on-disk
/// data segments contain the columnar buffers.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
// NOTE: schema::Schema (public builder) ≠ manifest::Schema (private descriptor).
pub struct Schema {
    /// [`Column`] descriptors keyed by name.
    ///
    /// The [`BTreeMap`] guarantees a stable deterministic column order for consistent binary
    /// encoding and schema comparison.
    #[cbor(n(0), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
}

impl Schema {
    /// Initialises a new empty [`Schema`] with no columns.
    pub fn new() -> Self {
        Self { columns: BTreeMap::new() }
    }

    /// Add a [`Column`] to [`self`](Schema) with the specified `name` and [`type`](A).
    ///
    /// Returns an empty [`Accumulator`](acc) for in-memory data accumulation. This design ensures
    /// schema verification is performed exactly once.
    pub fn column<A, B>(&mut self, name: B) -> Result<Box<dyn Accumulate<Item = A>>, Error>
    where
        A: Unfold,
        Schema: Unfolder<A>,
        String: From<B>,
    {
        let name = String::from(name);
        let col = Column::new::<A, Schema>();
        match self.columns.entry(name) {
            Entry::Vacant(entry) => entry.insert(col),
            Entry::Occupied(entry) if entry.get() == &col => entry.into_mut(),
            Entry::Occupied(entry) => return Error::collision(entry, col.ty).into(),
        };
        Ok(A::RawAcc::boxed())
    }

    /// Map the provided `Key` to generated [`Default`] values of [`V`]
    pub(crate) fn map<V>(key: String) -> (String, V)
    where
        V: Default,
    {
        (key, V::default())
    }
}

/* ---------------------------------------------------------------------------- Schema Internals */

/// A minimal column **descriptor** that provides type metadata for reading and writing values.
///
/// `Column` does **not** contain the actual buffer data; it is a lightweight descriptor for
/// discovery and random access without holding buffer contents in memory. Data is stored via one
/// or more on-disk data segments, each of which contains a buffer for this column.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Column {
    /// The [`Type`] of values contained within this column.
    #[n(0)]
    ty: Type,
}

impl Column {
    fn new<T, U>() -> Self
    where
        T: Unfold,
        U: Unfolder<T>,
    {
        Self { ty: T::with_unfolder::<U>() }
    }
}

/// A minimal type **descriptor** that provides a stable and extensible representation for
/// platform-agnostic Rust primitives; used when walking the type graph for schema encoding.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[non_exhaustive] // To accommodate the potential future stabilisation of additional types.
pub enum Type {
    /* ----------------------------------------------------------- Fixed-Size Machine Primitives */
    /// Boolean primitive which can be `true` or `false`.
    #[n(0)]
    Bool,
    /// [Unicode scalar value][1] representing a single character primitive.
    ///
    /// [1]: https://www.unicode.org/glossary/#unicode_scalar_value
    #[n(1)]
    Char,
    /// Rust numeric primitives.
    #[n(2)]
    Number(#[n(0)] Number),
    /* --------------------------------------------------------- Fixed Size Container Primitives */
    /// Optional (nullable) value wrapping one subtype.
    #[n(3)]
    Option {
        /// [`Type`] of the subtype root node.
        #[n(0)]
        subtype: Box<Type>,
    },
    /// Fixed size tuple wrapping an arbitrary number of subtypes.
    #[n(4)]
    Tuple {
        /// [`Type`] of each subtype root node. [`Vec::len`] returns the arity.
        #[n(0)]
        subtypes: Vec<Type>,
    },
    /* ------------------------------------------------------------ Unsized Container Primitives */
    /// Variable length UTF-8 string encoded as a sequence of bytes.
    #[n(5)]
    String,
    /// Variable length homogenous sequence wrapping one subtype.
    #[n(6)]
    Sequence {
        /// [`Type`] of the subtype root node.
        #[n(0)]
        subtype: Box<Type>,
    },
}

impl Type {
    /// A [`Type::Number`] descriptor for the `f32` primitive type.
    pub const F32: Self = Self::Number(Number { kind: number::Kind::Float, size: 4 });

    /// A [`Type::Number`] descriptor for the `f64` primitive type.
    pub const F64: Self = Self::Number(Number { kind: number::Kind::Float, size: 8 });

    /// A [`Type::Number`] descriptor for the `i128` primitive type.
    pub const I128: Self = Self::Number(Number { kind: number::Kind::Int, size: 16 });

    /// A [`Type::Number`] descriptor for the `i16` primitive type.
    pub const I16: Self = Self::Number(Number { kind: number::Kind::Int, size: 2 });

    /// A [`Type::Number`] descriptor for the `i32` primitive type.
    pub const I32: Self = Self::Number(Number { kind: number::Kind::Int, size: 4 });

    /// A [`Type::Number`] descriptor for the `i64` primitive type.
    pub const I64: Self = Self::Number(Number { kind: number::Kind::Int, size: 8 });

    /// A [`Type::Number`] descriptor for the `i8` primitive type.
    pub const I8: Self = Self::Number(Number { kind: number::Kind::Int, size: 1 });

    /// A [`Number`](Number) descriptor for the [`NonZeroI128`](num::NonZeroI128) type.
    pub const NZI128: Self = Self::Number(Number { kind: number::Kind::NonZeroInt, size: 16 });

    /// A [`Number`](Number) descriptor for the [`NonZeroI16`](num::NonZeroI16) type.
    pub const NZI16: Self = Self::Number(Number { kind: number::Kind::NonZeroInt, size: 2 });

    /// A [`Number`](Number) descriptor for the [`NonZeroI32`](num::NonZeroI32) type.
    pub const NZI32: Self = Self::Number(Number { kind: number::Kind::NonZeroInt, size: 4 });

    /// A [`Number`](Number) descriptor for the [`NonZeroI64`](num::NonZeroI64) type.
    pub const NZI64: Self = Self::Number(Number { kind: number::Kind::NonZeroInt, size: 8 });

    /// A [`Number`](Number) descriptor for the [`NonZeroI8`](num::NonZeroI8) type.
    pub const NZI8: Self = Self::Number(Number { kind: number::Kind::NonZeroInt, size: 1 });

    /// A [`Number`](Number) descriptor for the [`NonZeroU128`](num::NonZeroU128) type.
    pub const NZU128: Self = Self::Number(Number { kind: number::Kind::NonZeroUInt, size: 16 });

    /// A [`Number`](Number) descriptor for the [`NonZeroU16`](num::NonZeroU16) type.
    pub const NZU16: Self = Self::Number(Number { kind: number::Kind::NonZeroUInt, size: 2 });

    /// A [`Number`](Number) descriptor for the [`NonZeroU32`](num::NonZeroU32) type.
    pub const NZU32: Self = Self::Number(Number { kind: number::Kind::NonZeroUInt, size: 4 });

    /// A [`Number`](Number) descriptor for the [`NonZeroU64`](num::NonZeroU64) type.
    pub const NZU64: Self = Self::Number(Number { kind: number::Kind::NonZeroUInt, size: 8 });

    /// A [`Number`](Number) descriptor for the [`NonZeroU8`](num::NonZeroU8) type.
    pub const NZU8: Self = Self::Number(Number { kind: number::Kind::NonZeroUInt, size: 1 });

    /// A [`Type::Number`] descriptor for the `u128` primitive type.
    pub const U128: Self = Self::Number(Number { kind: number::Kind::UInt, size: 16 });

    /// A [`Type::Number`] descriptor for the `u16` primitive type.
    pub const U16: Self = Self::Number(Number { kind: number::Kind::UInt, size: 2 });

    /// A [`Type::Number`] descriptor for the `u32` primitive type.
    pub const U32: Self = Self::Number(Number { kind: number::Kind::UInt, size: 4 });

    /// A [`Type::Number`] descriptor for the `u64` primitive type.
    pub const U64: Self = Self::Number(Number { kind: number::Kind::UInt, size: 8 });

    /// A [`Type::Number`] descriptor for the `u8` primitive type.
    pub const U8: Self = Self::Number(Number { kind: number::Kind::UInt, size: 1 });

    /// Constructor for [`Type::Option`] wrapping the provided subtype.
    pub fn option(subtype: Self) -> Self {
        match subtype {
            // 1. Flatten nested options. Single null bitmap improves on-disk efficiency.
            Self::Option { subtype } => Self::Option { subtype },
            // 2. Box non-option subtypes to prevent unbounded enum size from infinite recursion.
            #[rustfmt::skip] // Single line match arm improves readability
            subtype => Self::Option { subtype: Box::new(subtype) },
        }
    }

    /// Constructor for [`Type::Sequence`] wrapping the provided subtype.
    pub fn sequence(subtype: Self) -> Self {
        Self::Sequence { subtype: Box::new(subtype) }
    }
}

mod number {
    //! This module provides a minimal and extensible [`Number`] **descriptor** for Rust numeric
    //! primitives.
    //!
    //! Defining a distinct enum variant for each fixed-width machine primitive type is fragile; as
    //! Rust stabilises new types – such as [`f16`][1] – new enum variants would need to be added,
    //! which may break backwards compatibility and binary encoding.
    //!
    //! Instead, this module defines an extensible [`Number`] descriptor to encode arbitrary numeric
    //! types via a combination of [kind](Kind) and [size](size_of) fields.
    //!
    //! [1]: https://rust-lang.github.io/rfcs/3453-f16-and-f128.html

    use minicbor::{CborLen, Decode, Encode};
    use std::fmt::{Display, Formatter};

    /// Semantic classification of the numeric primitive type.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
    #[non_exhaustive] // To accommodate the potential stabilisation of additional numeric kinds.
    pub enum Kind {
        /* ---------------------------------------------------------------------------- Unsigned */
        /// Unsigned integer type.
        #[n(0)]
        UInt,
        /// [Non-zero](num::NonZero) unsigned integer type.
        #[n(1)]
        NonZeroUInt,
        /* ------------------------------------------------------------------------------ Signed */
        /// Signed integer type.
        #[n(2)]
        Int,
        /// [Non-zero](num::NonZero) signed integer type.
        #[n(3)]
        NonZeroInt,
        /* ---------------------------------------------------------------------- Floating Point */
        /// Floating point type.
        #[n(4)]
        Float,
    }

    impl Display for Kind {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::UInt => f.write_str("u"),
                Self::NonZeroUInt => f.write_str("NonZeroU"),
                Self::Int => f.write_str("i"),
                Self::NonZeroInt => f.write_str("NonZeroI"),
                Self::Float => f.write_str("f"),
            }
        }
    }

    /// A minimal and extensible runtime numeric type **descriptor** that specifies:
    ///
    /// 1. The [kind](Kind) of number.
    /// 2. The [size](size_of) of each value in bytes.
    ///
    /// This type does **not** contain the actual numeric value; it is a lightweight descriptor for
    /// numeric type information without holding values in memory. Each unique combination of `Kind`
    /// and `bytes` corresponds to a specific Rust numeric primitive type.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
    pub struct Number {
        /// Semantic classification of the numeric primitive type.
        #[n(0)]
        pub kind: Kind,
        /// Number of bytes used to encode each value.
        #[n(1)]
        pub size: u8,
    }

    impl Display for Number {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}{}", self.kind, self.size * 8)
        }
    }
}

/* ------------------------------------------------------------------------------ Specific Error */

/// Errors returned by [`Schema`] composition.
///
/// Enum variants cover various granular error cases that may arise when working with schemas.
/// Users should consider handling errors explicitly wherever possible to provide meaningful error
/// messages and recovery actions.
///
/// ### Implementation
///
/// This enum is `#[non_exhaustive]` meaning additional variants may be added in future versions.
/// Implementers are advised to include a wildcard arm `_` to account for potential additions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[non_exhaustive] // To accommodate potential future error cases.
pub enum Error {
    /// A [`Column`] with the same [name](String) but a different [type](Type) already exists in
    /// the [`Schema`].
    ///
    /// Each schema stores columns in a [`BTreeMap`] keyed by column name. Reusing an existing
    /// name therefore overwrites the existing column definition, resulting in possible data loss.
    #[n(0)]
    Collision {
        /// Name shared by the new and existing columns.
        #[n(0)]
        name: String,
        /// [`Type`] of the existing [`Column`] in the [`Schema`].
        #[n(1)]
        existing: Type,
        /// [`Type`] of the new [`Column`] being added to the [`Schema`].
        #[n(2)]
        new: Type,
    },
    /// The requested type is not supported by this version of [`clem`](crate).
    ///
    /// Some types are deliberately omitted. Please read the [type documentation](Type) for more
    /// details. If you think a type should be supported, please open a new GitHub feature request
    /// with your use case and justification for inclusion.
    #[n(1)]
    Unsupported(#[n(0)] &'static str),
}

impl Error {
    /// Returns a new [`Error::Collision`] variant wrapping the column name and conflicting types.
    fn collision(occupied: Occupied, new: Type) -> Self {
        let name = occupied.key().clone();
        let existing = occupied.get().clone().ty;
        Self::Collision { name, existing, new }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Collision { name, .. } => write!(f, "Column '{name}' already in schema"),
            Self::Unsupported(msg) => write!(f, "Unsupported type → {msg}"),
        }
    }
}

impl std::error::Error for Error {}

//noinspection DuplicatedCode → Conversion is implemented for error types across different modules.
impl<T, E> From<Error> for Result<T, E>
where
    E: From<Error>,
{
    fn from(error: Error) -> Self {
        Err(E::from(error))
    }
}

/* --------------------------------------------------------------------- Unfold Trait Definition */

/// A platform-agnostic **type** that can be unfolded into its primitive [components](Type) using
/// an [`Unfolder`].
///
/// [`Clem`](crate) provides `Unfold` implementations for many Rust primitive and standard library
/// types. The complete list is [here](crate::schema). All of these types can be unfolded using clem
/// out of the box. Some types are deliberately omitted to preserve cross-platform support.
///
/// Clem provides the [`#[derive(unfold)]`][1] procedural macro to automatically generate `Unfold`
/// implementations for structs and enums in your program. See the [user guide][2] for more details.
///
/// Third-party crates are encouraged to implement `Unfold` on their public types to enable seamless
/// integration with on-disk storage.
// TODO [1] link to procedural macro documentation
// TODO [2] link to procedural macro user guide
pub(crate) trait Unfold: Sized {
    /// The [accumulator](Accumulate) type used to ingest unwrapped values of [`Self`].
    type RawAcc: Accumulate<Item = Self> + 'static;

    /// The [accumulator](Accumulate) type used to ingest [optional](Option) values of [`Self`].
    type OptAcc: Accumulate<Item = Option<Self>> + 'static;

    /// Delegates to [`unfold`](Unfolder::unfold) on the provided [`Unfolder`].
    fn with_unfolder<U>() -> Type
    where
        U: Unfolder<Self>,
    {
        U::unfold()
    }
}

/* ----------------------------------------------------------------- Unfold Trait Implementation */

impl Unfold for bool {
    type RawAcc = BitVec;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for char {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for u8 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for u16 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for u32 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for u64 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for u128 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for num::NonZeroU8 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroU16 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroU32 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroU64 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroU128 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for i8 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for i16 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for i32 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for i64 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for i128 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for num::NonZeroI8 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroI16 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroI32 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroI64 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for num::NonZeroI128 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptInSitu<Self>;
}

impl Unfold for f32 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl Unfold for f64 {
    type RawAcc = Vec<Self>;
    type OptAcc = OptBitVec<Self>;
}

impl<T> Unfold for Option<T>
where
    T: Unfold,
{
    type RawAcc = T::OptAcc;
    type OptAcc = Flatten<T::OptAcc>;
}

impl<T> Unfold for Vec<T>
where
    T: Unfold,
{
    type RawAcc = Seq<T>;
    type OptAcc = OptSeq<T>;
}

/* ------------------------------------------------------------------- Unfolder Trait Definition */

/// A **schema builder** that can unfold the supported type [`T`].
///
/// `Unfolder` is implemented independently for each supported type; enabling type-driven encoding.
/// For example, the default [`Schema`] builder unfolds `u8` into a [`Type::Number`] descriptor.
pub trait Unfolder<T>
where
    T: ?Sized,
{
    /// Specific unfolding logic for the supported type `T`.
    fn unfold() -> Type;
}

/* --------------------------------------------------------------- Unfolder Trait Implementation */

impl Unfolder<bool> for Schema {
    fn unfold() -> Type {
        Type::Bool
    }
}

impl Unfolder<char> for Schema {
    fn unfold() -> Type {
        Type::Char
    }
}

impl Unfolder<u8> for Schema {
    fn unfold() -> Type {
        Type::U8
    }
}

impl Unfolder<u16> for Schema {
    fn unfold() -> Type {
        Type::U16
    }
}

impl Unfolder<u32> for Schema {
    fn unfold() -> Type {
        Type::U32
    }
}

impl Unfolder<u64> for Schema {
    fn unfold() -> Type {
        Type::U64
    }
}

impl Unfolder<u128> for Schema {
    fn unfold() -> Type {
        Type::U128
    }
}

impl Unfolder<num::NonZeroU8> for Schema {
    fn unfold() -> Type {
        Type::NZU8
    }
}

impl Unfolder<num::NonZeroU16> for Schema {
    fn unfold() -> Type {
        Type::NZU16
    }
}

impl Unfolder<num::NonZeroU32> for Schema {
    fn unfold() -> Type {
        Type::NZU32
    }
}

impl Unfolder<num::NonZeroU64> for Schema {
    fn unfold() -> Type {
        Type::NZU64
    }
}

impl Unfolder<num::NonZeroU128> for Schema {
    fn unfold() -> Type {
        Type::NZU128
    }
}

impl Unfolder<i8> for Schema {
    fn unfold() -> Type {
        Type::I8
    }
}

impl Unfolder<i16> for Schema {
    fn unfold() -> Type {
        Type::I16
    }
}

impl Unfolder<i32> for Schema {
    fn unfold() -> Type {
        Type::I32
    }
}

impl Unfolder<i64> for Schema {
    fn unfold() -> Type {
        Type::I64
    }
}

impl Unfolder<i128> for Schema {
    fn unfold() -> Type {
        Type::I128
    }
}

impl Unfolder<num::NonZeroI8> for Schema {
    fn unfold() -> Type {
        Type::NZI8
    }
}

impl Unfolder<num::NonZeroI16> for Schema {
    fn unfold() -> Type {
        Type::NZI16
    }
}

impl Unfolder<num::NonZeroI32> for Schema {
    fn unfold() -> Type {
        Type::NZI32
    }
}

impl Unfolder<num::NonZeroI64> for Schema {
    fn unfold() -> Type {
        Type::NZI64
    }
}

impl Unfolder<num::NonZeroI128> for Schema {
    fn unfold() -> Type {
        Type::NZI128
    }
}

impl Unfolder<f32> for Schema {
    fn unfold() -> Type {
        Type::F32
    }
}

impl Unfolder<f64> for Schema {
    fn unfold() -> Type {
        Type::F64
    }
}

impl<T: Unfold> Unfolder<Option<T>> for Schema
where
    Schema: Unfolder<T, Ok = Type>,
{
    type Ok = Type;
    type Error = Infallible;

    fn unfold(&mut self) -> Result<Self::Ok, Self::Error> {
        Type::option(T::with_unfolder(self)?).into()
    }
}

impl<T: Unfold> Unfolder<Vec<T>> for Schema
where
    Schema: Unfolder<T, Ok = Type>,
{
    type Ok = Type;
    type Error = Infallible;

    fn unfold(&mut self) -> Result<Self::Ok, Self::Error> {
        Type::sequence(T::with_unfolder(self)?).into()
    }
}
