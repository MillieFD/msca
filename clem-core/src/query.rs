/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! A composable [`Query`] interface to [read](Read) data from any [clem](crate) file.
//!
//! ---
//!
//! Each new [`Query`] begins with **every** column and **every** buffer from the specified schema.
//! [`Filter`] functions are then applied subtractively to reduce the result set. Some filters are
//! evaluated eagerly **before** file IO; removing individual buffers or entire columns informed by
//! [manifest] statistics. Other filters are attached to the relevant column and evaluated lazily
//! **during** [deserialization](Deserialize). No file IO is executed until [`read`](Query::read)
//! is awaited.
//!
//! ```rust,ignore
//! let results = dataset
//!     .query("schema_name")?
//!     .select(["latitude", "longitude", "temperature"])
//!     .range("temperature", 10.0..=20.0)
//!     .eq("active", true)
//!     .read()
//!     .await?;
//! ```

#![doc = include_str!("../../doc/query-filters.md")]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{self, Display};
use std::num::NonZeroU32;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

use memmap2::Mmap;

use crate::io::{self, Deserialize};
use crate::manifest::{self, Buffer};
use crate::Serialize;

/* ------------------------------------------------------------------------------ Public Exports */

/// A composable query builder to [read](Read) data from any [clem](crate) file; initialised from
/// [`Dataset::query`][1] and executed when [`read`](Self::read) is [awaited][2].
///
/// Refer to the [module-level documentation](self) for implementation details and a list of
/// supported filters.
///
/// [1]: crate::Dataset::query
/// [2]: https://doc.rust-lang.org/book/ch17-00-async-await.html
// TODO → add derive attributes
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct Query {
    /// Read-only [memory map](Mmap) backed by the immutable segment region of a [clem](crate) file.
    ///
    /// Refer to the [safety documentation](io::File::mmap) for details.
    pub(crate) mmap: Arc<Mmap>,
    /// [`Column`] descriptors keyed by name.
    ///
    /// The [`BTreeMap`] guarantees a stable deterministic column order for consistent binary
    /// encoding and schema comparison.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
    /// Decimation factor applied to downsample the result set; defaults to 1 (keep all data).
    pub stride: NonZeroU32,
}


/* ----------------------------------------------------------------------------- Query Internals */

/// A minimal column **descriptor** for [`Query`] planning and execution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Encode, Decode, CborLen)]
pub struct Column {
    /// The [`Type`] of values contained within this [`Column`].
    #[n(0)]
    pub ty: Type,
    /// List of [`Buffer`] descriptors for this [`Column`] across all data segments.
    #[cbor(n(1), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    buffers: Vec<Buffer>,
    /// Deduplicated [`Filter`] set attached to this [`Column`] for lazy evaluation during
    /// [deserialization](Deserialize).
    #[cbor(n(2), skip_if = "HashSet::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "HashSet::is_empty")
    )]
    filters: HashSet<Filter>,
}

impl Column {
    /// Map the provided [`Key`](String) to a new empty [`Column`].
    pub(crate) fn map(entry: (&String, &manifest::Column)) -> (String, Self) {
        (entry.0.clone(), entry.1.clone().into())
    }

    /// Returns a new [`Decoder`] capable of [deserializing](Deserialize) bytes from each on-disk
    /// [`Buffer`] into instances of the requested [`Item`](I) type.
    ///
    /// The requested [`Type`] is evaluated against the on-disk [`Column`] type exactly once;
    /// enabling subsequent deserialization to occur fearlessly without additional runtime checks.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Type`] if the on-disk [`Type`] is incompatible with [`I`].
    fn reader<'a, I>(&'a self, mmap: &'a Mmap) -> Result<BoxRead<'a, I>, Error>
    where
        Schema: Unfolder<I>,
        Decoder<'a>: Read<I>,
    {
        let expected = Schema::unfold();
        match self.ty == expected {
            true => unimplemented!("Decoder construction path"),
            false => unimplemented!("Type mismatch error"),
        }
    }
}

impl From<manifest::Column> for Column {
    fn from(src: manifest::Column) -> Self {
        Self {
            ty: src.ty,
            buffers: src.buffers,
            filters: HashSet::new(),
        }
    }
}

/// A row-level predicate lazily evaluated during [deserialization](Deserialize).
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Hash, Encode, Decode, CborLen)]
pub(crate) enum Filter {
    /// Retain values within the specified range.
    #[n(0)]
    Range {
        /// Lower bound
        #[n(0)] // todo → CBOR and serde options e.g. skip_if
        // TODO → Vec is 24 (stack) + n (heap); u128 is 16 (stack); use [u8; 16] instead of Vec<u8>
        lb: Bound<Vec<u8>>,
        /// Upper bound
        #[n(1)] // todo → CBOR and serde options e.g. skip_if
        // TODO → Vec is 24 (stack) + n (heap); u128 is 16 (stack); use [u8; 16] instead of Vec<u8>
        ub: Bound<Vec<u8>>,
    },
}

impl Filter {
    /// Returns `true` if the provided [`Buffer`] is provably disjoint from the specified [`Range`].
    fn disjoint<D>(buf: &Buffer, range: Range<D>) -> Result<bool, io::Error>
    where
        D: for<'b> Deserialize<Src<'b> = &'b [u8]> + PartialOrd,
    {
        let min = D::deserialize(&buf.min)?;
        let max = D::deserialize(&buf.max)?;
        let above = match range.end_bound() {
            Bound::Included(h) => &min > h,
            Bound::Excluded(h) => &min >= h,
            Bound::Unbounded => false,
        };
        let below = match range.start_bound() {
            Bound::Included(l) => &max < l,
            Bound::Excluded(l) => &max <= l,
            Bound::Unbounded => false,
        };
        Ok(above || below)
    }
}

/// Serialize a [`Bound`] value into its binary representation for storage in a [`Filter`].
fn bytes<V: Serialize>(bound: Bound<&V>) -> Result<Bound<Vec<u8>>, number::Error> {
    match bound {
        Bound::Included(value) => Ok(Bound::Included(value.serialize()?.as_ref().to_vec())),
        Bound::Excluded(value) => Ok(Bound::Excluded(value.serialize()?.as_ref().to_vec())),
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

/* -------------------------------------------------------------------------------- Reader Trait */

/// A **composite type** that can build a [reader](BoxRead) reconstructing itself from the columns
/// of an open [`Query`].
///
/// Implementations are generated by `#[derive(Deserialize)]`; the generated reader holds one
/// [`column`](Query::column) decoder per field and assembles a value by pulling the next row from
/// each. Manual implementations are also supported.
pub trait Reader: Sized {
    /// Build a [reader](BoxRead) for [`Self`] by taking each field's column from `query` through
    /// [`Query::column`].
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Column`] if a required column is absent, or [`Error::Type`] if a column's
    /// on-disk [`Type`] is incompatible with the corresponding field.
    fn reader(query: &Query) -> Result<BoxRead<'_, Self>, Error>;
}

/* ------------------------------------------------------------------------------ Specific Error */

/// Errors returned from [`Query`] construction and execution.
///
/// Enum variants cover various granular error cases that may arise when working with queries.
/// Users should consider handling errors explicitly wherever possible to provide meaningful
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
    /// Underlying [`io::Error`] from the [clem](crate) [file](io::File).
    Io(io::Error),
    /// Underlying [`number::Error`] from a numerical operation or conversion.
    Number(number::Error),
    /// The requested [`Type`] did not match the actual on-disk [`Column`] type.
    Type {
        /// The [`Type`] expected by the caller.
        expected: Type,
        /// The actual on-disk column [`Type`].
        found: Type,
    },
    /// The requested [`Column`] name was not found in the query [`BTreeMap`].
    Column(String),
}

impl Error {
    /// Constructor for [`Error::Column`] wrapping the provided column [`name`]().
    pub(crate) fn column<S>(name: S) -> Self
    where
        String: From<S>,
    {
        let owned = name.into();
        Self::Column(owned)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "Query IO error → {e}"),
            Self::Number(e) => write!(f, "Number error → {e}"),
            Self::Type { expected, found } => write!(f, "Type error → {expected} ≠ {found}"),
            Self::Column(name) => write!(f, "Column '{name}' not found"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(src: io::Error) -> Self {
        match src {
            io::Error::Number(e) => e.into(), // Flatten number error nesting
            other => Self::Io(other),
        }
    }
}

impl From<number::Error> for Error {
    fn from(e: number::Error) -> Self {
        Self::Number(e)
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
