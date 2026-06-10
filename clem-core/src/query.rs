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
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use memmap2::Mmap;
use minicbor::{CborLen, Decode, Encode};

use crate::accumulate::Buffer;
use crate::io::{self, Deserialize, Deserializer};
use crate::manifest::{self, B};
use crate::read::{BoxRead, Decoder, Read};
use crate::schema::{number, Schema, Type, Unfolder};
use crate::{Reader, Serialize};

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

impl Query {
    /// Returns an [`Iterator`] over [`deserialized`][1] [`items`](I) from the [`Query`] result set.
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if a required column is not found in the query [`BTreeMap`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    ///
    /// [1]: Deserialize::deserialize
    pub fn read<I>(&self) -> Result<impl Iterator<Item = Result<I, io::Error>> + '_, Error>
    where
        I: Reader + 'static,
    {
        let read = I::reader(self)?.rows().step_by(self.stride.get() as usize);
        Ok(read)
    }

    /// Drain the [`Query`] result set into an owned [`Vec`] of [`deserialized`][1] [`items`](I).
    ///
    /// ### Errors
    ///
    /// See [`Query::read`] for a description of the error conditions that may arise during setup.
    /// Returns [`Error::Io`] if a file IO or deserialization error occurs during iteration.
    ///
    /// [1]: Deserialize::deserialize
    pub async fn collect<I>(self) -> Result<Vec<I>, Error>
    where
        I: Reader + 'static,
    {
        self.read::<I>()?.collect::<Result<Vec<I>, io::Error>>().map_err(Error::from)
    }

    /* --------------------------------------------------------------------------- Query Filters */

    /// A [`Query`] retains all columns defined by the [`Schema`] unless otherwise specified. The
    /// `select` filter restricts the returned columns to a named subset, reducing file IO to only
    /// the required buffers.
    ///
    /// ```rust,ignore
    /// .select(["a", "b"]) // Return only columns "a" and "b"
    /// ```
    ///
    /// Any [`Column`] omitted from `select` is never read from disk; the primary mechanism to
    /// reduce file IO on wide schemas. Omitting `select` is equivalent to selecting every column.
    ///
    /// Refer to the [module-level documentation](self) for more details.
    pub fn select<N, S>(mut self, names: N) -> Self
    where
        N: IntoIterator<Item = S>,
        String: From<S>,
    {
        let keep: BTreeSet<String> = names.into_iter().map(String::from).collect();
        self.columns.retain(|name, column| keep.contains(name));
        self // return to builder pattern
    }

    /// Retain rows from the named [`Column`] only if the deserialized [`Item`](I) value falls
    /// within the specified [`Range`](RangeBounds). Excluded rows are removed from all columns.
    ///
    /// `range` is a **mixed** filter:
    /// 1. Eagerly evaluated **before** IO using [`Buffer`] statistics.
    /// 2. Lazily evaluated **during** [deserialization](Deserialize) to filter individual rows.
    ///
    /// ```rust,ignore
    /// .range("temperature", 10..20) // 10.0 ≤ temperature < 20.0 inclusive range
    /// .range("altitude", 100..=500) // inclusive upper bound on additonal column
    /// ```
    ///
    /// Open or half-open ranges are also supported:
    ///
    /// ```rust,ignore
    /// .range("pressure", 101.3..) // pressure ≥ 101.3  (no upper bound)
    /// .range("pressure", ..105.0) // pressure < 105.0  (no lower bound)
    /// ```
    ///
    /// Refer to the [module-level documentation](self) for more details.
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the column's on-disk [`Type`] is incompatible with [`I`].
    /// - [`Error::Io`] if an error occurs during [deserialization](Deserialize).
    pub fn range<I, B>(mut self, name: &str, bounds: B) -> Result<Self, Error>
    where
        I: Serialize + for<'a> Deserialize<Src<'a> = &'a [u8]> + PartialOrd,
        B: RangeBounds<I>,
        Schema: Unfolder<I>,
    {
        let col = self.columns.get_mut(name).ok_or_else(|| Error::column(name))?.verify::<I>()?;
        let filter = Filter::bounds(&bounds);
        col.filters.insert(filter);
        let n = col.buffers.len();
        let mut keep = col
            .buffers
            .iter()
            .try_fold(Vec::with_capacity(n), |mut acc, buf| unsafe {
                acc.push(buf.disjoint(&bounds)?);
                Ok::<Vec<bool>, Error>(acc)
            })?
            .into_iter()
            .cycle();
        for column in self.columns.values_mut() {
            column.buffers.retain(|buf| keep.next().unwrap_or(false))
        }
        Ok(self)
    }

    /// Sample every nth row from the result set. Useful for decimation and preview reads on dense
    /// time-series data.
    ///
    /// ```rust,ignore
    /// .stride(10) // return every 10th row
    /// ```
    ///
    /// The default stride value `1` includes every row after filtering.
    pub fn stride(mut self, n: u32) -> Self {
        self.stride = NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN);
        self // return to builder pattern
    }
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
    buffers: Vec<manifest::Buffer>,
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
            false => Error::Type { expected, found: self.ty.clone() }.into(),
        }
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type; otherwise returns [`Ok`](Ok)`(`[`Self`](Column)`)` unmodified for method chaining.
    fn verify<I>(&mut self) -> Result<&mut Self, Error>
    where
        Schema: Unfolder<I>,
    {
        let expected = Schema::unfold();
        match self.ty == expected {
            true => Ok(self),
            false => Error::Type { expected, found: self.ty.clone() }.into(),
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
#[non_exhaustive] // To accommodate potential future filter types.
pub(crate) enum Filter {
    /// Retain values within the specified range.
    #[n(0)]
    Range {
        /// Lower bound
        #[n(0)]
        lb: Bound<[u8; B]>,
        /// Upper bound
        #[n(1)]
        ub: Bound<[u8; B]>,
    },
}

impl Filter {
    /// Construct a [`Filter::Range`] from the provided [`range`](RangeBounds).
    fn bounds<B, I>(range: &B) -> Self
    where
        B: RangeBounds<I>,
        I: Serialize,
    {
        Self::Range {
            lb: range.start_bound().map(|v| [u8::MIN; B].serialize_push(v).unwrap_or([u8::MIN; B])),
            ub: range.end_bound().map(|v| [u8::MAX; B].serialize_push(v).unwrap_or([u8::MAX; B])),
        }
    }

    /// Returns `true` if the [`item`](I) satisfies [`self`](Filter).
    pub(crate) fn evaluate<I, S>(&self, item: &I) -> Result<bool, io::Error>
    where
        I: for<'a> Deserialize<Src<'a> = &'a [u8]> + PartialOrd,
    {
        // NOTE: This dispatch function will grow as new filter variants are added
        match self {
            Self::Range { lb, ub } => Filter::contains(lb, ub, item),
        }
    }

    /// Returns `true` if the [`item`](I) is contained within the specified [`Range`](RangeBounds).
    pub(crate) fn contains<I, S>(lb: &Bound<S>, ub: &Bound<S>, item: &I) -> Result<bool, io::Error>
    where
        I: for<'a> Deserialize<Src<'a> = &'a [u8]> + PartialOrd,
        S: AsRef<[u8]>,
    {
        let above = match lb {
            Bound::Included(bytes) => *item >= bytes.as_ref().deserialize_into()?,
            Bound::Excluded(bytes) => *item > bytes.as_ref().deserialize_into()?,
            Bound::Unbounded => true,
        };
        let below = match ub {
            Bound::Included(bytes) => *item <= bytes.as_ref().deserialize_into()?,
            Bound::Excluded(bytes) => *item < bytes.as_ref().deserialize_into()?,
            Bound::Unbounded => true,
        };
        Ok(above && below)
    }
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
