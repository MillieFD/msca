/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! A composable [`Query`] interface to [read](Read) data from any [msca](crate) file.
//!
//! ---
//!
//! Each new [`Query`] begins with every [`Column`](column::Column) and every [`Buffer`] from the
//! specified [`Schema`]. Individual columns can be resolved and filtered to subtractively reduce
//! the result set. Some filters are evaluated eagerly **before** file IO; removing individual
//! [buffers](Buffer) using [manifest] statistics. Other filters are attached to read-time
//! [adapters](column::Adapter) and evaluated lazily **during** [deserialization](Deserialize).
//!
//! A [`Query`] is a factory for strongly-typed [`Column`](column::Column) handles over one schema,
//! plus a set of unfiltered composite conveniences. Extraction is selection: a column is read only
//! when a handle is opened for it via [`column`](Query::column). Filters live on the handle as
//! concrete typed state and are applied to each item **after** deserialization, so every item is
//! deserialized exactly once and every predicate is an infallible, statically-dispatched test.
//!
//! ```rust,ignore
//! let overheating = dataset
//!     .query("schema_name")?
//!     .column::<f64>("temperature")?
//!     .range(35.0..)?
//!     .read();
//! ```
//!
//! Items are deserialized exactly once. Every filter is an infallible monomorphized test. No file
//! [`IO`](io) is executed until the [`Iterator`] returned by a terminal method is polled.

#![doc = include_str!("../../../doc/query-filters.md")]
#![doc = include_str!("../../../doc/query-columns.md")]

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use std::hash::Hash;
use std::iter;
use std::num::{self, TryFromIntError};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use funty::Unsigned;
use memmap2::Mmap;
use minicbor::{CborLen, Decode, Encode};
use xxhash_rust::xxh3::Xxh3Builder;

use crate::io::{self, Deserialize};
use crate::manifest;
use crate::read::{Composite, Outcome, Read, Reader};
use crate::schema::{number, Schema, Type, Unfolder};

pub mod column;
pub mod stream;

/* --------------------------------------------------------------------------------------- Query */

/// A composable query interface to [read](Read) data from any [msca](crate) file; initialised from
/// [`Dataset::query`][1] and executed lazily when [`read`](Self::read) is iterated.
///
/// [`Query`] also provides a [`Column`](column::Column) factory for the specified [`Schema`].
///
/// Refer to the [module-level documentation](self) for implementation details.
///
/// [1]: crate::Dataset::query
#[derive(Clone, Debug)]
pub struct Query {
    /// Read-only [memory map](Mmap) backed by the immutable segment region.
    ///
    /// Refer to the [safety documentation](io::File::mmap) for details.
    pub(crate) mmap: Arc<Mmap>,
    /// [`Column`] descriptors keyed by name; cloned from the [manifest] at construction.
    ///
    /// [`BTreeMap`] guarantees a deterministic column order for consistent [serialisation][1] and
    /// [`Schema`] comparison.
    ///
    /// [1]: crate::accumulate::Serialize
    pub(crate) columns: BTreeMap<String, Column>,
}

impl Query {
    /// Generate a [`HashMap`] containing the position [index](N) for each unique on-disk [item](I).
    ///
    /// The [`Dataset`][1] is read in ascending insertion order; items record their first index and
    /// subsequent duplicate items are discarded.
    ///
    /// ### Errors
    ///
    /// - [`Error::Number`] if a first-occurrence index overflows [`N`].
    /// - [`Error::Io`] if a deserialization failure occurs.
    ///
    /// [1]: crate::dataset::Dataset
    pub fn unique<I, N>(&self) -> Result<HashMap<I, N, Xxh3Builder>, Error>
    where
        N: Unsigned,
        I: Read + Eq + Hash + 'static,
        for<'q> I::Src<'q>: Composite<'q, Query> + Iterator<Item = Outcome<I>> + 'q,
    {
        let mut map = HashMap::with_hasher(Xxh3Builder::new());
        let mut next = Some(N::MIN);
        for item in self.read::<I>()? {
            let i = next.ok_or(number::Error::Zero)?;
            if let Entry::Vacant(entry) = map.entry(item?) {
                entry.insert(i);
            }
            next = i.checked_add(N::ONE);
        }
        Ok(map)
    }

/* --------------------------------------------------------------------------- Column Descriptor */

/// A minimal column **descriptor** for [`Query`] planning and execution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Encode, Decode, CborLen)]
pub struct Column {
    /// The [`Type`] of items contained within this [`Column`].
    #[n(0)]
    pub ty: Type,
    /// [`Buffer`](manifest::Buffer) descriptors for the [`Column`] across all data segments.
    #[cbor(n(1), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub(crate) buffers: Vec<manifest::Buffer>,
}

impl Column {
    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type; otherwise returns an immutable reference to [`self`](Column) for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Column::accepts`] if a direct **or** nested inner-type match is permissible. Use
    /// [`Column::exact_mut`] if a mutable reference is required for downstream functions.
    pub fn exact<I>(&self) -> Result<&Self, Error>
    where
        Schema: Unfolder<I>,
    {
        let expect = Schema::unfold();
        match self.ty == expect {
            true => Ok(self),
            false => Error::Type { expect, actual: self.ty.clone() }.into(),
        }
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type; otherwise returns a mutable reference to [`self`](Column) for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Column::accepts`] if a direct **or** nested inner-type match is permissible. Use
    /// [`Column::exact`] if an immutable reference is required for downstream functions.
    pub fn exact_mut<I>(&mut self) -> Result<&mut Self, Error>
    where
        Schema: Unfolder<I>,
    {
        self.exact::<I>()?;
        Ok(self)
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type **or** nested inner subtype; otherwise returns an immutable reference to
    /// [`self`](Column) for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Type::exact`] if a direct non-nested match is required. Use
    /// [`Column::accepts_mut`] if a mutable reference is required for downstream functions.
    pub fn accepts<I>(&self) -> Result<&Self, Error>
    where
        Schema: Unfolder<I>,
    {
        let inner = Schema::unfold();
        match self.ty == inner || matches!(&self.ty, Type::Option { subtype: s } if **s == inner) {
            true => Ok(self),
            false => Error::Type { expect: inner, actual: self.ty.clone() }.into(),
        }
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type **or** nested inner subtype; otherwise returns a mutable reference to [`self`](Column)
    /// for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Type::exact`] if a direct non-nested match is required. Use [`Column::accepts`]
    /// if an immutable reference is required for downstream functions.
    pub fn accepts_mut<I>(&mut self) -> Result<&mut Self, Error>
    where
        Schema: Unfolder<I>,
    {
        self.accepts()?;
        Ok(self)
    }

    /// Map the provided [`Key`](String) to a new empty [`Column`].
    pub(crate) fn map(entry: (&String, &manifest::Column)) -> (String, Self) {
        (entry.0.clone(), entry.1.clone().into())
    }
}

impl From<manifest::Column> for Column {
    fn from(src: manifest::Column) -> Self {
        Self { ty: src.ty, buffers: src.buffers }
    }
}

/* --------------------------------------------------------------------------------- Query Error */

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
#[non_exhaustive] // accommodate potential future error cases
pub enum Error {
    /// The requested [`Column`] name was not found in the query [`BTreeMap`].
    Column {
        /// The requested [`Column`] name.
        name: String,
    },
    /// Underlying [`io::Error`] from the [msca](crate) [file](io::File).
    Io(io::Error),
    /// Underlying [`number::Error`] from a numerical operation or conversion.
    Number(number::Error),
    /// The requested [`Type`] did not match the actual on-disk [`Column`] type.
    Type {
        /// The [`Type`] expected by the caller.
        expect: Type,
        /// The actual on-disk column [`Type`].
        actual: Type,
    },
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column { name } => write!(f, "Column '{name}' not found"),
            Self::Io(e) => write!(f, "Query IO error → {e}"),
            Self::Number(e) => write!(f, "Number error → {e}"),
            Self::Type { expect, actual } => write!(f, "Type error → {expect} ≠ {actual}"),
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

impl From<TryFromIntError> for Error {
    fn from(e: TryFromIntError) -> Self {
        number::Error::from(e).into()
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
