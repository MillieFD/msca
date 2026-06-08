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
use std::num::NonZeroU32;
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

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {}
