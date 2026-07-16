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
    fn unique<I, N>(&self) -> Result<HashMap<I, N, Xxh3Builder>, Error>
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
}
