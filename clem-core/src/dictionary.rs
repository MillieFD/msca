/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

#![doc = include_str!("../../doc/dictionary.md")]

use std::collections::btree_map::{BTreeMap, Entry};
use std::fmt;
use std::num::NonZeroU64;

use crate::accumulate::{Accumulate, Buffer, Seq, Serialize};
use crate::io::{Header, Write};
use crate::schema::{number, Unfold};
use crate::segment::Variant;
use crate::{schema, Sector};

/* ------------------------------------------------------------------------------ Public Exports */

/// A minimal **in-memory accumulator** for large [`items`](I) indexed by small unique [`keys`](K).
#[doc = include_str!("../../doc/dictionary.md")]
pub struct Dictionary<K, I>
where
    K: Unfold + Ord,
    I: Serialize,
{
    /// Sorted [`items`](I) indexed by small unique [`keys`](K).
    pub(crate) entries: BTreeMap<K, I>,
    /// [`Sector`] of the bound dictionary schema segment.
    pub(crate) schema: Sector,
}

impl<K, I> Dictionary<K, I>
where
    K: Unfold + Ord,
    I: Serialize,
{
    /// Insert a new entry pair into the [`Dictionary`].
    ///
    /// - Returns [`None`] if the map did not contain this key.
    /// - Returns [`Some`] wrapping the previous value if the key was present.
    ///
    /// Refer to [`BTreeMap::insert`] documentation for more details. Use [`checked_insert`][1] for
    /// cases where overwriting existing values is undesirable and should return an error.
    ///
    /// [1]: Self::checked_insert
    pub fn insert(&mut self, key: K, item: I) -> Option<I> {
        self.entries.insert(key, item)
    }

    /// Insert a new entry into the [`Dictionary`].
    ///
    /// Returns an immutable reference to the inserted [`item`](I) on success, or [`Error`] if the
    /// [`map`](BTreeMap) already contains the specified [`key`](K). Existing items are **not**
    /// overwritten.
    ///
    /// Use [`insert`](Self::insert) for cases where updating existing values in situ is desired.
    pub fn checked_insert(&mut self, key: K, value: I) -> Result<&I, Error> {
        match self.entries.entry(key) {
            Entry::Vacant(entry) => Ok(&*entry.insert(value)),
            Entry::Occupied(entry) => Error::Collision.into(),
        }
    }

    /// Returns the number of accumulated entries.
    pub fn count(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Returns `true` if the [`Dictionary`] contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Reinitialise the [`Dictionary`] without writing to disk. All data is permanently lost.
    ///
    /// Note that this method may not affect the allocated capacity of the underlying storage.
    pub fn discard(&mut self) {
        self.entries.clear();
    }
}
