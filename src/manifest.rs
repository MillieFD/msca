/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! A [`manifest`] footer lists file [`segments`] by type. Data segments are grouped by schema
//! alongside segment-level statistics e.g. min and max values. The manifest acts like the index
//! of a book to enhance:
//!
//! - Segment discovery
//! - Random access
//! - Predicate pruning
//!
//! The manifest is encoded as **CBOR** with definite-length text maps to enable schema and column
//! access by name. A `metadata` key is included when user-specified file-level metadata is present.
//! The manifest is moved and updated when new segments are added.
//!
//! ```text
//! Manifest
//! ├─ metadata (optional)
//! ├─ dictionaries: BTreeMap (optional)
//! └─ schemas: BTreeMap
//! ├─ <schema-name>
//! │  ├─ sector: Sector
//! │  └─ columns: BTreeMap
//! │     ├─ <column-name>
//! │     │  └─ buffers: [Buffer]
//! │     │     ├─ sector: Sector
//! │     │     ├─ count: NonZeroU32
//! │     │     ├─ min: T where T: Ord
//! │     │     └─ max: T where T: Ord
//! │     ⋮
//! │     └─ <final-column>
//! ⋮
//! └─ <final-schema>
//! ```
//!
//! Schema lookup by name returns the corresponding schema segment and a map of all schema columns.
//! A `BTreeMap<String, Schema>` sorted in lexicographic order is used to ensure a fully
//! deterministic layout regardless of insertion order.
//!
//! ```text
//! manifest["schema_name"] → Schema { segment: Segment, columns: BTreeMap<String, Column> }
//! ```
//!
//! Column lookup by name returns the corresponding collection of buffers across all on-disk data
//! segments.
//!
//! ```text
//! manifest["schema_name"]["column_name"] → [Buffer]
//! ```
//!
//! Each `Buffer` contains a `sector: Sector` alongside data statistics such as `min` and `max` for
//! predicate pruning.

use std::collections::BTreeMap;
use std::collections::btree_map::{Entry, OccupiedEntry};
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroU64;

use minicbor::{CborLen, Decode, Encode};
use smol::io::{AsyncRead, AsyncReadExt};

use crate::{Deserialize, Sector, Serialize, accumulate, io};

/// Shorthand [`OccupiedEntry`] for a [`Schema`] that already exists in the [`Manifest`].
type Occupied<'a> = OccupiedEntry<'a, String, Schema>;

/* ------------------------------------------------------------------------------ Public Exports */

/// Manifest of file segments and accompanying metadata for random access and predicate pruning.
/// See the [module-level documentation](self) for details.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
#[cbor(tag(100))]
pub(crate) struct Manifest {
    /// Schema segments keyed by name.
    #[cbor(n(0), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub schemas: BTreeMap<String, Schema>,
    /// Dictionaries keyed by name. Entries are **not** duplicated in the generic `schemas` map.
    #[cbor(n(1), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub dictionaries: BTreeMap<String, Dictionary>,
    /// Indexes keyed by name. Entries are **not** duplicated in the generic `dictionaries` map.
    #[cbor(n(2), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub indexes: BTreeMap<String, Index>,
    /// Implementers can use the optional free-form `metadata.toml` to attach file-level
    /// domain-specific information such as:
    ///
    /// - Date and time
    /// - Experimental parameters
    /// - Provenance
    ///
    /// If a metadata section is included in the file, a corresponding `length` and `offset` are
    /// described in the `manifest`. The core library includes a read and write surface, but
    /// implementers must include their own metadata parsing and validation logic.
    #[cfg(feature = "metadata")]
    #[cbor(n(3), skip_if = "Option::is_none")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub metadata: Option<Sector>,
}

impl Manifest {
    /// [`Deserialize`] a file [`Manifest`] from the provided [`File`](AsyncRead) at the specified
    /// [`Sector`].
    pub async fn from_file<F>(file: &mut F, sector: Sector) -> Result<Self, io::Error>
    where
        F: AsyncRead + Unpin + ?Sized,
    {
        let size = sector.length.get() as usize;
        let mut buf = Vec::with_capacity(size);
        file.read_exact(&mut buf).await?;
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Manifest::deserialize(&buf)
    }

    /// Add a [`Schema`] to [`self`](Manifest) with the specified `name` and [`Sector`].
    ///
    /// Resolves name conflicts by comparing the new and existing schema definitions; returning
    /// [`Ok`] if the underlying definitions are identical (deduplication) or [`Error::Collision`]
    /// if the underlying definitions differ.
    ///
    /// Returns an immutable reference to the inserted or existing [`Schema`] on success.
    pub fn schema(&mut self, name: impl Into<String>, schema: Schema) -> Result<&Schema, Error> {
        let name = name.into();
        match self.schemas.entry(name) {
            Entry::Vacant(entry) => Ok(entry.insert(schema)),
            Entry::Occupied(entry) if entry.get() == &schema => Ok(entry.into_mut()),
            Entry::Occupied(entry) => Error::collision(entry, schema).into(),
        }
    }

    /// todo → fn doc comment
    pub fn rebuild(data: &[u8], tail: NonZeroU64) -> Result<Self, Error> {
        unimplemented!("Manifest::rebuild is not yet implemented")
    }
}

impl Serialize for Manifest {
    type Buffer = Vec<u8>;

    fn size(&self) -> Result<NonZeroU64, accumulate::Error> {
        let size: u64 = minicbor::len(self).try_into()?;
        size.try_into().map_err(accumulate::Error::Convert)
    }

    fn serialize_into(&self, buf: &mut [u8]) {
        // SAFETY: minicbor::encode is infallible when writing to Vec<u8>
        minicbor::encode(self, buf).expect("Failed to encode manifest as CBOR");
    }

    fn serialize(&self) -> Result<Self::Buffer, accumulate::Error> {
        let size = self.size()?.get().try_into()?;
        let mut buf = Vec::with_capacity(size);
        self.serialize_into(&mut buf);
        debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
        Ok(buf)
    }
}

impl Deserialize for Manifest {
    type Error = io::Error;

    fn deserialize(src: &[u8]) -> Result<Self, Self::Error> {
        minicbor::decode(src).map_err(io::Error::Decode)
    }
}

/// A minimal schema segment **descriptor** that specifies:
///
/// 1. [`Sector`] where the schema segment is located on disk.
/// 2. [`BTreeMap`] of [`Column`] descriptors keyed by name.
///
/// This type does **not** contain the actual schema definition or columnar data buffers; it is a
/// lightweight descriptor for segment discovery and access without holding buffer contents in
/// memory. An on-disk schema segment encodes the schema definition (column names and types) while
/// on-disk data segments contain the columnar buffers.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Schema {
    /// Location of the schema segment including header.
    #[n(0)]
    pub sector: Sector,
    /// [`Column`] descriptors keyed by name.
    ///
    /// The [`BTreeMap`] guarantees a stable deterministic column order for consistent binary
    /// encoding and schema comparison.
    #[cbor(n(1), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
}

/// A minimal column **descriptor** that wraps a list of [`Buffer`] descriptors.
///
/// This type does **not** contain the actual buffer data; it is a lightweight descriptor for column
/// discovery and access without holding buffer contents in memory. Data is stored via one or more
/// on-disk data segments, each of which contains a buffer for this column.
///
/// [`Vec`] order in-memory is **not** guaranteed to reflect [`Sector`] order on-disk.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Column {
    /// List of [`Buffer`] descriptors for this column across all data segments.
    #[cbor(n(0), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub buffers: Vec<Buffer>,
}

/// A minimal columnar buffer **descriptor** that specifies:
///
/// 1. [`Sector`] where the buffer is located on disk.
/// 2. Number of data entries e.g. for index arithmetic.
/// 3. Buffer statistics such as `min` and `max` for predicate pruning.
///
/// This type does **not** contain the actual buffer data; it is a lightweight descriptor for buffer
/// discovery and access without holding buffer contents in memory. Data is stored via contiguous
/// buffers distributed across one or more on-disk data segments.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Buffer {
    /// Location of the schema segment including header.
    #[n(0)]
    pub sector: Sector,
    /// Number of data entries.
    ///
    /// Empty buffers are never written to disk. [`NonZeroU64`] is used to enforce this invariant.
    #[n(1)]
    pub count: NonZeroU64,
    /// Minimum value recorded in this buffer. Used for segment-level predicate pruning.
    ///
    /// Data is stored via an arbitrary-length [`Vec`] containing raw bytes encoded in
    /// platform-native endianness. Decode according to the [`Buffer`] type described by the schema.
    #[n(2)]
    pub min: Vec<u8>,
    /// Maximum value recorded in this buffer. Used for segment-level predicate pruning.
    ///
    /// Data is stored via an arbitrary-length [`Vec`] containing raw bytes encoded in
    /// platform-native endianness. Decode according to the [`Buffer`] type described by the schema.
    #[n(3)]
    pub max: Vec<u8>,
}

/// A minimal dictionary segment **descriptor** that specifies:
///
/// 1. [`Sector`] where the schema segment is located on disk.
/// 2. [`BTreeMap`] of [`Column`] descriptors keyed by name.
///
/// This type does **not** contain the actual schema definition or columnar data buffers; it is a
/// lightweight descriptor for segment discovery and access without holding buffer contents in
/// memory. An on-disk schema segment encodes the schema definition (column names and types) while
/// on-disk data segments contain the columnar buffers.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Dictionary {
    /// Location of the schema segment including header.
    #[n(0)]
    pub schema: Sector,
    /// Column descriptors keyed by name.
    #[cbor(n(1), skip_if = "BTreeMap::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
}

impl Dictionary {
    /// Returns a reference to the "key" column descriptor for this dictionary.
    pub fn key(&self) -> &Column {
        // SAFETY: Dictionaries are guaranteed to contain a "key" column at the type tree root.
        // 1. Serializer rejects incompatible type layouts during dictionary initialisation.
        // 2. Deserializer compares the type tree root against the required { key: K, value: V }
        //    layout. Only exact matches are deserialized into Dictionary instances.
        self.columns.get("key").expect("Dictionary does not contain a 'key' column")
    }
}

/// A minimal dictionary index **descriptor** that specifies:
///
/// 1. Underlying [`Dictionary`] descriptor.
/// 2. Next available `key` for appending new entries to the dictionary.
///
/// This type does **not** contain the actual dictionary entries; it is a lightweight descriptor for
/// index discovery and access without holding buffer contents in memory. An on-disk schema segment
/// encodes the schema definition (column names and types) while on-disk data segments contain the
/// columnar buffers.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, Decode, CborLen)]
pub(crate) struct Index {
    /// Underlying [`Dictionary`] descriptor.
    #[n(0)]
    pub dictionary: Dictionary,
    /// Next available key.
    ///
    /// Data is stored via an arbitrary-length [`Vec`] containing raw bytes encoded in
    /// platform-native endianness. Decode according to the `Key` type described by the schema.
    #[n(1)]
    pub next: Vec<u8>,
}

/* ------------------------------------------------------------------------------ Specific Error */

/// Errors returned by [`Manifest`] operations.
///
/// Enum variants cover various granular error cases that may arise when working with the manifest.
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
    /// A [`Schema`] with the same name but different contents already exists in the [`Manifest`].
    ///
    /// The manifest stores schemas in a [`BTreeMap`] keyed by name. Reusing an existing name
    /// therefore overwrites the existing schema definition, resulting in possible data loss.
    #[n(0)]
    Collision {
        /// Name shared by the new and existing schemas.
        #[n(0)]
        name: String,
        /// The existing [`Schema`] in the [`Manifest`].
        #[n(1)]
        existing: Schema,
        /// The new [`Schema`] being added to the [`Manifest`].
        #[n(2)]
        new: Schema,
    },
}

impl Error {
    /// Returns a new [`Error::Collision`] variant wrapping the schema name and conflicting sectors.
    fn collision(occupied: Occupied, new: Schema) -> Self {
        let name = occupied.key().clone();
        let existing = occupied.get().clone();
        Self::Collision { name, existing, new }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Collision { name, .. } => write!(f, "Schema '{name}' already in manifest"),
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

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {}
