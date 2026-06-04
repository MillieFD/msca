/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! The [`Query`] API provides a composable interface for reading data from any [clem](crate) file.
//! Conditions are chained method-by-method and evaluated lazily; no file IO occurs until
//! `.read().await` is called.
//!
//! ```rust,ignore
//! let results = dataset
//!     .query("schema_name")
//!     .select(["latitude", "longitude", "temperature"])
//!     .range("temperature", 10.0..=20.0)
//!     .eq("active", true)
//!     .read()
//!     .await?;
//! ```

#![doc = include_str!("../../doc/query-filters.md")]
#![doc = include_str!("../../doc/query-joins.md")]

/* ------------------------------------------------------------------------------ Public Exports */

/// A [query filter](self) that evaluates the raw bytes **during file IO** and before
/// [deserialization](Deserialize). Returns `true` if the row should be retained.
pub(crate) type Filter = Box<dyn Fn(&[u8]) -> Result<bool, io::Error>>;
