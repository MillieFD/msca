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
//! evaluated **before** file IO to remove individual buffers or entire columns using [manifest]
//! statistics. Other filters are attached to the relevant column and evaluated lazily **during**
//! buffer [deserialization](Deserialize). No file IO is executed until [`read`](Query::read) is
//! awaited.
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

/* ------------------------------------------------------------------------------ Public Exports */

/// A [query filter](self) that evaluates the raw bytes **during file IO** and before
/// [deserialization](Deserialize). Returns `true` if the row should be retained.
pub(crate) type Filter = Box<dyn Fn(&[u8]) -> Result<bool, io::Error>>;

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {}
