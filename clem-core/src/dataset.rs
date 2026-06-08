/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Public-facing user interface for [`clem`](crate) datasets.
//!
//! ---
//!
//! [`Dataset`] is the primary entry-point for working with a [`clem`](crate) file. It provides a
//! high-level surface for registering [`Record`] types and ingesting data while delegating
//! low-level IO to an internal [`File`] handle.

use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use crate::io::File;
use crate::query::{self, Query};
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

/// A high-level handle to an open [`clem`](crate) dataset.
///
/// `Dataset` exposes the public surface for registering [`Record`] types and ingesting data,
/// delegating low-level IO to an internal [`File`] handle.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub struct Dataset {
    /// Underlying [`File`] handle backing this dataset.
    file: File,
    /// Read-only [memory map](Mmap) backed by the [clem](crate) file.
    ///
    /// ### ⚠️ Warning
    ///
    /// Undefined behaviour may occur if the mapped region is modified. The [`Mmap`] is therefore
    /// tightly scoped; mapping only the immutable segment region to reduce the risk of undefined
    /// behaviour. Refer to the [`File::mmap`] documentation for more details.
    mmap: Arc<Mmap>,
}

impl Dataset {
    /// Create a new empty [`Dataset`] at the specified [`path`](P).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`path`](P)
    /// - The current process lacks read and write permissions
    /// - The platform does not support [memory mapping](memmap2)
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    pub async fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let file = File::create(path).await?;
        let mmap = unsafe { file.mmap(file.header.tail)? }.into();
        Ok(Self { file, mmap })
    }

    /// Open an existing [`Dataset`] with read and write permissions at the specified [`path`](P).
    ///
    /// A [`Mmap`] is scoped to the immutable segment file region. Implementors must ensure that the
    /// provided [`path`](P) remains valid and accessible for the entire duration of the operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the underlying system call fails. This can occur for a variety of
    /// reasons, including:
    ///
    /// - A file already exists at the specified [`path`](P)
    /// - The current process lacks read and write permissions
    /// - Unexpected `EOF` while parsing the [`Header`] or [`Manifest`]
    /// - The platform does not support [memory mapping](memmap2)
    ///
    /// Returns [`Error::Zero`] if a `u64` overflow occurs while calculating `size` or `offset` for
    /// the relevant file regions.
    pub async fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path).await?;
        let mmap = unsafe { file.mmap(file.header.tail)? }.into();
        Ok(Self { file, mmap })
    }
    pub fn query(&self, name: &str) -> Result<Query, query::Error> {
        let columns = self
            .file
            .manifest
            .schemas
            .get(name)
            .ok_or_else(|| query::Error::column(name))?
            .columns
            .iter()
            .map(query::Column::map) // Clone each entry
            .collect();
        Ok(Query {
            mmap: self.mmap.clone(), // Inexpensive Arc Clone
            columns,
            stride: NonZeroU32::MIN,
        })
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
}
