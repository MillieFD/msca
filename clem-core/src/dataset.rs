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

impl Dataset {}
