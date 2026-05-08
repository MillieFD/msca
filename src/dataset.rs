/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! todo mod doc

use crate::io::File;
use crate::manifest::Manifest;

/* ------------------------------------------------------------------------------ Public Exports */

/// todo struct doc comment
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub struct Dataset {
    /// todo field doc comment
    manifest: Manifest,
    /// todo field doc comment
    file: File,
}

impl Dataset {}
