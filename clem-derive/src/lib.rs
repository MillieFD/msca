/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Procedural macros for the `clem` storage engine.
//!
//! ---
//!
//! Each macro expansion is implemented in the corresponding submodule; refer to the module-level
//! documentation for more details. Generated code resolves all paths via the `clem` facade which
//! re-exports this crate. Standalone use of `clem-derive` is not supported.

#![doc = include_str!("../../doc/derive.md")]

mod data;
mod read;

