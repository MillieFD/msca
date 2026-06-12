/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Procedural macro expansion logic for `#[derive(Read)]`.
//!
//! ### Using `#[derive(Read)]`
//!
//! Add the attribute to any algebraic data type.
//!
//! ```rust,ignore
//! #[derive(Read)]
//! struct Record {
//!     uuid: u8,
//!     latitude: f64,
//!     longitude: f64,
//! }
//! ```
//!
//! TODO → Document query construction and composite row streaming via Dataset::query
//!
//! Field streaming is determined by the field [`Type`](syn::Type):
//!
//! - Supported primitive types stream from the corresponding column in the `Query`.
//! - Algebraic types defer to their own `#[derive(Read)]` implementation
//!
//! This design allows for recursive nesting of `#[derive(Read)]` types which rebuild from a flat
//! collection of primitive columns. Fields are processed in **name-sorted** order corresponding to
//! the deterministic platform-invariant [`BTreeMap`][1] column order used throughout [clem](crate).
//!
//! ### Expansion
//!
//! Generated code lives inside an anonymous `const` block to avoid collision with user items.
//!
//! 1. A hidden composite context type holding one boxed sub-stream per field.
//! 2. A [`TryFrom`] implementation to construct the composite context from a borrowed `Query`.
//! 3. A `Read` implementation pulling one item per sub-stream in lockstep.
//!
//! [1]: std::collections::BTreeMap

use proc_macro2::TokenStream;