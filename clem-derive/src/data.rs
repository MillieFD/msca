/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Procedural macro expansion logic for `#[derive(Data)]`.
//!
//! ### Using `#[derive(Data]`
//!
//! Add the attribute to any algebraic data type.
//!
//! ```rust
//! #[derive(Data)]
//! struct Record {
//!     uuid: u8,
//!     latitude: f64,
//!     longitude: f64,
//! }
//! ```
//!
//! TODO → Document new schema and empty accumulator initialisation from Dataset once API exists
//!
//! Field registration is determined by the field [`Type`](syn::Type):
//!
//! - Supported primitive types register a corresponding column in the [`Schema`](clem::Schema).
//! - Algebraic types defer to their own `#[derive(Data)]` implementation
//!
//! This design allows for recursive nesting of `#[derive(Data)]` types which flatten into a single
//! collection of primitive columns. Fields are processed in **name-sorted** order corresponding to
//! the deterministic platform-invariant [`BTreeMap`][1] column order used throughout [clem](crate).
//!
//! ### Expansion
//!
//! Generated code lives inside an anonymous `const` block to avoid collision with user items.
//!
//! 1. A hidden composite [accumulator][2] type holding one boxed sub-accumulator per field.
//! 2. An [`Accumulate`][3] implementation distributing pushed items across the sub-accumulators.
//! 3. A [`Serialize`][4] implementation chaining sub-accumulators into one contiguous buffer.
//! 4. A [`Data`][5] implementation to register columns and construct the composite accumulator.
//!
//! [1]: std::collections::BTreeMap
//! [2]: clem::Accumulate::Acc
//! [3]: clem::Accumulate
//! [4]: clem::Serialize
//! [5]: clem::Data

use proc_macro2::TokenStream;
use quote::quote;