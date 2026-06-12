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
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident};

use crate::{fields, Field};

/* ------------------------------------------------------------------------------ Public Exports */

/// Expand `#[derive(Read)]` according to the [module-level documentation](self).
///
/// ### Errors
///
/// Returns [`syn::Error`] if the input is not supported, has unnamed fields, or has no fields.
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    // 1. Resolve struct names
    let src = &input.ident;
    let ctx = &format_ident!("{src}Context");
    // 2. Extract and sort fields by name
    let fields = &fields(input)?;
    // 3. Generate trait implementations
    let context = context(ctx, fields);
    let try_from = try_from(ctx, fields);
    // 4. Wrap in an anonymous const block
    Ok(quote! {
        const _: () = {
            #context
            #try_from
        };
    })
}

/* ----------------------------------------------------------------------- TokenStream Expansion */

/// Generate the hidden composite **context** type holding one boxed sub-stream per field.
fn context(ctx: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    quote! {
        /// Generated composite context holding one boxed column stream per field.
        struct #ctx<'a> {
            #( #idents: ::clem::Stream<'a, #types>, )*
        }
    }
}

/// Implement [`TryFrom`] for the generated [`context`].
///
/// - Each field resolves the corresponding column from a borrowed `Query`.
/// - The requested type is verified against the on-disk column type exactly once.
/// - Missing or mismatched columns abort construction eagerly with `query::Error`.
///
/// This design allows subsequent stream iteration and item deserialization to progress fearlessly
/// without additional runtime type checks.
fn try_from(ctx: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    let names = Field::names(fields);
    quote! {
        impl<'a> ::core::convert::TryFrom<&'a ::clem::Query> for #ctx<'a> {
            type Error = ::clem::query::Error;

            fn try_from(
                query: &'a ::clem::Query,
            ) -> ::core::result::Result<Self, Self::Error> {
                ::core::result::Result::Ok(Self {
                    #( #idents: query.column::<#types>(#names)?, )*
                })
            }
        }
    }
}
