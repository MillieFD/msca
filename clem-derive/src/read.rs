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
//! 1. A composite context type holding one boxed sub-stream per field.
//! 2. A [`TryFrom`] implementation to construct the composite context from a borrowed `Query`.
//! 3. A `Read` implementation pulling one item per sub-stream in lockstep.
//!
//! [1]: std::collections::BTreeMap

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, Visibility};

use crate::{fields, Field};

/* ------------------------------------------------------------------------------ Public Exports */

/// Expand `#[derive(Read)]` according to the [module-level documentation](self).
///
/// ### Errors
///
/// Returns [`syn::Error`] if the input is not supported, has unnamed fields, or has no fields.
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    // 1. Resolve struct names and visibility
    let src = &input.ident;
    let ctx = &format_ident!("{src}Context");
    let vis = &input.vis;
    // 2. Extract and sort fields by name
    let fields = &fields(input)?;
    // 3. Generate trait implementations
    let context = context(vis, ctx, fields);
    let try_from = try_from(ctx, fields);
    let read = read(src, ctx, fields);
    // 4. Wrap in an anonymous const block
    Ok(quote! {
        const _: () = {
            #context
            #try_from
            #read
        };
    })
}

/* ----------------------------------------------------------------------- TokenStream Expansion */

/// Generate the composite **context** type holding one boxed sub-stream per field.
fn context(vis: &Visibility, ctx: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    quote! {
        /// Generated composite context holding one boxed column stream per field.
        #vis struct #ctx<'a> {
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

/// Implement `Read` for the external type.
///
/// - `next` pulls one outcome per field from the generated [`context`] in lockstep.
/// - Errors surface eagerly, rejecting the whole item.
/// - An exhausted column terminates the composite stream.
/// - The item is rebuilt only if every field succeeds; any excluded field excludes the item.
///
/// The unit `Src` carries no state; each boxed column stream owns its own source internally.
fn read(src: &Ident, ctx: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    quote! {
        impl ::clem::Read for #src {
            type Ctx<'a> = #ctx<'a>;

            type Src<'a> = ();

            fn next<'a>(
                _: &mut Self::Src<'a>,
                ctx: &mut Self::Ctx<'a>,
            ) -> ::clem::Outcome<#src> {
                #(
                    let #idents = match ctx.#idents.next() {
                        ::core::option::Option::Some(::clem::Outcome::Error(error)) => {
                            return ::clem::Outcome::Error(error);
                        }
                        ::core::option::Option::Some(outcome) => outcome,
                        ::core::option::Option::None => return ::clem::Outcome::Finished,
                    };
                )*
                match ( #( #idents, )* ) {
                    ( #( ::clem::Outcome::Success(#idents), )* ) => {
                        ::clem::Outcome::Success(#src { #( #idents, )* })
                    }
                    _ => ::clem::Outcome::Excluded,
                }
            }
        }
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;
    use crate::tests::has;

    /// [`expand`] emits the hidden context and one implementation per generated trait.
    #[test]
    fn expand_emits_impls() {
        let input: DeriveInput = parse_quote! { struct Row { a: u32, b: f64 } };
        let code = expand(&input).expect("Expansion failed").to_string();
        assert!(has(&code, "struct RowContext<'a>"));
        assert!(has(&code, "TryFrom<&'a ::clem::Query> for RowContext<'a>"));
        assert!(has(&code, "impl ::clem::Read for Row"));
    }

    /// [`expand`] propagates the source visibility to the generated context.
    ///
    /// The context appears in the public `Read::Ctx` GAT. A `pub` source must therefore yield a
    /// `pub` context to avoid leaking a private type through the public interface.
    #[test]
    fn expand_context_inherits_visibility() {
        let input: DeriveInput = parse_quote! { pub struct Row { a: u32, b: f64 } };
        let code = expand(&input).expect("Expansion failed").to_string();
        assert!(has(&code, "pub struct RowContext<'a>"));
    }

    /// [`expand`] output parses as valid Rust.
    #[test]
    fn expand_parses() {
        let input: DeriveInput = parse_quote! { struct Row { a: u32, b: f64 } };
        let expanded = expand(&input).expect("Expansion failed");
        syn::parse2::<syn::File>(expanded).expect("Generated code does not parse");
    }

    /// [`expand`] rejects inputs without named fields.
    ///
    /// Field names are required to resolve column streams from the `Query`.
    #[test]
    // TODO → add enum support via variant discriminate (existing support for numerical primitives)
    fn expand_rejects_enum() {
        let input: DeriveInput = parse_quote! { enum Level { Low } };
        assert!(expand(&input).is_err());
    }
}
