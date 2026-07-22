/*
Project: msca
GitHub: https://github.com/MillieFD/msca

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
//! the deterministic platform-invariant [`BTreeMap`][1] column order used throughout [msca](crate).
//!
//! ### Expansion
//!
//! Generated code lives inside an anonymous `const` block to avoid collision with user items.
//!
//! 1. A composite reader type holding one boxed sub-stream per field.
//! 2. A `Composite` implementation over the `Query` (unfiltered composite path).
//! 3. A `Composite` implementation over a `Join` chain, emitted for two or more fields (filtered
//!    path).
//! 4. A `Read` implementation pulling one item per sub-stream in lockstep.
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
    let reader = &format_ident!("{src}Reader");
    let vis = &input.vis;
    // 2. Extract and sort fields by name
    let fields = &fields(input)?;
    // 3. Generate the reader struct and its trait implementations
    let structure = structure(vis, reader, fields);
    let query = query(reader, fields);
    let join = join(reader, fields);
    let iterate = iterate(src, reader, fields);
    // 4. Wrap in an anonymous const block to avoid collision with user items
    Ok(quote! {
        const _: () = {
            #structure
            #query
            #join
            #iterate
        };
    })
}

/* ----------------------------------------------------------------------- TokenStream Expansion */

/// Generate the composite **reader struct**: one boxed column stream per field.
///
/// Each field holds a type-erased [`Outcome`] iterator, because the opaque stream types of the
/// columns differ and a struct field must name one concrete type. The reader appears in the public
/// `Read::Src` GAT, so it inherits the source visibility to avoid leaking a private type through
/// the public interface.
fn structure(vis: &Visibility, reader: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    quote! {
        /// Generated composite reader holding one boxed column stream per field.
        #vis struct #reader<'a> {
            #(
                #idents: ::std::boxed::Box<
                    dyn ::core::iter::Iterator<Item = ::msca::Outcome<#types>> + 'a
                >,
            )*
        }
    }
}

/// Implement [`Composite`] over the borrowed [`Query`] (unfiltered composite path).
///
/// Each field resolves its column stream from the query through the internal `stream` method,
/// boxing the opaque iterator into the field type. The requested type is verified against the
/// on-disk column type exactly once; a missing or mismatched column aborts eagerly.
fn query(reader: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let names = Field::names(fields);
    let types = Field::types(fields);
    let bounds = stream_bounds(fields);
    quote! {
        impl<'a> ::msca::Composite<'a, ::msca::Query> for #reader<'a>
        where
            #bounds
        {
            fn new(src: &'a ::msca::Query) -> ::core::result::Result<Self, ::msca::query::Error> {
                ::core::result::Result::Ok(Self {
                    #( #idents: ::std::boxed::Box::new(src.stream::<#types>(#names)?), )*
                })
            }
        }
    }
}

/// The `where` bounds `Query::stream` requires of each field: the field must be [`Read`] and
/// [`Clone`], its column reader must [`Deserialize`] and [`Reader`], and the schema must unfold it.
fn stream_bounds(fields: &[Field<'_>]) -> TokenStream {
    let types = Field::types(fields);
    quote! {
        #(
            #types: ::msca::Read + ::core::clone::Clone + 'a,
            <#types as ::msca::Read>::Src<'a>:
                ::msca::Deserialize<'a, Ok = <#types as ::msca::Read>::Src<'a>>
                    + ::msca::Reader<'a, #types>,
            ::msca::Schema: ::msca::schema::Unfolder<#types>,
        )*
    }
}

/// Implement [`Composite`] over a left-nested [`Join`] chain (filtered path), emitted only for two
/// or more fields; a single-field type reads through the [`Query`] path alone, so this is empty.
///
/// Each leg is reached through the [`Join`] `a`/`b` fields in name-sorted order, its column name
/// verified against the expected field, and its stream boxed into the reader.
fn join(reader: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let count = fields.len();
    if count < 2 {
        return TokenStream::new();
    }
    let idents = Field::idents(fields);
    let names = Field::names(fields);
    let legs = legs(count);
    let chain = chain(&legs);
    let bounds = join_bounds(&legs, fields);
    let resolve = (0..count).map(|index| resolve(idents[index], &names[index], index, count));
    quote! {
        impl<'a, #(#legs),*> ::msca::Composite<'a, #chain> for #reader<'a>
        where
            #bounds
        {
            fn new(src: &'a #chain) -> ::core::result::Result<Self, ::msca::query::Error> {
                #( #resolve )*
                ::core::result::Result::Ok(Self { #( #idents, )* })
            }
        }
    }
}

/// The leg type parameters `L0, L1, …` for a join over `count` columns.
fn legs(count: usize) -> Vec<Ident> {
    (0..count).map(|index| format_ident!("L{index}")).collect()
}

/// Assemble the left-nested `Join<Join<…>, Ln>` type over the leg parameters.
fn chain(legs: &[Ident]) -> TokenStream {
    legs[1..].iter().fold(
        quote! { L0 },
        |chain, leg| quote! { ::msca::Join<#chain, #leg> },
    )
}

/// The `where` bounds requiring each leg to be a [`Column`] yielding its name-sorted field type.
fn join_bounds(legs: &[Ident], fields: &[Field<'_>]) -> TokenStream {
    let types = Field::types(fields);
    quote! {
        #( #legs: ::msca::Column<Item = #types> + 'a, )*
    }
}

/// Resolve one leg into its boxed column stream, binding it to the field `ident` after verifying
/// the leg column `name` matches the expected field.
///
/// Stream acquisition is fallible – each leg constructs its per-buffer sources eagerly – so a
/// framing error aborts composite assembly rather than surfacing mid-iteration.
fn resolve(ident: &Ident, name: &str, index: usize, count: usize) -> TokenStream {
    let access = access(index, count);
    quote! {
        let #ident = {
            let leg = #access;
            if ::msca::query::column::Adapter::root(leg).name != #name {
                return ::core::result::Result::Err(
                    ::msca::query::Error::Column { name: #name.into() },
                );
            }
            ::std::boxed::Box::new(::msca::query::column::Adapter::stream(leg)?)
        };
    }
}

/// Build the `&src.a…b` field chain reaching the leg for field `index` in a left-nested join of
/// `count` legs. Field `0` is the fully left-nested `a` chain; every later field reads the right
/// `b` branch after ascending the remaining `a` levels.
fn access(index: usize, count: usize) -> TokenStream {
    let ascents = match index {
        0 => count - 1,
        _ => count - 1 - index,
    };
    let base = (0..ascents).fold(quote! { src }, |expr, _| quote! { #expr.a });
    match index {
        0 => quote! { &#base },
        _ => quote! { &#base.b },
    }
}

/// Implement [`Iterator`] and `Read` for the external type: reconstruct one item per lockstep pull.
///
/// - `next` pulls one outcome per field stream in lockstep.
/// - [`None`] from any field stream terminates the composite stream.
/// - Errors surface eagerly, rejecting the whole item.
/// - The item is rebuilt from every field value; it is [`Include`] only if no field was excluded,
///   otherwise [`Exclude`] carrying the same reconstructed item.
fn iterate(src: &Ident, reader: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    quote! {
        impl<'a> ::core::iter::Iterator for #reader<'a> {
            type Item = ::msca::Outcome<#src>;

            fn next(&mut self) -> ::core::option::Option<::msca::Outcome<#src>> {
                let mut include = true;
                #(
                    let #idents = match ::core::iter::Iterator::next(&mut self.#idents)? {
                        ::msca::Outcome::Error(error) => {
                            return ::core::option::Option::Some(::msca::Outcome::Error(error));
                        }
                        ::msca::Outcome::Include(#idents) => #idents,
                        ::msca::Outcome::Exclude(#idents) => {
                            include = false;
                            #idents
                        }
                    };
                )*
                let row = #src { #( #idents, )* };
                ::core::option::Option::Some(match include {
                    true => ::msca::Outcome::Include(row),
                    false => ::msca::Outcome::Exclude(row),
                })
            }
        }

        impl ::msca::Read for #src {
            type Src<'a> = #reader<'a>;
        }
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;
    use crate::tests::{has, row};

    /* ---------------------------------------------------------------------------- Shared State */

    /// Expand the shared [`row`] and render the generated tokens as one string to search.
    fn code() -> String {
        expand(&row()).expect("Expansion failed").to_string()
    }

    /* ------------------------------------------------------------------------------ Unit Tests */

    /// [`expand`] emits the hidden reader, both `Composite` implementations, and the `Read`
    /// rebuilder.
    #[test]
    fn expand_emits_reader_and_impls() {
        let code = code();
        assert!(has(&code, "struct RowReader<'a>"));
        assert!(has(&code, "Composite<'a, ::msca::Query> for RowReader<'a>"));
        assert!(has(
            &code,
            "Composite<'a, ::msca::Join<L0, L1>> for RowReader<'a>"
        ));
        assert!(has(&code, "Iterator for RowReader<'a>"));
        assert!(has(&code, "impl ::msca::Read for Row"));
    }

    /// [`expand`] acquires every column stream fallibly: the `Query` path propagates from
    /// `Query::stream` and each `Join` leg from `Column::stream`, so framing errors abort early.
    #[test]
    fn expand_acquires_streams_fallibly() {
        let code = code();
        assert!(has(&code, "src.stream::<u32>(\"a\")?"));
        assert!(has(&code, "::msca::query::column::Adapter::stream(leg)?"));
    }

    /// [`expand`] omits the `Join` `Composite` implementation for a single-field struct.
    #[test]
    fn expand_single_field_skips_join() {
        let input: DeriveInput = parse_quote! { struct One { a: u32 } };
        let code = expand(&input).expect("Expansion failed").to_string();
        assert!(has(&code, "Composite<'a, ::msca::Query> for OneReader<'a>"));
        assert!(!has(&code, "::msca::Join"));
    }

    /// [`expand`] propagates the source visibility to the generated reader.
    ///
    /// The reader appears in the public `Read::Reader` GAT. A `pub` source must therefore yield a
    /// `pub` reader to avoid leaking a private type through the public interface.
    #[test]
    fn expand_reader_inherits_visibility() {
        let input: DeriveInput = parse_quote! { pub struct Row { a: u32, b: f64 } };
        let code = expand(&input).expect("Expansion failed").to_string();
        assert!(has(&code, "pub struct RowReader<'a>"));
    }

    /// Each reader field is a type-erased boxed [`Outcome`] iterator; the opaque column stream
    /// types differ, so a struct field cannot name them directly.
    #[test]
    fn expand_boxes_each_reader_field() {
        let code = code();
        let item = "Item = ::msca::Outcome<u32>";
        let field = format!("a: ::std::boxed::Box<dyn ::core::iter::Iterator<{item}> + 'a>");
        assert!(has(&code, &field));
    }

    /// [`expand`] output parses as valid Rust.
    #[test]
    fn expand_output_parses() {
        let expanded = expand(&row()).expect("Expansion failed");
        syn::parse2::<syn::File>(expanded).expect("Generated code does not parse");
    }

    /// [`expand`] rejects inputs without named fields.
    ///
    /// Field names are required to resolve column streams from the `Query`.
    #[test]
    // TODO → add enum support via variant discriminate (existing support for numerical primitives)
    fn expand_rejects_enum() {
        let input: DeriveInput = parse_quote! { enum Level { Low } };
        expand(&input).expect_err("Unsupported input accepted");
    }
}
