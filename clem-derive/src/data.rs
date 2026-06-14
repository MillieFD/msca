/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! Procedural macro expansion logic for `#[derive(Data)]`.
//!
//! ### Using `#[derive(Data)]`
//!
//! Add the attribute to any algebraic data type.
//!
//! ```rust,ignore
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
//! - Supported primitive types register a corresponding column in the `Schema`.
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
//! 1. A hidden composite accumulator type holding one boxed sub-accumulator per field.
//! 2. An `Accumulate` implementation distributing pushed items across the sub-accumulators.
//! 3. A `Serialize` implementation chaining sub-accumulators into one contiguous buffer.
//! 4. A `Data` implementation to register columns and construct the composite accumulator.
//!
//! [1]: std::collections::BTreeMap

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident};

use crate::{fields, Field};

/* ------------------------------------------------------------------------------ Public Exports */

/// Expand `#[derive(Data)]` according to the [module-level documentation](self).
///
/// ### Errors
///
/// Returns [`syn::Error`] if the input is not supported, has unnamed fields, or has no fields.
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    // 1. Resolve struct names
    let src = &input.ident;
    let acc = &format_ident!("{src}Accumulator");
    // 2. Extract and sort fields by name
    let fields = &fields(input)?;
    // 3. Generate trait implementations
    let accumulator = accumulator(acc, fields);
    let accumulate = accumulate(src, acc, fields);
    let serialize = serialize(acc, fields);
    let data = data(src, acc, fields);
    // 4. Wrap in an anonymous const block
    Ok(quote! {
        const _: () = {
            #accumulator
            #accumulate
            #serialize
            #data
        };
    })
}

/* ----------------------------------------------------------------------- TokenStream Expansion */

/// Generate the hidden composite **accumulator** type holding one boxed sub-accumulator per field.
fn accumulator(acc: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    quote! {
        /// Generated composite accumulator holding one boxed sub-accumulator per field.
        struct #acc {
            #( #idents: ::clem::BoxAcc<#types>, )*
        }
    }
}

/// Implement `Accumulate` for the generated [`accumulator`].
///
/// - `push` distributes each incoming field across the sub-accumulators.
/// - `discard` and `buffers` delegate to each sub-accumulator in order.
/// - `is_empty` and `count` delegate to the first field only.
///
/// This design ensures that all sub-accumulators advance in lockstep. `buffers` threads the
/// `offset` through each delegated call to encode buffers contiguously, returning the final offset.
fn accumulate(src: &Ident, acc: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    // NOTE: crate::fields rejects empty structs; the first field is guaranteed to exist
    let head = idents[0];
    quote! {
        impl ::clem::Accumulate for #acc {
            type Item = #src;

            fn push(&mut self, value: #src) {
                #( self.#idents.push(value.#idents); )*
            }

            fn discard(&mut self) {
                #( self.#idents.discard(); )*
            }

            fn is_empty(&self) -> bool {
                self.#head.is_empty()
            }

            fn count(&self) -> u64 {
                self.#head.count()
            }

            fn buffers(
                &self,
                offset: u64,
                columns: &mut ::clem::Columns,
            ) -> ::core::result::Result<u64, ::clem::schema::number::Error> {
                #( let offset = self.#idents.buffers(offset, columns)?; )*
                ::core::result::Result::Ok(offset)
            }
        }
    }
}

/// Implement `Serialize` for the generated [`accumulator`].
///
/// - `size` folds the aligned serialized size of every sub-accumulator.
/// - `serialize_into` delegates to each sub-accumulator in order.
/// - `serialize` allocates using `size` and fills via `serialize_into`.
fn serialize(acc: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    quote! {
        impl ::clem::Serialize for #acc {
            type Buffer = ::std::vec::Vec<u8>;

            fn size(
                &self,
            ) -> ::core::result::Result<::core::num::NonZeroU64, ::clem::schema::number::Error> {
                let total = [ #( self.#idents.size(), )* ]
                    .into_iter()
                    .try_fold(u64::MIN, |acc, size| {
                        let size = ::clem::Align::align(size?)?;
                        acc.checked_add(size).ok_or(::clem::schema::number::Error::Zero)
                    })?;
                ::core::num::NonZeroU64::new(total).ok_or(::clem::schema::number::Error::Zero)
            }

            fn serialize_into<'a>(
                &self,
                buf: &'a mut [u8],
            ) -> ::core::result::Result<&'a mut [u8], ::clem::schema::number::Error> {
                #( let buf = self.#idents.serialize_into_aligned(buf)?; )*
                ::core::result::Result::Ok(buf)
            }

            fn serialize(
                &self,
            ) -> ::core::result::Result<::std::vec::Vec<u8>, ::clem::schema::number::Error> {
                let size = self
                    .size()?
                    .get()
                    .try_into()
                    .map_err(::clem::schema::number::Error::from)?;
                let mut buf = ::std::vec![0u8; size];
                self.serialize_into(&mut buf)?;
                // NOTE: cannot use static assertion; size depends on runtime data accumulation
                ::core::debug_assert_eq!(buf.len(), size, "actual size ≠ predicted size");
                ::core::result::Result::Ok(buf)
            }
        }
    }
}

/// Implement `Data` for the annotated external type.
fn data(src: &Ident, acc: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    let names = Field::names(fields);
    quote! {
        impl ::clem::Data for #src {
            fn accumulator(
                schema: &mut ::clem::Schema,
            ) -> ::core::result::Result<::clem::BoxAcc<#src>, ::clem::schema::Error> {
                ::core::result::Result::Ok(::std::boxed::Box::new(#acc {
                    #( #idents: schema.column::<#types, &'static str>(#names)?, )*
                }))
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

    /// [`expand`] emits the hidden accumulator and one implementation per generated trait.
    #[test]
    fn expand_emits_impls() {
        let input: DeriveInput = parse_quote! { struct Row { a: u32, b: f64 } };
        let code = expand(&input).expect("Expansion failed").to_string();
        assert!(has(&code, "struct RowAccumulator"));
        assert!(has(&code, "impl ::clem::Accumulate for RowAccumulator"));
        assert!(has(&code, "impl ::clem::Serialize for RowAccumulator"));
        assert!(has(&code, "impl ::clem::Data for Row"));
    }

    /// [`expand`] chains sub-accumulators through the aligned serialization surface.
    #[test]
    fn expand_pads_columns() {
        let input: DeriveInput = parse_quote! { struct Row { a: u32, b: f64 } };
        let code = expand(&input).expect("Expansion failed").to_string();
        assert!(has(&code, "Align"));
        assert!(has(&code, "serialize_into_aligned"));
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
    /// Field names are required to generate column names in the `Schema`.
    #[test]
    fn expand_rejects_tuple() {
        let input: DeriveInput = parse_quote! { struct Tuple(u32); };
        assert!(expand(&input).is_err());
    }
}
