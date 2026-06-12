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
use syn::{DeriveInput, Ident};

use crate::{fields, Field};

/* ------------------------------------------------------------------------------ Public Exports */

/// Expand `#[derive(Data)]` according to the [module-level documentation](self).
///
/// ### Errors
///
/// Returns [`syn::Error`] if the input is not a struct, has unnamed fields, or has no fields.
pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;
    let fields = fields(input)?;
    let accumulator = accumulator(&fields);
    let accumulate = accumulate(name, &fields);
    let serialize = serialize(&fields);
    Ok(quote! {
        const _: () = {
            #accumulator
            #accumulate
            #serialize
        };
    })
}

/* ----------------------------------------------------------------------- TokenStream Expansion */

/// Generate the hidden composite [accumulator][1] type holding one [sub-accumulator][2] per field.
///
/// [1]: clem::Accumulate::Acc
/// [2]: clem::BoxAcc
fn accumulator(fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    let types = Field::types(fields);
    quote! {
        /// Generated composite accumulator holding one boxed sub-accumulator per field.
        struct Acc {
            #( #idents: ::clem::BoxAcc<#types>, )*
        }
    }
}

/// Implement [`Accumulate`](clem::Accumulate) for the generated [`accumulator`].
///
/// - [`push`][1] distributes each incoming field across the sub-accumulators.
///
/// - [`push`][1] distributes each incoming item field into the corresponding sub-accumulator;
/// ensuring all sub-accumulators advance in lockstep.
///
/// - [`discard`][2] and [`buffers`][3] delegate to each sub-accumulator in order.
/// - [`is_empty`][4] and [`count`][5] delegate to the first field only.
///
/// This design ensures that all sub-accumulators advance in lockstep. [`buffers`][3] threads the
/// `offset` through each delegated call to encode buffers contiguously, returning the final offset.
///
/// [1]: clem::Accumulate::push
/// [2]: clem::Accumulate::discard
/// [3]: clem::Accumulate::buffers
/// [4]: clem::Accumulate::is_empty
/// [5]: clem::Accumulate::count
fn accumulate(name: &Ident, fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    // NOTE: crate::fields rejects empty structs; the first field is guaranteed to exist
    let head = idents[0];
    quote! {
        impl ::clem::Accumulate for Acc {
            type Item = #name;

            fn push(&mut self, value: #name) {
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

/// Implement [`Serialize`](clem::Serialize) for the generated [`accumulator`].
///
/// - [`size`][1] sums the serialized size of every sub-accumulator.
/// - [`serialize_into`][2] delegates to each sub-accumulator in order.
/// - [`serialize`][3] allocates using [`size`][1] and fills via [`serialize_into`][2].
///
/// [1]: Serialize::size
/// [2]: Serialize::serialize_into
/// [3]: Serialize::serialize
fn serialize(fields: &[Field<'_>]) -> TokenStream {
    let idents = Field::idents(fields);
    quote! {
        impl ::clem::Serialize for Acc {
            type Buffer = ::std::vec::Vec<u8>;

            fn size(
                &self,
            ) -> ::core::result::Result<::core::num::NonZeroU64, ::clem::schema::number::Error> {
                let total: u64 = 0;
                #(
                    let total = total
                        .checked_add(self.#idents.size()?.get())
                        .ok_or(::clem::schema::number::Error::Zero)?;
                )*
                ::core::num::NonZeroU64::new(total).ok_or(::clem::schema::number::Error::Zero)
            }

            fn serialize_into<'a>(
                &self,
                buf: &'a mut [u8],
            ) -> ::core::result::Result<&'a mut [u8], ::clem::schema::number::Error> {
                #( let buf = self.#idents.serialize_into(buf)?; )*
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
                ::core::result::Result::Ok(buf)
            }
        }
    }
}
