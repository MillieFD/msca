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

/// A single field from the external struct; borrows from [`DeriveInput`].
#[derive(Clone, Copy)]
struct Field<'a> {
    /// Field [identifier](Ident); used to generate the corresponding schema column name.
    ident: &'a Ident,
    /// Field [type](Type); parameterises the generated column accumulator or stream.
    ty: &'a Type,
}

impl<'a> Field<'a> {
    /// Returns the [identifier](Ident) for each [`Field`].
    fn idents(fields: &[Self]) -> Vec<&'a Ident> {
        fields.iter().map(|field| field.ident).collect()
    }

    /// Returns the [`Type`] for each [`Field`].
    fn types(fields: &[Self]) -> Vec<&'a Type> {
        fields.iter().map(|field| field.ty).collect()
    }

    /// Returns the column [`name`](String) for each [`Field`].
    fn names(fields: &[Self]) -> Vec<String> {
        let name = |field: &Self| field.ident.to_string();
        fields.iter().map(name).collect()
    }
}

