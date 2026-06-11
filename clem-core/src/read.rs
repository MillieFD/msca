/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use crate::io::{self, Deserialize};
use crate::manifest::Buffer;
use crate::query::{self, Filter};
use crate::schema::{Schema, Unfolder};

/* ------------------------------------------------------------------------------ Public Exports */

/// Shorthand type-erased stack-allocated [pointer](Box) to a lazy [`Iterator`] yielding one
/// deserialized [`Outcome`] per candidate [`Item`](I).
///
/// Constructed via [`Read::boxed`]. Returns [`None`] once every candidate [`Buffer`] is consumed.
pub type Stream<'a, I> = Box<dyn Iterator<Item = Outcome<I>> + 'a>;

/// The result of [deserializing](Deserialize) one [`Item`](I) from a [`Read`](Read) [`Stream`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub enum Outcome<I> {
    /// A [deserialized](Deserialize::deserialize) [`Item`](I) which satisfies every [`Filter`].
    Success(I),
    /// The [`Item`](I) was rejected by one or more [filters](Filter) during [deserialization][1].
    ///
    /// [1]: Deserialize::deserialize
    Excluded,
    /// Every candidate [`Item`](I) has been [`Read`].
    Finished,
    /// An [`Error`](io::Error) occurred while [deserializing](Deserialize) or [filtering](Filter)
    /// the [`Item`](I).
    Error(io::Error),
}

/// A minimal column **data source** with [deserialization](Deserialize) context; used during
/// [`Query`] execution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Copy, Clone, Debug)]
pub struct Column<'a> {
    /// Retained [`Buffer`] descriptors for this [`Column`] across all data segments.
    pub(crate) buffers: &'a [Buffer],
    /// Read-only [memory map](Mmap) backed by the immutable segment region of a [clem](crate) file.
    ///
    /// Refer to the [safety documentation](io::File::mmap) for details.
    pub(crate) mmap: &'a Mmap,
    /// Deduplicated [`Filter`] set used to [`evaluate`](Filter::evaluate) deserialized items.
    pub(crate) filters: &'a HashSet<Filter>,
}

impl<'a> Column<'a> {
    /// Returns a read-only [memory map](Mmap) [slice][1] over the raw data bytes of the specified
    /// [`Buffer`]. Excludes the buffer [`header`](Buffer::HEADER).
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Truncated`](io::Error::Truncated) if the buffer extends beyond the end of
    /// the [`Mmap`] or is shorter than the fixed-length buffer header.
    ///
    /// [1]: https://doc.rust-lang.org/std/primitive.slice.html
    fn bytes(&self, buffer: &Buffer) -> Result<&'a [u8], io::Error> {
        let bytes = buffer.sector.slice(self.mmap)?;
        let actual = bytes.len();
        bytes.get(Buffer::HEADER..).ok_or(io::Error::truncated(Buffer::HEADER, actual))
    }

    /// Returns a read-only [memory map](Mmap) [`BitSlice`] over the raw data bytes of the specified
    /// [`Buffer`].
    ///
    /// Excludes the buffer [`header`](Buffer::HEADER) and leverages [`Buffer::count`] to discard
    /// any trailing bit padding.
    ///
    /// ### Errors
    ///
    /// - [`Error::Truncated`](io::Error::Truncated) if the buffer extends beyond the end of
    /// the [`Mmap`] or contains fewer bits than the expected `count`.
    /// - [`Error::Number`](io::Error::Number) if the row count overflows [`usize`].
    fn bits(&self, buffer: &Buffer) -> Result<&'a BitSlice<u8, Lsb0>, io::Error> {
        let count = buffer.count.get().try_into()?;
        let bytes = buffer.sector.slice(self.mmap)?.view_bits::<Lsb0>();
        let actual = bytes.len().mul(8);
        bytes.get(..count).ok_or(io::Error::truncated(count, actual))
    }
}

/* ----------------------------------------------------------------------- Read Trait Definition */

/// A **byte-stream** interface that lazily [deserializes](Deserialize::deserialize) and
/// [filters](Filter) successive [`items`](I) from the [clem](crate) file.
pub trait Read<I> {
    /// Advance the byte stream to [`Deserialize`] one candidate row as [`I`] and evaluate against
    /// the column [filters](Filter).
    fn next(&mut self) -> Result<Outcome<I>, io::Error>;
}