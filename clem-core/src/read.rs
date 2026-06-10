/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use crate::io::{self, Deserialize};
use crate::manifest::Buffer;
use crate::query::Filter;
/// The result of [deserializing](Deserialize) a single [`Item`](I) from the [`Read`] stream.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Outcome<I> {
    /// A [deserialized](Deserialize::deserialize) [`Item`](I) which satisfies every [`Filter`].
    Success(I),
    /// The [`Item`](I) was rejected by one or more [filters](Filter) during [deserialization][1].
    ///
    /// [1]: Deserialize::deserialize
    Excluded,
    /// Every candidate [`Column`](crate::query::Column) [`Item`](I) has been [`Read`].
    Finished,
}

/* ----------------------------------------------------------------------- Read Trait Definition */

/// A **byte-stream** interface that lazily [deserializes](Deserialize::deserialize) and
/// [filters](Filter) successive [`items`](I) from the [clem](crate) file.
pub trait Read<I> {
    /// Advance the byte stream to [`Deserialize`] one candidate row as [`I`] and evaluate against
    /// the column [filters](Filter).
    fn next(&mut self) -> Result<Outcome<I>, io::Error>;
}