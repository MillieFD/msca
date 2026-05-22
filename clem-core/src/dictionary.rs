/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

#![doc = include_str!("../../doc/dictionary.md")]

use std::collections::btree_map::{BTreeMap, Entry};
use std::fmt;
use std::num::NonZeroU64;

use crate::accumulate::{Accumulate, Buffer, Seq, Serialize};
use crate::io::{Header, Write};
use crate::schema::{number, Unfold};
use crate::segment::Variant;
use crate::{schema, Sector};

/* ------------------------------------------------------------------------------ Public Exports */

/// A minimal **in-memory accumulator** for large [`items`](I) indexed by small unique [`keys`](K).
#[doc = include_str!("../../doc/dictionary.md")]
pub struct Dictionary<K, I>
where
    K: Unfold + Ord,
    I: Serialize,
{
    /// Sorted [`items`](I) indexed by small unique [`keys`](K).
    pub(crate) entries: BTreeMap<K, I>,
    /// [`Sector`] of the bound dictionary schema segment.
    pub(crate) schema: Sector,
}
