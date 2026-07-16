/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! A composable [`Query`] interface to [read](Read) data from any [msca](crate) file.
//!
//! ---
//!
//! Each new [`Query`] begins with **every** column and **every** buffer from the specified schema.
//! Individual [columns](column::Column) can be resolved and filtered to subtractively reduce the
//! result set. Some filters are evaluated eagerly **before** file IO; removing individual
//! [buffers](Buffer) using [manifest] statistics. Other filters are attached to read-time
//! [adapters](column::Adapter) and evaluated lazily **during** [deserialization](Deserialize).
//!
//! A [`Query`] is a factory for strongly-typed [`Column`](column::Column) handles over one schema,
//! plus a set of unfiltered composite conveniences. Extraction is selection: a column is read only
//! when a handle is opened for it via [`column`](Query::column). Filters live on the handle as
//! concrete typed state and are applied to each item **after** deserialization, so every item is
//! deserialized exactly once and every predicate is an infallible, statically-dispatched test.
//!
//! ```rust,ignore
//! let overheating = dataset
//!     .query("schema_name")?
//!     .column::<f64>("temperature")?
//!     .range(35.0..)?
//!     .read();
//! ```
//!
//! Items are deserialized exactly once. Every filter is an infallible monomorphized test. No file
//! [`IO`](io) is executed until the [`Iterator`] returned by a terminal method is polled.

#![doc = include_str!("../../../doc/query-filters.md")]
#![doc = include_str!("../../../doc/query-columns.md")]

use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::iter;
use std::num::{self, TryFromIntError};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use memmap2::Mmap;

use crate::io::{self, Deserialize};
use crate::manifest;
use crate::read::{Composite, Outcome, Read, Reader};
use crate::schema::{number, Schema, Type, Unfolder};

pub mod column;
pub mod stream;
