/*
Project: clem
GitHub: https://github.com/MillieFD/clem

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

//! A composable [`Query`] interface to [read](Read) data from any [clem](crate) file.
//!
//! ---
//!
//! Each new [`Query`] begins with **every** column and **every** buffer from the specified schema.
//! [`Filter`] functions are then applied subtractively to reduce the result set. Some filters are
//! evaluated eagerly **before** file IO; removing individual buffers or entire columns informed by
//! [manifest] statistics. Other filters are attached to the relevant column and evaluated lazily
//! **during** [deserialization](Deserialize).
//!
//! ```rust,ignore
//! let results = dataset
//!     .query("schema_name")?
//!     .select(["latitude", "longitude", "temperature"])
//!     .range("temperature", 10.0..=20.0)?
//!     .eq("active", true)?
//!     .read()?;
//! ```
//!
//! No file IO is executed until the [`Iterator`] returned by [`read`](Query::read) is polled.

#![doc = include_str!("../../doc/query-filters.md")]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{self, Display};
use std::iter;
use std::num::{self, TryFromIntError};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use memmap2::Mmap;
use minicbor::{CborLen, Decode, Encode};

use crate::accumulate::{Buffer, OptBitVec};
use crate::io::{self, Deserialize, Deserializer};
use crate::manifest::{self, B};
use crate::read::{self, Outcome, Read, Reader, Stream};
use crate::schema::{number, Schema, Type, Unfold, Unfolder};
use crate::Serialize;

/* ------------------------------------------------------------------------------ Public Exports */

/// A composable query builder to [read](Read) data from any [clem](crate) file; initialised from
/// [`Dataset::query`][1] and executed lazily when [`read`](Self::read) is iterated.
///
/// Refer to the [module-level documentation](self) for implementation details and a list of
/// supported filters.
///
/// [1]: crate::Dataset::query
// TODO → add derive attributes
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct Query {
    /// Read-only [memory map](Mmap) backed by the immutable segment region of a [clem](crate) file.
    ///
    /// Refer to the [safety documentation](io::File::mmap) for details.
    pub(crate) mmap: Arc<Mmap>,
    /// [`Column`] descriptors keyed by name.
    ///
    /// The [`BTreeMap`] guarantees a stable deterministic column order for consistent binary
    /// encoding and schema comparison.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub columns: BTreeMap<String, Column>,
    /// Decimation factor applied to downsample the result set; defaults to 1 (keep all data).
    pub stride: num::NonZeroU32,
}

impl Query {
    /// Returns an [`Iterator`] over [`deserialized`][1] [`items`](I) from the [`Query`] result set.
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if a required column is not found in the query [`BTreeMap`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    ///
    /// [1]: Deserialize::deserialize
    pub fn read<'a, I>(&'a self) -> Result<impl Iterator<Item = Result<I, io::Error>> + 'a, Error>
    where
        I: Read + 'a,
        I::Src<'a>: TryFrom<&'a Query, Error = Error> + Iterator<Item = Outcome<I>> + 'a,
    {
        let mut reader: I::Src<'a> = self.try_into()?;
        let iter = iter::from_fn(move || {
            loop {
                return match reader.next()? {
                    Outcome::Include(item) => Ok(item).into(),
                    Outcome::Exclude => continue,
                    Outcome::Error(error) => Err(error).into(),
                };
            }
        })
        .step_by(self.stride.get().try_into()?);
        Ok(iter)
    }

    /// Drain the [`Query`] result set into an owned [`Vec`] of [`deserialized`][1] [`items`](I).
    ///
    /// ### Errors
    ///
    /// See [`Query::read`] for a description of the error conditions that may arise during setup.
    /// Returns [`Error::Io`] if a file IO or deserialization error occurs during iteration.
    ///
    /// [1]: Deserialize::deserialize
    pub fn collect<I>(self) -> Result<Vec<I>, Error>
    where
        I: Read + 'static,
        for<'a> I::Src<'a>: TryFrom<&'a Query, Error = Error> + Iterator<Item = Outcome<I>> + 'a,
    {
        self.read::<I>()?.collect::<Result<Vec<I>, io::Error>>().map_err(Error::from)
    }

    /// Returns a reference to the [`Column`] descriptor corresponding to the provided `name`.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Column`] if the requested `name` is not found in the [`Query`].
    pub fn get(&self, name: &str) -> Result<&Column, Error> {
        self.columns.get(name).ok_or_else(|| Error::column(name))
    }

    /// Returns a mutable reference to the [`Column`] corresponding to the provided `name`.
    ///
    /// ### Errors
    ///
    /// Returns [`Error::Column`] if the requested `name` is not found in the [`Query`].
    fn get_mut(&mut self, name: &str) -> Result<&mut Column, Error> {
        self.columns.get_mut(name).ok_or_else(|| Error::column(name))
    }

    /// Returns a [`Stream`] yielding [`deserialized`][1] [`items`](I) from the named [`Column`].
    ///
    /// The requested [`Type`] is verified against the on-disk [`Column`] type exactly once.
    /// Subsequent deserialization can progress fearlessly without additional runtime checks.
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the requested `name` is not found in the query [`BTreeMap`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    ///
    /// [1]: Deserialize::deserialize
    pub fn column<'a, I>(&'a self, name: &str) -> Result<Stream<'a, I>, Error>
    where
        I: Read + 'a,
        I::Src<'a>: Reader<'a, I> + TryFrom<&'a [u8]>,
        Schema: Unfolder<I>,
    {
        // NOTE: Type::verify exactly once at initialisation (eager); progress fearlessly
        let column = self.get(name)?.exact()?;
        let stream = read::Column {
            buffers: column.buffers.iter(),
            mmap: &self.mmap,
            filters: &column.filters,
        }
        .stream();
        Ok(stream)
    }

    /* --------------------------------------------------------------------------- Query Filters */

    /// A [`Query`] retains all columns defined by the [`Schema`] unless otherwise specified. The
    /// `select` filter restricts the returned columns to a named subset, reducing file IO to only
    /// the required buffers.
    ///
    /// ```rust,ignore
    /// .select(["a", "b"]) // Return only columns "a" and "b"
    /// ```
    ///
    /// Any [`Column`] omitted from `select` is never read from disk; the primary mechanism to
    /// reduce file IO on wide schemas. Omitting `select` is equivalent to selecting every column.
    ///
    /// Refer to the [module-level documentation](self) for more details.
    pub fn select<N, S>(mut self, names: N) -> Self
    where
        N: IntoIterator<Item = S>,
        String: From<S>,
    {
        let keep: BTreeSet<String> = names.into_iter().map(String::from).collect();
        self.columns.retain(|name, column| keep.contains(name));
        self // return to builder pattern
    }

    /// Retain rows from the named [`Column`] only if the deserialized [`item`](I) falls within the
    /// specified [`Range`](RangeBounds). Excluded rows are removed from all columns.
    ///
    /// `range` is a **mixed** filter:
    /// 1. Eagerly evaluated **before** IO using [`Buffer`] statistics.
    /// 2. Lazily evaluated **during** [deserialization](Deserialize) to filter individual rows.
    ///
    /// ```rust,ignore
    /// .range("temperature", 10..20) // 10.0 ≤ temperature < 20.0 inclusive range
    /// .range("altitude", 100..=500) // inclusive upper bound on additonal column
    /// ```
    ///
    /// Open or half-open ranges are also supported:
    ///
    /// ```rust,ignore
    /// .range("pressure", 101.3..) // pressure ≥ 101.3  (no upper bound)
    /// .range("pressure", ..105.0) // pressure < 105.0  (no lower bound)
    /// ```
    ///
    /// Refer to the [module-level documentation](self) for more details.
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    /// - [`Error::Io`] if an error occurs during [deserialization](Deserialize).
    pub fn range<I, B>(mut self, name: &str, bounds: B) -> Result<Self, Error>
    where
        I: Serialize + for<'a> Deserialize + PartialOrd,
        B: RangeBounds<I>,
        Schema: Unfolder<I>,
    {
        let column = self.get_mut(name)?.accepts_mut()?;
        // 1. Insert filter for lazy evaluation during deserialization
        let filter = Filter::bounds(&bounds);
        column.filters.insert(filter);
        // 2. Eagerly evaluate buffer min / max statistics
        let n = column.buffers.len();
        let mut keep = column
            .buffers
            .iter()
            // SAFETY: Type::verify guarantees that bounds match the on-disk column type
            .try_fold(Vec::with_capacity(n), |mut acc, buf| unsafe {
                acc.push(!buf.disjoint(&bounds)?);
                Ok::<Vec<bool>, Error>(acc)
            })?
            .into_iter()
            .cycle();
        for column in self.columns.values_mut() {
            column.buffers.retain(|buf| keep.next().unwrap_or(false))
        }
        Ok(self)
    }

    /// Retain only rows where the [`item`](I) in the specified [`Column`] exactly equals a given
    /// [`value`](I). Useful for boolean flags, integer codes, and enum discriminants.
    ///
    /// ### Guidance
    ///
    /// This filter can be applied to any [equatable](Eq) type. [`Option`] columns test the inner
    /// [`Some`] and exclude [`None`] items.
    ///
    /// ```rust,ignore
    /// .eq("active", true)
    /// .eq("sensor_id", 42u32)
    /// ```
    ///
    /// Refer to the [module-level documentation](self) for more details.
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    pub fn equal<I>(mut self, name: &str, value: I) -> Result<Self, Error>
    where
        I: Serialize,
        Schema: Unfolder<I>,
    {
        self.get_mut(name)?.accepts_mut()?.filters.insert(Filter::equal(&value)?);
        Ok(self) // return to builder pattern
    }

    /// Retain only rows where the [`item`](I) in the specified [`Column`] is a member of a
    /// [finite set](S).
    ///
    /// ### Guidance
    ///
    /// This filter can be applied to any [equatable](Eq) type.
    ///
    /// ```rust,ignore
    /// .one_of("sensor_id", [1u32, 4, 7, 12])
    /// ```
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    pub fn one_of<I, S>(mut self, name: &str, values: S) -> Result<Self, Error>
    where
        I: Serialize,
        S: IntoIterator<Item = I>,
        Schema: Unfolder<I>,
    {
        self.get_mut(name)?.accepts_mut()?.filters.insert(Filter::one_of(values)?);
        Ok(self) // return to builder pattern
    }

    /// Reject any rows where the [`item`](I) in the specified [`Column`] is a member of a
    /// [finite set](S).
    ///
    /// ### Guidance
    ///
    /// This filter can be applied to any [equatable](Eq) type.
    ///
    /// ```rust,ignore
    /// .none_of("status_code", [404u16, 500])
    /// ```
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the requested [`Type`] is incompatible with the actual [`Column`] type.
    pub fn none_of<I, V>(mut self, name: &str, values: V) -> Result<Self, Error>
    where
        I: Serialize,
        V: IntoIterator<Item = I>,
        Schema: Unfolder<I>,
    {
        self.get_mut(name)?.accepts_mut()?.filters.insert(Filter::none_of(values)?);
        Ok(self) // return to builder pattern
    }

    /// Retain only rows where the [`item`](I) in the specified [`Column`] is [`Some`].
    ///
    /// ```rust,ignore
    /// .is_some("calibration")
    /// ```
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the column [`Type`] is not [`Option`].
    //noinspection RsSelfConvention → function name matches the corresponding filter variant
    pub fn is_some(mut self, name: &str) -> Result<Self, Error> {
        self.get_mut(name)?.optional()?.filters.insert(Filter::IsSome);
        Ok(self) // return to builder pattern
    }

    /// Retain only rows where the [`item`](I) in the specified [`Column`] is [`None`].
    ///
    /// ```rust,ignore
    /// .is_none("error_code")
    /// ```
    ///
    /// ### Errors
    ///
    /// - [`Error::Column`] if the named [`Column`] is not found in the [`Query`].
    /// - [`Error::Type`] if the column [`Type`] is not [`Option`].
    //noinspection RsSelfConvention → function name matches the corresponding filter variant
    pub fn is_none(mut self, name: &str) -> Result<Self, Error> {
        self.get_mut(name)?.optional()?.filters.insert(Filter::IsNone);
        Ok(self) // return to builder pattern
    }

    /// Sample every nth row from the result set. Useful for decimation and preview reads on dense
    /// time-series data.
    ///
    /// ```rust,ignore
    /// .stride(10) // return every 10th row
    /// ```
    ///
    /// The default stride value `1` includes every row after filtering.
    pub fn stride(mut self, n: u32) -> Self {
        self.stride = num::NonZeroU32::new(n).unwrap_or(num::NonZeroU32::MIN);
        self // return to builder pattern
    }
}

/* ----------------------------------------------------------------------------- Query Internals */

/// A minimal column **descriptor** for [`Query`] planning and execution.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Encode, Decode, CborLen)]
pub struct Column {
    /// The [`Type`] of values contained within this [`Column`].
    #[n(0)]
    pub ty: Type,
    /// List of [`Buffer`] descriptors for this [`Column`] across all data segments.
    #[cbor(n(1), skip_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    buffers: Vec<manifest::Buffer>,
    /// Deduplicated [`Filter`] set attached to this [`Column`] for lazy evaluation during
    /// [deserialization](Deserialize).
    #[cbor(n(2), skip_if = "HashSet::is_empty")]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "HashSet::is_empty")
    )]
    filters: HashSet<Filter>,
}

impl Column {
    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type; otherwise returns an immutable reference to [`self`](Column) for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Column::accepts`] if a direct **or** nested inner-type match is permissible. Use
    /// [`Column::exact_mut`] if a mutable reference is required for downstream functions.
    pub fn exact<I>(&self) -> Result<&Self, Error>
    where
        Schema: Unfolder<I>,
    {
        let expect = Schema::unfold();
        match self.ty == expect {
            true => Ok(self),
            false => Error::Type { expect, actual: self.ty.clone() }.into(),
        }
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type; otherwise returns a mutable reference to [`self`](Column) for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Column::accepts`] if a direct **or** nested inner-type match is permissible. Use
    /// [`Column::exact`] if an immutable reference is required for downstream functions.
    pub fn exact_mut<I>(&mut self) -> Result<&mut Self, Error>
    where
        Schema: Unfolder<I>,
    {
        self.exact::<I>()?;
        Ok(self)
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type **or** nested inner subtype; otherwise returns an immutable reference to
    /// [`self`](Column) for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Type::exact`] if a direct non-nested match is required. Use
    /// [`Column::accepts_mut`] if a mutable reference is required for downstream functions.
    pub fn accepts<I>(&self) -> Result<&Self, Error>
    where
        Schema: Unfolder<I>,
    {
        let inner = Schema::unfold();
        match self.ty == inner || matches!(&self.ty, Type::Option { subtype: s } if **s == inner) {
            true => Ok(self),
            false => Error::Type { expect: inner, actual: self.ty.clone() }.into(),
        }
    }

    /// Returns [`Error::Type`] if the requested [`Type`] does not match the on-disk [`Column`]
    /// type **or** nested inner subtype; otherwise returns a mutable reference to [`self`](Column)
    /// for method chaining.
    ///
    /// ### Guidance
    ///
    /// Refer to [`Type::exact`] if a direct non-nested match is required. Use [`Column::accepts`]
    /// if an immutable reference is required for downstream functions.
    pub fn accepts_mut<I>(&mut self) -> Result<&mut Self, Error>
    where
        Schema: Unfolder<I>,
    {
        self.accepts()?;
        Ok(self)
    }

    /// Returns [`Error::Type`] if the on-disk [`column`](Column)`.`[`type`](Type) is not
    /// [`Option`]; otherwise returns an immutable reference to [`self`](Column) for method
    /// chaining.
    fn optional(&mut self) -> Result<&mut Self, Error> {
        let option = || Type::Option { subtype: Type::Any.into() };
        match &self.ty {
            Type::Option { .. } => Ok(self),
            other => Error::Type { expect: option(), actual: other.clone() }.into(),
        }
    }

    /// Map the provided [`Key`](String) to a new empty [`Column`].
    pub(crate) fn map(entry: (&String, &manifest::Column)) -> (String, Self) {
        (entry.0.clone(), entry.1.clone().into())
    }
}

impl From<manifest::Column> for Column {
    fn from(src: manifest::Column) -> Self {
        Self {
            ty: src.ty,
            buffers: src.buffers,
            filters: HashSet::new(),
        }
    }
}

/// A row-level predicate lazily evaluated during [deserialization](Deserialize).
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq, Hash, Encode, Decode, CborLen)]
#[non_exhaustive] // To accommodate potential future filter types.
#[doc(hidden)] // Reachable through the Evaluate trait for manual implementation.
pub enum Filter {
    /// Retain items within the specified range.
    #[n(0)]
    Range {
        /// Lower bound
        #[n(0)]
        lb: Bound<[u8; B]>,
        /// Upper bound
        #[n(1)]
        ub: Bound<[u8; B]>,
    },
    /// Retain items that are exactly [equal](Eq) to the inner operand.
    ///
    /// ### Wrapped Data
    ///
    /// The equality operand is [serialized](Serialize) as LE bytes into a fixed-size array with
    /// trailing zeros. [Deserialize] according to the [`Type`] specified by the [`Schema`].
    #[n(1)]
    Eq(#[cbor(n(0), with = "minicbor::bytes")] [u8; B]),
    /// Retain items that are a member of the operand set.
    ///
    /// ### Wrapped Data
    ///
    /// Each equality operand is [serialized](Serialize) as LE bytes into a fixed-size array with
    /// trailing zeros and collected into a [`BTreeSet`] to ensure uniqueness. [Deserialize]
    /// according to the [`Type`] specified by the [`Schema`].
    #[n(2)]
    OneOf(#[cbor(n(0), skip_if = "BTreeSet::is_empty")] BTreeSet<[u8; B]>),
    /// Reject items that are a member of the operand set.
    ///
    /// ### Wrapped Data
    ///
    /// Each equality operand is [serialized](Serialize) as LE bytes into a fixed-size array with
    /// trailing zeros and collected into a [`BTreeSet`] to ensure uniqueness. [Deserialize]
    /// according to the [`Type`] specified by the [`Schema`].
    #[n(3)]
    NoneOf(#[cbor(n(0), skip_if = "BTreeSet::is_empty")] BTreeSet<[u8; B]>),
    /// Retain [`Option`] items that are [`Some`].
    #[n(4)]
    IsSome,
    /// Retain [`Option`] items that are [`None`].
    #[n(5)]
    IsNone,
}

impl Filter {
    /// [`Serialize`] each unique [item](I) from a [finite set](S).
    fn set<I, S>(set: S) -> Result<BTreeSet<[u8; B]>, number::Error>
    where
        I: Serialize,
        S: IntoIterator<Item = I>,
    {
        set.into_iter().map(|i| [u8::MIN; B].serialize_push(&i)).collect()
    }

    /* --------------------------------------------------------------------- Filter Constructors */

    /// Construct a [`Filter::Range`] from the provided [`range`](RangeBounds).
    pub(crate) fn bounds<B, I>(range: &B) -> Self
    where
        B: RangeBounds<I>,
        I: Serialize,
    {
        Self::Range {
            lb: range.start_bound().map(|v| [u8::MIN; B].serialize_push(v).unwrap_or([u8::MIN; B])),
            ub: range.end_bound().map(|v| [u8::MAX; B].serialize_push(v).unwrap_or([u8::MAX; B])),
        }
    }

    /// Construct a [`Filter::Eq`] from the provided [`item`](I).
    fn equal<I: Serialize>(item: &I) -> Result<Self, number::Error> {
        Ok(Self::Eq([u8::MIN; B].serialize_push(item)?))
    }

    /// Construct a [`Filter::OneOf`] from the provided [`item`](I) [`set`](S).
    fn one_of<I, S>(set: S) -> Result<Self, number::Error>
    where
        I: Serialize,
        S: IntoIterator<Item = I>,
    {
        Ok(Self::OneOf(Self::set(set)?))
    }

    /// Construct a [`Filter::NoneOf`] from the provided [`item`](I) [`set`](S).
    fn none_of<I, S>(set: S) -> Result<Self, number::Error>
    where
        I: Serialize,
        S: IntoIterator<Item = I>,
    {
        Ok(Self::NoneOf(Self::set(set)?))
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Filter::Range { .. } => write!(f, "Filter::Range"),
            Filter::Eq(..) => write!(f, "Filter::Eq"),
            Filter::OneOf(..) => write!(f, "Filter::OneOf"),
            Filter::NoneOf(..) => write!(f, "Filter::NoneOf"),
            Filter::IsSome => write!(f, "Filter::IsSome"),
            Filter::IsNone => write!(f, "Filter::IsNone"),
        }
    }
}

/* ------------------------------------------------------------------- Evaluate Trait Definition */

/// A **type** that can be tested against a [`Filter`] during [deserialization][1].
///
/// [1]: Deserialize::deserialize
pub trait Evaluate: Sized {
    /// Returns `true` if `self` is contained within the specified [`Range`](RangeBounds).
    fn range<S>(&self, lb: &Bound<S>, ub: &Bound<S>) -> Result<bool, io::Error>
    where
        Self: Deserialize + PartialOrd,
        S: AsRef<[u8]>,
    {
        let above = match lb {
            Bound::Included(bytes) => *self >= bytes.as_ref().deserialize_into()?,
            Bound::Excluded(bytes) => *self > bytes.as_ref().deserialize_into()?,
            Bound::Unbounded => true,
        };
        let below = match ub {
            Bound::Included(bytes) => *self <= bytes.as_ref().deserialize_into()?,
            Bound::Excluded(bytes) => *self < bytes.as_ref().deserialize_into()?,
            Bound::Unbounded => true,
        };
        Ok(above && below)
    }

    /// Returns `true` if `self` is exactly equal to [`other`](O)
    fn equal<O>(&self, other: &O) -> Result<bool, io::Error>
    where
        Self: Deserialize + PartialEq,
        O: AsRef<[u8]>,
    {
        Ok(*self == other.as_ref().deserialize_into()?)
    }

    /// Returns `true` if `self` is a member of the specified [set](S).
    fn one_of<S>(&self, set: &S) -> Result<bool, io::Error>
    where
        Self: Deserialize + PartialEq,
        for<'a> &'a S: IntoIterator<Item = &'a [u8; B]>,
    {
        set.into_iter().try_fold(false, |acc, bytes| match acc {
            true => Ok(true), // short-circuit without deserializing
            false => Ok(*self == bytes.as_ref().deserialize_into()?),
        })
    }

    /// Returns `true` if `self` is not a member of the specified [set](S).
    fn none_of<S>(&self, set: &S) -> Result<bool, io::Error>
    where
        Self: Deserialize + PartialEq,
        for<'a> &'a S: IntoIterator<Item = &'a [u8; B]>,
    {
        set.into_iter().try_fold(true, |acc, bytes| match acc {
            true => Ok(*self != bytes.as_ref().deserialize_into()?),
            false => Ok(false), // short-circuit without deserializing
        })
    }

    /// Returns `true` if `self` is [`Some`].
    fn is_some(&self) -> Result<bool, io::Error> {
        Ok(true) // NOTE: non-option types (default) return true; cannot be None by definition
    }

    /// Returns `true` if `self` is [`None`].
    fn is_none(&self) -> Result<bool, io::Error> {
        Ok(false) // NOTE: non-option types (default) return false; must be Some by definition
    }

    /// Dispatch function that returns `true` if `self` satisfies the provided [`Filter`].
    fn assess(&self, filter: &Filter) -> Result<bool, io::Error>;

    /// Returns [`Outcome::Include`] if `self` satisfies **every** [`Filter`].
    fn evaluate<S>(self, filters: &S) -> Outcome<Self>
    where
        for<'a> &'a S: IntoIterator<Item = &'a Filter>,
    {
        match filters.into_iter().try_fold(true, |acc, f| match acc {
            false => Ok(false), // short-circuit without evaluating the remaining filters
            true => self.assess(f),
        }) {
            Ok(true) => Outcome::Include(self),
            Ok(false) => Outcome::Exclude,
            Err(e) => Outcome::Error(e),
        }
    }
}

/* --------------------------------------------------------------- Evaluate Trait Implementation */

impl<I> Evaluate for I
where
    I: Unfold<RawAcc = Vec<I>, OptAcc = OptBitVec<I>> + Deserialize + PartialOrd,
{
    fn assess(&self, filter: &Filter) -> Result<bool, io::Error> {
        match filter {
            Filter::Range { lb, ub } => self.range(lb, ub),
            Filter::Eq(other) => self.equal(other),
            Filter::OneOf(set) => self.one_of(set),
            Filter::NoneOf(set) => self.none_of(set),
            Filter::IsSome => self.is_some(),
            Filter::IsNone => self.is_none(),
        }
    }
}

impl<I> Evaluate for Option<I>
where
    I: Deserialize + PartialOrd,
    Option<I>: Deserialize,
{
    fn is_some(&self) -> Result<bool, io::Error> {
        Ok(self.is_some())
    }

    fn is_none(&self) -> Result<bool, io::Error> {
        Ok(self.is_none())
    }

    fn assess(&self, filter: &Filter) -> Result<bool, io::Error> {
        match filter {
            Filter::Range { lb, ub } => self.range(lb, ub),
            Filter::Eq(other) => self.equal(other),
            Filter::OneOf(set) => self.one_of(set),
            Filter::NoneOf(set) => self.none_of(set),
            Filter::IsSome => Evaluate::is_some(self),
            Filter::IsNone => Evaluate::is_none(self),
        }
    }
}

impl Evaluate for bool {
    fn assess(&self, filter: &Filter) -> Result<bool, io::Error> {
        match filter {
            Filter::Eq(other) => Ok(other[0] == *self as u8),
            Filter::IsSome => self.is_some(),
            Filter::IsNone => self.is_none(),
            some => io::Error::Filter { filter: some.clone(), actual: Type::Bool }.into(),
        }
    }
}

/* ------------------------------------------------------------------------------ Specific Error */

/// Errors returned from [`Query`] construction and execution.
///
/// Enum variants cover various granular error cases that may arise when working with queries.
/// Users should consider handling errors explicitly wherever possible to provide meaningful
/// error messages and recovery actions.
///
/// ### Implementation
///
/// This enum is `#[non_exhaustive]` meaning additional variants may be added in future versions.
/// Implementers are advised to include a wildcard arm `_` to account for potential additions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
#[non_exhaustive] // To accommodate potential future error cases.
pub enum Error {
    /// The requested [`Column`] name was not found in the query [`BTreeMap`].
    Column(String),
    /// Underlying [`io::Error`] from the [clem](crate) [file](io::File).
    Io(io::Error),
    /// Underlying [`number::Error`] from a numerical operation or conversion.
    Number(number::Error),
    /// The requested [`Type`] did not match the actual on-disk [`Column`] type.
    Type {
        /// The [`Type`] expected by the caller.
        expect: Type,
        /// The actual on-disk column [`Type`].
        actual: Type,
    },
}

impl Error {
    /// Constructor for [`Error::Column`] wrapping the provided column [`name`](S).
    pub(crate) fn column<S>(name: S) -> Self
    where
        String: From<S>,
    {
        let owned = name.into();
        Self::Column(owned)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(name) => write!(f, "Column '{name}' not found"),
            Self::Io(e) => write!(f, "Query IO error → {e}"),
            Self::Number(e) => write!(f, "Number error → {e}"),
            Self::Type { expect, actual } => write!(f, "Type error → {expect} ≠ {actual}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(src: io::Error) -> Self {
        match src {
            io::Error::Number(e) => e.into(), // Flatten number error nesting
            other => Self::Io(other),
        }
    }
}

impl From<number::Error> for Error {
    fn from(e: number::Error) -> Self {
        Self::Number(e)
    }
}

impl From<TryFromIntError> for Error {
    fn from(e: TryFromIntError) -> Self {
        number::Error::from(e).into()
    }
}

//noinspection DuplicatedCode → Conversion is implemented for error types in different modules.
impl<T, E> From<Error> for Result<T, E>
where
    E: From<Error>,
{
    fn from(error: Error) -> Self {
        Err(E::from(error))
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use bitvec::vec::BitVec;
    use memmap2::MmapMut;

    use super::*;
    use crate::accumulate::{Accumulate, OptBitVec, OptInSitu, OptSeq, Seq};
    use crate::Sector;

    /// Collect the [`Include`](Outcome::Include) values from a [`Stream`], dropping
    /// [`Exclude`](Outcome::Exclude) and panicking on any [`Error`](Outcome::Error).
    fn collected<I>(stream: Stream<I>) -> Vec<I> {
        stream
            .filter_map(|outcome| match outcome {
                Outcome::Include(item) => Some(item),
                Outcome::Exclude => None,
                Outcome::Error(error) => panic!("Read error → {error}"),
            })
            .collect()
    }

    /// Build a single-segment `u32` [`Column`] descriptor with the given statistics. The `min` and
    /// `max` statistics are [serialized](Serialize) into their fixed-size [`[u8; B]`](B) arrays.
    fn column(min: u32, max: u32, count: u64) -> Column {
        let buffer = manifest::Buffer {
            sector: Sector {
                offset: u64::MIN,
                length: NonZeroU64::MIN,
            },
            count: NonZeroU64::new(count).expect("Zero rows"),
            min: [u8::MIN; B].serialize_push(&min).expect("min"),
            max: [u8::MAX; B].serialize_push(&max).expect("max"),
        };
        Column {
            ty: Type::U32,
            buffers: vec![buffer],
            filters: HashSet::new(),
        }
    }

    /// Build a single-column [`Query`] named `v` over the provided serialized bytes.
    fn query(bytes: &[u8], ty: Type, count: u64) -> Query {
        let mut mmap = MmapMut::map_anon(bytes.len().max(1)).expect("Anonymous map failed");
        mmap[..bytes.len()].copy_from_slice(bytes);
        let header = manifest::Buffer::HEADER as u64;
        let buffer = manifest::Buffer {
            sector: Sector {
                offset: header,
                length: NonZeroU64::new(bytes.len() as u64 - header).expect("Empty body"),
            },
            count: NonZeroU64::new(count).expect("Zero rows"),
            min: [u8::MIN; B],
            max: [u8::MAX; B],
        };
        let column = Column {
            ty,
            buffers: vec![buffer],
            filters: HashSet::new(),
        };
        Query {
            mmap: Arc::new(mmap.make_read_only().expect("Read-only conversion failed")),
            columns: BTreeMap::from([(String::from("v"), column)]),
            stride: num::NonZeroU32::MIN,
        }
    }

    #[test]
    fn disjoint_below_and_above() {
        let column = column(10, 20, 3);
        let buffer = &column.buffers[0];
        // SAFETY: the descriptor and bounds are both `u32`, matching the column type.
        // Segment [10, 20] is disjoint from 30.. and from ..5
        assert!(unsafe { buffer.disjoint(&(30u32..)) }.expect("ok"));
        assert!(unsafe { buffer.disjoint(&(..5u32)) }.expect("ok"));
        // Segment [10, 20] overlaps 15..25
        assert!(!unsafe { buffer.disjoint(&(15u32..25)) }.expect("ok"));
    }

    #[test]
    fn column_round_trip() {
        let data: Vec<u32> = vec![10, 20, 30];
        let bytes = data.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::U32, 3);
        let rows: Vec<u32> = query
            .column::<u32>("v")
            .expect("Column failed")
            .map(|outcome| match outcome {
                Outcome::Include(item) => item,
                other => panic!("Unexpected outcome → {other:?}"),
            })
            .collect();
        assert_eq!(rows, data);
    }

    #[test]
    fn column_type_mismatch_errors() {
        let bytes = vec![1u32].serialize().expect("Serialize failed");
        let query = query(&bytes, Type::U32, 1);
        assert!(matches!(query.column::<u16>("v"), Err(Error::Type { .. })));
    }

    /// A bit-packed [`bool`] column streams back exactly `count` bits (no trailing padding bits).
    #[test]
    fn bool_column_round_trip() {
        let mut acc = BitVec::default();
        [true, false, true].into_iter().for_each(|bit| acc.push(bit));
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::Bool, 3);
        let rows = collected(query.column::<bool>("v").expect("Column failed"));
        assert_eq!(rows, vec![true, false, true]);
    }

    /// An [`OptBitVec`] column round-trips optionals; only [`Some`] values occupy the data region.
    #[test]
    fn opt_bit_vec_column_round_trip() {
        let mut acc = OptBitVec::<u32>::default();
        [Some(1u32), None, Some(3)].into_iter().for_each(|v| acc.push(v));
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::option(Type::U32), 3);
        let rows = collected(query.column::<Option<u32>>("v").expect("Column failed"));
        assert_eq!(rows, vec![Some(1), None, Some(3)]);
    }

    /// A niche optional column streams via the byte reader; the niche encodes [`None`].
    #[test]
    fn niche_option_column_round_trip() {
        let mut acc = OptInSitu::<NonZeroU64>::default();
        [NonZeroU64::new(5), None, NonZeroU64::new(7)].into_iter().for_each(|v| acc.push(v));
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::option(Type::NZU64), 3);
        let rows = collected(query.column::<Option<NonZeroU64>>("v").expect("Column failed"));
        assert_eq!(rows, vec![NonZeroU64::new(5), None, NonZeroU64::new(7)]);
    }

    /// A [`Seq`] column round-trips unsized rows from base-1 cumulative end offsets.
    #[test]
    fn seq_column_round_trip() {
        let mut acc = Seq::<u8>::default();
        acc.push(vec![97, 98, 99]);
        acc.push(vec![100, 101]);
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::sequence(Type::U8), 2);
        let rows = collected(query.column::<Vec<u8>>("v").expect("Column failed"));
        assert_eq!(rows, vec![vec![97, 98, 99], vec![100, 101]]);
    }

    /// An optional [`Seq`] column round-trips optionals; the zero-offset niche encodes [`None`].
    #[test]
    fn opt_seq_column_round_trip() {
        let mut acc = OptSeq::<u8>::default();
        acc.push(Some(vec![97, 98]));
        acc.push(None::<Vec<u8>>);
        acc.push(Some(vec![99]));
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::option(Type::sequence(Type::U8)), 3);
        let rows = collected(query.column::<Option<Vec<u8>>>("v").expect("Column failed"));
        assert_eq!(rows, vec![Some(vec![97, 98]), None, Some(vec![99])]);
    }

    /// A [`String`] column decodes each row's UTF-8 byte run, including multi-byte scalars.
    #[test]
    fn string_column_round_trip() {
        let mut acc = Seq::<u8>::default();
        acc.push("héllo".as_bytes().to_vec());
        acc.push("xyz".as_bytes().to_vec());
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::String, 2);
        let rows = collected(query.column::<String>("v").expect("Column failed"));
        assert_eq!(rows, vec![String::from("héllo"), String::from("xyz")]);
    }

    /// A [`str`] column borrows each row zero-copy from the map; the caller can collect at will.
    #[test]
    fn str_column_zero_copy() {
        let mut acc = Seq::<u8>::default();
        acc.push(b"abc".to_vec());
        acc.push(b"de".to_vec());
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::String, 2);
        let rows = collected(query.column::<&str>("v").expect("Column failed"));
        assert_eq!(rows, vec!["abc", "de"]);
    }

    /// An [`eq`](Query::equal) filter excludes non-matching rows during iteration.
    #[test]
    fn eq_filter_excludes_non_matching() {
        let bytes = vec![10u32, 20, 30].serialize().expect("Serialize failed");
        let query = query(&bytes, Type::U32, 3).equal("v", 20u32).expect("eq failed");
        let rows = collected(query.column::<u32>("v").expect("Column failed"));
        assert_eq!(rows, vec![20]);
    }

    /// [`eq`](Query::equal) streams non-deserializable [`bool`] columns via the mask reader.
    #[test]
    fn eq_filter_bool_column() {
        let mut acc = BitVec::default();
        [true, false, true].into_iter().for_each(|bit| acc.push(bit));
        let bytes = acc.serialize().expect("Serialize failed");
        let query = query(&bytes, Type::Bool, 3).equal("v", false).expect("eq failed");
        let rows = collected(query.column::<bool>("v").expect("Column failed"));
        assert_eq!(rows, vec![false]);
    }

    /// [`is_some`](Query::is_some) retains [`Some`] rows; [`is_none`](Query::is_none) retains
    /// [`None`] rows, delegating validity to the optional mask.
    #[test]
    fn validity_filters_split_optionals() {
        let bytes = {
            let mut acc = OptBitVec::<u32>::default();
            [Some(1u32), None, Some(3)].into_iter().for_each(|v| acc.push(v));
            acc.serialize().expect("Serialize failed")
        };
        let some = query(&bytes, Type::option(Type::U32), 3).is_some("v").expect("is_some failed");
        assert_eq!(
            collected(some.column::<Option<u32>>("v").expect("Column failed")),
            vec![Some(1), Some(3)]
        );
        let none = query(&bytes, Type::option(Type::U32), 3).is_none("v").expect("is_none failed");
        assert_eq!(
            collected(none.column::<Option<u32>>("v").expect("Column failed")),
            vec![None]
        );
    }

    /// The builder eagerly rejects a validity filter on a non-optional column; the requested
    /// [`Option`] type does not match the on-disk scalar [`Type`].
    #[test]
    fn is_some_on_non_optional_errors() {
        let bytes = vec![1u32].serialize().expect("Serialize failed");
        let result = query(&bytes, Type::U32, 1).is_some("v");
        assert!(matches!(result, Err(Error::Type { .. })));
    }

    /// The builder eagerly rejects a value filter whose type is incompatible with the column.
    #[test]
    fn eq_type_mismatch_errors() {
        let bytes = vec![1u32].serialize().expect("Serialize failed");
        let result = query(&bytes, Type::U32, 1).equal("v", true);
        assert!(matches!(result, Err(Error::Type { .. })));
    }
}
