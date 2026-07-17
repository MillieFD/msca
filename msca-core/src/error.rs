/*
Project: msca
GitHub: https://github.com/MillieFD/msca

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fmt;

use crate::schema::number;
use crate::{io, manifest, query, schema, segment};

/* ------------------------------------------------------------------------------ Public Exports */

/// Errors returned by [msca](crate).
///
/// Enum variants cover various granular error cases that may arise when working with datasets,
/// schemas, or column operations. Users should consider handling errors explicitly wherever
/// possible to provide meaningful error messages and recovery actions.
///
/// ### Implementation
///
/// This enum is `#[non_exhaustive]` meaning additional variants may be added in future versions.
/// Implementers are advised to include a wildcard arm `_` to account for potential additions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
#[non_exhaustive] // To accommodate potential future error cases.
pub enum Error {
    /// Underlying [`io::Error`] from the [msca](crate) [file](io::File).
    Io(io::Error),
    /// Underlying [`manifest::Error`] from [`Segment`](segment::Segment) registration or retrieval.
    Manifest(manifest::Error),
    /// Underlying [`number::Error`] from a numerical operation or conversion.
    Number(number::Error),
    /// Underlying [`query::Error`] from [querying](query) the [`Dataset`](crate::Dataset).
    Query(query::Error),
    /// Underlying [`schema::Error`] from schema composition.
    Schema(schema::Error),
    /// Underlying [`segment::Error`] while encoding a `Segment`
    Segment(segment::Error),
    /// Underlying [`std::array::TryFromSliceError`] while parsing a slice into a fixed-size
    /// array.
    Slice(std::array::TryFromSliceError),
    /// Underlying [`std::str::Utf8Error`] while attempting to interpret `[u8]` as a [`String`].
    Utf8(std::str::Utf8Error),
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(io::Error::Io(e)) => write!(f, "File IO error → {e}"),
            Self::Io(e) => write!(f, "File IO error → {e}"),
            Self::Manifest(e) => write!(f, "Manifest error → {e}"),
            Self::Number(e) => write!(f, "Number error → {e}"),
            Self::Query(e) => write!(f, "Query error → {e}"),
            Self::Schema(e) => write!(f, "Schema error → {e}"),
            Self::Segment(e) => write!(f, "Segment error → {e}"),
            Self::Slice(e) => write!(f, "Try from slice error → {e}"),
            Self::Utf8(e) => write!(f, "UTF8 from bytes error → {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Manifest(e) => Some(e),
            Self::Number(e) => Some(e),
            Self::Query(e) => Some(e),
            Self::Schema(e) => Some(e),
            Self::Segment(e) => Some(e),
            Self::Slice(e) => Some(e),
            Self::Utf8(e) => Some(e),
            other => None, // Some variants do not wrap an inner error source
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        io::Error::from(error).into()
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::Utf8(error)
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(error: std::num::TryFromIntError) -> Self {
        number::Error::from(error).into()
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(error: std::array::TryFromSliceError) -> Self {
        Self::Slice(error)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

impl From<minicbor::decode::Error> for Error {
    fn from(error: minicbor::decode::Error) -> Self {
        io::Error::from(error).into()
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        match error {
            io::Error::Manifest(e) => Self::Manifest(e),
            io::Error::Number(e) => Self::Number(e),
            io::Error::Slice(e) => Self::Slice(e),
            io::Error::Schema(e) => Self::Schema(e),
            other => Self::Io(other),
        }
    }
}

impl From<manifest::Error> for Error {
    fn from(error: manifest::Error) -> Self {
        Self::Manifest(error)
    }
}

impl From<number::Error> for Error {
    fn from(error: number::Error) -> Self {
        Self::Number(error)
    }
}

impl From<query::Error> for Error {
    fn from(error: query::Error) -> Self {
        match error {
            query::Error::Number(e) => Self::Number(e),
            other => Self::Query(other),
        }
    }
}

impl From<schema::Error> for Error {
    fn from(error: schema::Error) -> Self {
        match error {
            schema::Error::Number(e) => Self::Number(e),
            other => Self::Schema(other),
        }
    }
}

impl From<segment::Error> for Error {
    fn from(error: segment::Error) -> Self {
        Self::Segment(error)
    }
}

/* --------------------------------------------------------------------------------------- Tests */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_io_error() {
        use std::io::{self, ErrorKind};
        let source = io::Error::new(ErrorKind::Other, "Test IO error");
        let error: Error = source.into();
        assert_eq!(error.to_string(), "File IO error → Test IO error");
    }

    #[test]
    #[allow(invalid_from_utf8)]
    fn from_utf8_error() {
        use std::str;
        let source = str::from_utf8(b"\xFF").unwrap_err();
        let error: Error = source.into();
        assert!(error.to_string().starts_with("UTF8 from bytes error →"));
    }
}
