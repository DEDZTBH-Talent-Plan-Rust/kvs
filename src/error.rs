#![deny(missing_docs)]
//! Defines error for KvStore
//!
//! Using the "An Error and ErrorKind pair" pattern

use failure::Backtrace;
use failure::{Context, Fail};
use std::fmt;
use std::fmt::Display;

#[derive(Debug)]
/// Error by KvStore
pub struct Error {
    inner: Context<ErrorKind>,
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        *self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
/// Error kind of KvStore
pub enum ErrorKind {
    #[fail(display = "IO Error occurred")]
    /// Error caused by io
    Io,
    #[fail(display = "ser/de Error occurred")]
    /// Error caused by serde
    Serde,
}
