#![deny(missing_docs)]
//! Defines a log and its ser/de behavior.
//!
//! I used bincode ser/de format. It is simple, minimizes the space used by
//! each log by only storing what is necessary (no field names), and content
//! of key/value is human-readable to certain extent.

use crate::error::ErrorKind;
use crate::Result;
use failure::ResultExt;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize, Debug)]
/// Definition of KvLog.
pub enum KvLog {
    /// set command, stores key and value
    Set(String, String),
    /// remove command, stores key
    Rm(String),
}

impl KvLog {
    /// Creating a new KvLog::Set
    pub fn new_set(key: String, value: String) -> KvLog {
        KvLog::Set(key, value)
    }

    /// Creating a new KvLog::Rm
    pub fn new_rm(key: String) -> KvLog {
        KvLog::Rm(key)
    }

    /// Serialize to writer using bincode format
    ///
    /// # Errors
    ///
    /// Serde - Serialization of a `KvLog` failed.
    ///
    pub fn serialize_to_writer<W>(&self, writer: W) -> Result<()>
    where
        W: io::Write,
    {
        bincode::serialize_into(writer, self).context(ErrorKind::Serde)?;
        Ok(())
    }

    /// Deserialize from reader using bincode format
    ///
    /// # Errors
    ///
    /// Serde - Deserialization of a `KvLog` failed.
    ///
    pub fn deserialize_from_reader<R>(reader: R) -> Result<KvLog>
    where
        R: io::Read,
    {
        let kvstore = bincode::deserialize_from(reader).context(ErrorKind::Serde)?;
        Ok(kvstore)
    }

    /// Turn KvLog into its key.
    pub fn into_key(self) -> String {
        match self {
            KvLog::Set(k, _) => k,
            KvLog::Rm(k) => k,
        }
    }
}
