#![deny(missing_docs)]
//! Defines a log and its ser/de behavior.
//!
//! I used bincode ser/de format. It is simple, minimizes the space used by
//! each log by only storing the key/value length and content, and content
//! of key/value is human-readable to certain extent.

use crate::error::ErrorKind;
use crate::Result;
use failure::ResultExt;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize, Debug)]
/// Definition of KVLog.
pub struct KVLog {
    /// key in a log
    pub key: String,
    /// value in a log
    pub value: String,
}

impl KVLog {
    /// Creating a new KVLog
    pub fn new(key: String, value: String) -> KVLog {
        KVLog { key, value }
    }

    /// Serialize to writer using bincode format
    pub fn serialize_to_writer<W>(&self, writer: W) -> Result<()>
    where
        W: io::Write,
    {
        bincode::serialize_into(writer, self).context(ErrorKind::Serde)?;
        Ok(())
    }

    /// Deserialize from reader using bincode format
    pub fn deserialize_from_reader<R>(reader: R) -> Result<KVLog>
    where
        R: io::Read,
    {
        let kvstore = bincode::deserialize_from(reader).context(ErrorKind::Serde)?;
        Ok(kvstore)
    }
}
