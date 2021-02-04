#![deny(missing_docs)]
//! kvs is a key-value pair storage using log structure.
//!
//! # Examples
//!
//! ```rust
//! use kvs::KvStore;
//! use tempfile::TempDir;
//!
//! let mut kv = KvStore::open(TempDir::new().unwrap()).unwrap();
//! assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
//!
//! kv.set("key1".to_owned(), "42".to_owned()).unwrap();
//! kv.set("key2".to_owned(), "43".to_owned()).unwrap();
//! assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("42".to_owned()));
//! assert_eq!(kv.get("key2".to_owned()).unwrap(), Some("43".to_owned()));
//!
//! kv.remove("key1".to_owned()).unwrap();
//! kv.remove("key3".to_owned()).unwrap();
//! assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
//! ```

mod error;
mod kvlog;

pub use crate::kvlog::KVLog;

use crate::error::{Error, ErrorKind};
use failure::ResultExt;
use std::fs::*;
use std::io::BufWriter;
use std::path::PathBuf;

/// Since there is only 1 log file right now, its name is hardcoded.
static LOG_FILE_NAME: &str = "0.bin";

/// Result type of KvStore
pub type Result<T> = std::result::Result<T, Error>;

/// A KvStore stores key-value pairs in memory.
///
/// KvStore uses a `HashMap<String, String>` to store data.
///
/// # Examples
///
/// ```rust
/// use kvs::KvStore;
/// use tempfile::TempDir;
///
/// let mut kv = KvStore::open(TempDir::new().unwrap()).unwrap();
///
/// kv.set("key1".to_owned(), "42".to_owned()).unwrap();
/// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("42".to_owned()));
///
/// kv.remove("key1".to_owned()).unwrap();
/// assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
/// ```
pub struct KvStore {
    append_writer: BufWriter<File>,
}

impl KvStore {
    /// Inserts a key-value pair into the map.
    ///
    /// If the `KvStore` did have this key present, the value is updated.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let mut kv = KvStore::open(TempDir::new().unwrap()).unwrap();
    ///
    /// kv.set("key1".to_owned(), "12".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("12".to_owned()));
    ///
    /// kv.set("key1".to_owned(), "11".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("11".to_owned()));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // we only need to append
        let kvlog = KVLog::new(key, value);
        kvlog.serialize_to_writer(&mut self.append_writer)?;
        Ok(())
    }

    /// Returns the value corresponding to the key.
    ///
    /// The returned value is a copy of the value stored in `KvStore` if present.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let mut kv = KvStore::open(TempDir::new().unwrap()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
    ///
    /// kv.set("key1".to_owned(), "12".to_owned()).unwrap();
    /// let returned_opt = kv.get("key1".to_owned()).unwrap();
    /// assert_eq!(returned_opt, Some("12".to_owned()));
    ///
    /// kv.set("key1".to_owned(), "11".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("11".to_owned()));
    /// assert_eq!(returned_opt, Some("12".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    /// Removes a key from the map if the key is present.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let mut kv = KvStore::open(TempDir::new().unwrap()).unwrap();
    ///
    /// kv.remove("key1".to_owned()).unwrap(); // nothing will change
    ///
    /// kv.set("key1".to_owned(), "12".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("12".to_owned()));
    ///
    /// kv.remove("key1".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
    ///
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!();
    }

    /// Opens a KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let dir_path = path.as_path();
        if !dir_path.exists() {
            create_dir(dir_path);
        }
        let log_file_path = dir_path.join(LOG_FILE_NAME);
        let append_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path)
            .context(ErrorKind::Io)?;
        let append_writer = BufWriter::new(append_file);
        Ok(KvStore { append_writer })
    }
}
