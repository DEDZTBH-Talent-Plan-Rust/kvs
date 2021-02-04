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

use crate::error::Error;
pub use crate::error::ErrorKind;
pub use crate::kvlog::KVLog;
use failure::ResultExt;
use std::fs::*;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

/// Since there is only 1 log file right now, its name is hardcoded.
const LOG_FILE_NAME: &str = "0.bin";

/// Write buffer size is 64 KiB. This allows for lower writing frequency.
const WRITE_BUFFER_SIZE: usize = 64 * 1024;

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
    /// Path to the log file
    log_file_path: PathBuf,
    append_writer: BufWriter<File>,
}

impl Drop for KvStore {
    /// To make sure all buffers are flushed.
    fn drop(&mut self) {
        match self.append_writer.flush() {
            Ok(_) => {}
            Err(e) => eprintln!("An error occurred when flushing buffer: {}", e.to_string()),
        }
    }
}

/// Not eof
fn has_more<R: BufRead>(mut reader: R) -> Result<bool> {
    Ok(reader.fill_buf().context(ErrorKind::Io)?.len() > 0)
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
        let kvlog = KVLog::new_set(key, value);
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
    /// # Errors
    ///
    /// KeyNotFound - If the key does not exist.
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
        let f = OpenOptions::new()
            .read(true)
            .open(&self.log_file_path)
            .context(ErrorKind::Io)?;
        let mut reader = BufReader::new(f);
        let mut key_exists = false;
        while has_more(&mut reader)? {
            match KVLog::deserialize_from_reader(&mut reader)? {
                KVLog::Set(log_key, _) => {
                    if log_key == key {
                        key_exists = true
                    }
                }
                KVLog::Rm(log_key) => {
                    if log_key == key {
                        key_exists = false
                    }
                }
            }
        }
        if key_exists {
            let kvlog = KVLog::new_rm(key);
            kvlog.serialize_to_writer(&mut self.append_writer)?;
            Ok(())
        } else {
            Err(Error::from(ErrorKind::KeyNotFound))
        }
    }

    /// Opens a KvStore from given directory and setup the in-memory KvStore.
    ///
    /// The directory will be created if not exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let dir_path = path.as_path();
        if !dir_path.exists() {
            create_dir(dir_path).context(ErrorKind::Io)?;
        }
        let log_file_path = dir_path.join(LOG_FILE_NAME);
        let append_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .context(ErrorKind::Io)?;
        let append_writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, append_file);
        Ok(KvStore {
            log_file_path,
            append_writer,
        })
    }
}
