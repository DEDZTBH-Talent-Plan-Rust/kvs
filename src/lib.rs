#![deny(missing_docs)]
//! kvs is a key-value pair storage using log structure.
//!
//! # Examples
//!
//! ```rust
//! use kvs::KvStore;
//! use tempfile::TempDir;
//!
//! let tempdir = TempDir::new().unwrap();
//! let path = tempdir.path();
//! let mut kv = KvStore::open(path).unwrap();
//! assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
//!
//! kv.set("key1".to_owned(), "42".to_owned()).unwrap();
//! kv.set("key2".to_owned(), "43".to_owned()).unwrap();
//! assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("42".to_owned()));
//! assert_eq!(kv.get("key2".to_owned()).unwrap(), Some("43".to_owned()));
//!
//! kv.remove("key1".to_owned()).unwrap();
//! kv.remove("key3".to_owned()).unwrap_err(); // missing key
//! assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
//! ```

mod error;
mod kvlog;

use crate::error::Error;
pub use crate::error::ErrorKind;
pub use crate::kvlog::KvLog;
use failure::ResultExt;
use std::collections::HashMap;
use std::fs::*;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Since there is only 1 log file right now, its name is hardcoded.
const LOG_FILE_NAME: &str = "0.bin";

/// Write buffer size is 16 KiB. This allows for lower writing frequency.
/// (If I set it higher the compaction test will falsely pass)
const WRITE_BUFFER_SIZE: usize = 16 * 1024;

/// Result type of KvStore
pub type Result<T> = std::result::Result<T, Error>;

/// A KvStore stores key-value pairs in log structure on disk.
///
/// A KvStore is created by KvStore::Open. It keeps a log pointer map in memory to speed up commands.
///
/// # Examples
///
/// ```rust
/// use kvs::KvStore;
/// use tempfile::TempDir;
///
/// let tempdir = TempDir::new().unwrap();
/// let path = tempdir.path();
/// let mut kv = KvStore::open(path).unwrap();
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
    /// Writer in append mode for adding new log to disk
    append_writer: BufWriter<File>,
    /// Log pointer map
    log_pointer: HashMap<String, u64>,
}

impl Drop for KvStore {
    /// To make sure buffer is flushed on drop.
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

/// Get current reader position
fn position<R: BufRead + Seek>(mut reader: R) -> Result<u64> {
    Ok(reader.seek(SeekFrom::Current(0)).context(ErrorKind::Io)?)
}

/// Get file length in bytes
fn file_len(path: &PathBuf) -> Result<u64> {
    Ok(metadata(path).context(ErrorKind::Io)?.len())
}

impl KvStore {
    /// Set a key-value pair.
    ///
    /// If the KvStore did have this key present, the value is updated via a new set command appended.
    /// The new command is not necessarily writen to log file immediately due to buffer.
    ///
    /// # Errors
    ///
    /// - Io: Failed to open log file
    /// - Serde: Failed to serialize the set command
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let tempdir = TempDir::new().unwrap();
    /// let path = tempdir.path();
    /// let mut kv = KvStore::open(path).unwrap();
    ///
    /// kv.set("key1".to_owned(), "12".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("12".to_owned()));
    ///
    /// kv.set("key1".to_owned(), "11".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("11".to_owned()));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // record current offset
        let new_offset = file_len(&self.log_file_path)? + self.append_writer.buffer().len() as u64;

        // append log
        let kvlog = KvLog::new_set(key, value);
        kvlog.serialize_to_writer(&mut self.append_writer)?;

        // update log pointer map
        self.log_pointer.insert(kvlog.key(), new_offset);

        Ok(())
    }

    /// Returns the value corresponding to the key.
    ///
    /// The returned value is a copy of the value stored in `KvStore` if present.
    ///
    /// # Errors
    ///
    /// - Io: If log file failed to be read
    /// - Serde: If log deserialization failed when reading log file.
    /// - Corruption: If log file is different from log pointer map in memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let tempdir = TempDir::new().unwrap();
    /// let path = tempdir.path();
    /// let mut kv = KvStore::open(path).unwrap();
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
        match self.log_pointer.get(&key) {
            None => Ok(None),
            Some(&offset) => {
                let log_len = file_len(&self.log_file_path)?;

                let kvlog = if offset >= log_len {
                    // log is still in buffer
                    let buffer = self.append_writer.buffer();
                    let mut reader = Cursor::new(buffer);
                    reader
                        .seek(SeekFrom::Start(offset - log_len))
                        .context(ErrorKind::Io)?;
                    KvLog::deserialize_from_reader(reader)?
                } else {
                    // log is in file
                    let f = File::open(&self.log_file_path).context(ErrorKind::Io)?;
                    let mut reader = BufReader::new(f);
                    reader
                        .seek(SeekFrom::Start(offset))
                        .context(ErrorKind::Io)?;
                    KvLog::deserialize_from_reader(reader)?
                };

                match kvlog {
                    KvLog::Set(k, v) => {
                        // Optional check for key
                        if key != k {
                            return Err(Error::from(ErrorKind::Corruption));
                        }
                        Ok(Some(v))
                    }
                    _ => Err(Error::from(ErrorKind::Corruption)),
                }
            }
        }
    }

    /// Removes a key from the map if the key is present.
    ///
    /// If the KvStore did have this key present, the value is "removed" via a new remove command appended.
    /// The new command is not necessarily writen to log file immediately due to buffer.
    ///
    /// # Errors
    ///
    /// - KeyNotFound: If the key does not exist.
    /// - Serde: If log serialization failed.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let tempdir = TempDir::new().unwrap();
    /// let path = tempdir.path();
    /// let mut kv = KvStore::open(path).unwrap();
    ///
    /// kv.remove("key1".to_owned()).unwrap_err(); // missing key
    ///
    /// kv.set("key1".to_owned(), "12".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), Some("12".to_owned()));
    ///
    /// kv.remove("key1".to_owned()).unwrap();
    /// assert_eq!(kv.get("key1".to_owned()).unwrap(), None);
    ///
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.log_pointer.contains_key(&key) {
            // update log file
            let kvlog = KvLog::new_rm(key);
            kvlog.serialize_to_writer(&mut self.append_writer)?;

            // update log pointer map
            self.log_pointer.remove(&kvlog.key());

            Ok(())
        } else {
            Err(Error::from(ErrorKind::KeyNotFound))
        }
    }

    /// Opens a KvStore from given directory and setup the in-memory log pointer map.
    ///
    /// The directory will be created if not exist.
    ///
    /// # Errors
    ///
    /// - Io: If creation of directory failed or file failed to open.
    /// - Serde: If log deserialization failed when reading log file.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// use tempfile::TempDir;
    ///
    /// let tempdir = TempDir::new().unwrap();
    /// let path = tempdir.path();
    /// let mut kv = KvStore::open(path).unwrap();
    ///
    /// kv.set("key1".to_owned(), "42".to_owned()).unwrap();
    /// // Do other things ...
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let dir_path = path.as_path();
        if !dir_path.exists() {
            create_dir(dir_path).context(ErrorKind::Io)?;
        }

        // set up log file path
        let log_file_path = dir_path.join(LOG_FILE_NAME);

        // set up append_writer used by set and rm
        let append_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .context(ErrorKind::Io)?;
        let append_writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, append_file);

        // build log pointer map
        let mut reader = BufReader::new(File::open(&log_file_path).context(ErrorKind::Io)?);
        let mut log_pointer: HashMap<String, u64> = HashMap::new();
        while has_more(&mut reader)? {
            let pos = position(&mut reader)?;
            match KvLog::deserialize_from_reader(&mut reader)? {
                KvLog::Set(log_key, _) => log_pointer.insert(log_key, pos),
                KvLog::Rm(log_key) => log_pointer.remove(&log_key),
            };
        }

        Ok(KvStore {
            log_file_path,
            append_writer,
            log_pointer,
        })
    }
}
