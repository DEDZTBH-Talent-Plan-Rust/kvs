#![deny(missing_docs)]
//! kvs is a single-threaded key-value pair storage using log structure.
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
use std::fs;
use std::fs::*;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Since there is only 1 log file right now, its name is hardcoded.
const LOG_FILE_NAME: &str = "0.bin";
/// Used by compaction
const TEMP_LOG_FILE_NAME: &str = "compact.tmp";

/// Write buffer size is 16 KiB. This allows for lower writing frequency.
/// (If I set it higher the compaction test will falsely pass)
const WRITE_BUFFER_SIZE: usize = 16 * 1024;

/// Compact file when there are enough redundant records.
const COMPACT_REDUNDANT_THRESHOLD: usize = 1024;

/// Result type of KvStore
pub type Result<T> = std::result::Result<T, Error>;

type LogPointerMap = HashMap<String, u64>;

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
    /// Reader that can be reused by get
    reader: BufReader<File>,
    /// Writer in append mode for adding new log to disk
    append_writer: BufWriter<File>,
    /// Log pointer map
    log_pointer: LogPointerMap,
    /// Redundant record number, used for compaction.
    redundant_count: usize,
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
fn position<R: Seek>(mut reader: R) -> Result<u64> {
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
        if let Some(_) = self.log_pointer.insert(kvlog.key(), new_offset) {
            self.increment_redundant();
        };

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
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.log_pointer.get(&key) {
            None => Ok(None),
            Some(&offset) => match self.get_kvlog_from_offset(offset)? {
                KvLog::Set(_k, v) => {
                    // Optional check for key match
                    // if key != _k {
                    //     return Err(Error::from(ErrorKind::Corruption));
                    // }
                    Ok(Some(v))
                }
                _ => Err(Error::from(ErrorKind::Corruption)),
            },
        }
    }

    /// Underlying implementation for get
    fn get_kvlog_from_offset(&mut self, offset: u64) -> Result<KvLog> {
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
            let reader = &mut self.reader;
            reader
                .seek(SeekFrom::Start(offset))
                .context(ErrorKind::Io)?;
            KvLog::deserialize_from_reader(reader)?
        };

        Ok(kvlog)
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
            if let Some(_) = self.log_pointer.remove(&kvlog.key()) {
                self.increment_redundant();
            };

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
        let mut log_pointer: LogPointerMap = HashMap::new();
        let mut redundant_count = 0;
        while has_more(&mut reader)? {
            let pos = position(&mut reader)?;
            let update_result = match KvLog::deserialize_from_reader(&mut reader)? {
                KvLog::Set(log_key, _) => log_pointer.insert(log_key, pos),
                KvLog::Rm(log_key) => log_pointer.remove(&log_key),
            };
            if let Some(_) = update_result {
                redundant_count += 1;
            }
        }

        Ok(KvStore {
            log_file_path,
            reader,
            append_writer,
            log_pointer,
            redundant_count,
        })
    }

    /// Increment redundant count and compact the log file if needed.
    /// If compaction failed, will print an error message without panicking.
    /// See `compact` for more information.
    fn increment_redundant(&mut self) {
        self.redundant_count += 1;
        if self.redundant_count >= COMPACT_REDUNDANT_THRESHOLD {
            match self.compact() {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Failed to compact: {:?}", e);
                }
            }
        }
    }

    /// Compact the log file. Only keep the latest set records for each key.
    /// If latest record for a key is rm, the key will not be present at all after compaction.
    /// Will preserve order of the latest records.
    ///
    /// It will create a new file and write the new compacted log in it.
    /// The new file is discarded if an error occurred and the original file is unmodified (aka "recovered").
    /// Should anything failed, the KvStore will not be modified.
    /// Otherwise, the old file is replaced with the new file and KvStore is updated.
    fn compact(&mut self) -> Result<()> {
        let mut new_log_file_path = self.log_file_path.clone();
        new_log_file_path.pop();
        new_log_file_path = new_log_file_path.join(TEMP_LOG_FILE_NAME);

        // set up append_writer used by set and rm
        if new_log_file_path.exists() {
            fs::remove_file(&new_log_file_path).context(ErrorKind::Io)?;
        }
        let new_append_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&new_log_file_path)
            .context(ErrorKind::Io)?;

        let mut new_append_writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, new_append_file);

        // Make sure the original log pointer map is not modified
        let mut new_log_pointer: LogPointerMap = self.log_pointer.clone();
        let mut log_pointers = new_log_pointer.iter_mut().collect::<Vec<_>>();
        // Sort by log pointer to ensure original order is preserved in log file. Remove this line if this is not necessary.
        log_pointers.sort_unstable_by_key(|x| *x.1);
        for (_key, val) in log_pointers {
            let kvlog = self.get_kvlog_from_offset(*val)?;

            // Optional check for kvlog
            // match kvlog {
            //     KvLog::Set(k, _) => {
            //         if k != _key {
            //             return Err(Error::from(ErrorKind::Corruption));
            //         }
            //     }
            //     _ => return Err(Error::from(ErrorKind::Corruption)),
            // }

            // Update log pointer map right away
            *val = file_len(&new_log_file_path)? + new_append_writer.buffer().len() as u64;
            kvlog.serialize_to_writer(&mut new_append_writer)?;
        }

        // create reader in advance so we can rollback if this fails
        let new_reader = BufReader::new(File::open(&new_log_file_path).context(ErrorKind::Io)?);

        // New file is ready, overwrite the old file. Rollback after this is impossible.
        rename(&new_log_file_path, &self.log_file_path).context(ErrorKind::Io)?;

        // Update in-memory components
        self.reader = new_reader;
        self.append_writer = new_append_writer;
        self.log_pointer = new_log_pointer;
        self.redundant_count = 0;

        Ok(())
    }
}
