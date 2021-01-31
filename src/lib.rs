#![deny(missing_docs)]
//! kvs is a key-value pair storage in memory.
//!
//! # Examples
//!
//! ```rust
//! use kvs::KvStore;
//!
//! let mut kv = KvStore::new();
//! assert_eq!(kv.get("key1".to_owned()), None);
//!
//! kv.set("key1".to_owned(), "42".to_owned());
//! kv.set("key2".to_owned(), "43".to_owned());
//! assert_eq!(kv.get("key1".to_owned()), Some("42".to_owned()));
//! assert_eq!(kv.get("key2".to_owned()), Some("43".to_owned()));
//!
//! kv.remove("key1".to_owned());
//! kv.remove("key3".to_owned());
//! assert_eq!(kv.get("key1".to_owned()), None);
//! ```


use std::collections::HashMap;

/// A KvStore stores key-value pairs in memory.
///
/// KvStore uses a `HashMap<String, String>` to store data.
///
/// # Examples
///
/// ```rust
/// use kvs::KvStore;
///
/// let mut kv = KvStore::new();
///
/// kv.set("key1".to_owned(), "42".to_owned());
/// assert_eq!(kv.get("key1".to_owned()), Some("42".to_owned()));
///
/// kv.remove("key1".to_owned());
/// assert_eq!(kv.get("key1".to_owned()), None);
/// ```
pub struct KvStore {
    store: HashMap<String, String>
}

impl KvStore {
    /// Creates an empty `KvStore`.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut kv1 = KvStore::new();
    /// let mut kv2 = KvStore::new();
    ///
    /// kv2.set("152mm".to_owned(), "12".to_owned());
    /// assert_eq!(kv1.get("152mm".to_owned()), None);
    /// assert_eq!(kv2.get("152mm".to_owned()), Some("12".to_owned()));
    /// ```
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new()
        }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the `KvStore` did have this key present, the value is updated.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut kv = KvStore::new();
    ///
    /// kv.set("key1".to_owned(), "12".to_owned());
    /// assert_eq!(kv.get("key1".to_owned()), Some("12".to_owned()));
    ///
    /// kv.set("key1".to_owned(), "11".to_owned());
    /// assert_eq!(kv.get("key1".to_owned()), Some("11".to_owned()));
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Returns the value corresponding to the key.
    ///
    /// The returned value is a copy of the value stored in `KvStore` if present.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut kv = KvStore::new();
    /// assert_eq!(kv.get("key1".to_owned()), None);
    ///
    /// kv.set("key1".to_owned(), "12".to_owned());
    /// let returned_opt = kv.get("key1".to_owned());
    /// assert_eq!(returned_opt, Some("12".to_owned()));
    ///
    /// kv.set("key1".to_owned(), "11".to_owned());
    /// assert_eq!(kv.get("key1".to_owned()), Some("11".to_owned()));
    /// assert_eq!(returned_opt, Some("12".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    /// Removes a key from the map if the key is present.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut kv = KvStore::new();
    ///
    /// kv.remove("key1".to_owned()); // nothing will change
    ///
    /// kv.set("key1".to_owned(), "12".to_owned());
    /// assert_eq!(kv.get("key1".to_owned()), Some("12".to_owned()));
    ///
    /// kv.remove("key1".to_owned());
    /// assert_eq!(kv.get("key1".to_owned()), None);
    ///
    /// ```
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}