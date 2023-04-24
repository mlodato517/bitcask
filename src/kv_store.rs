//! A key-value store. This has an API similar to the standard library's `HashMap`.

use std::collections::HashMap;

#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Create a new, empty key-value store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Associate the passed value with the passed key in the store. This can later be retrieved
    /// with `get`.
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Gets the value currently associated with the key, if there is one.
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Removes the associated value for the specified key.
    pub fn remove(&mut self, key: String) {
        let _ = self.map.remove(&key);
    }
}
