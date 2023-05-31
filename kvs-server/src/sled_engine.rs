use std::borrow::Borrow;

use kvs::{Error, KvsEngine, Result};

pub struct SledDb(pub sled::Db);

impl Drop for SledDb {
    fn drop(&mut self) {
        // See https://docs.rs/sled/latest/sled/struct.Db.html#method.was_recovered
        if let Err(e) = self.0.flush() {
            tracing::warn!(?e, "Failed to flush sled");
        }
    }
}

impl KvsEngine for SledDb {
    fn set<V: AsRef<str>>(&mut self, key: String, value: V) -> Result<()> {
        let _ = self.0.insert(key, value.as_ref()).map_err(|e| {
            tracing::warn!(?e, "Failed to insert into sled");
            Error::msg("Failed to insert into sled")
        })?;

        // TODO This is still needed here despite the Drop impl. Maybe Drop isn't called when we
        // get a SIGTERM. Might want a custom signal handler.
        if let Err(e) = self.0.flush() {
            tracing::warn!(?e, "Failed to flush sled");
        }
        Ok(())
    }

    fn get<K: Borrow<str>>(&mut self, key: K) -> Result<Option<String>> {
        let maybe_result = sled::Tree::get(&self.0, key.borrow()).map_err(|e| {
            tracing::warn!(?e, "Failed to get from sled");
            Error::msg("Failed to read from sled")
        })?;
        match maybe_result {
            Some(ivec) => {
                let s = std::str::from_utf8(ivec.as_ref()).map_err(|e| {
                    tracing::warn!(?e, "Invalid utf8 from sled");
                    Error::msg("Invalid utf8 from sled")
                })?;
                Ok(Some(s.to_owned()))
            }
            None => Ok(None),
        }
    }

    fn remove<K: Borrow<str>>(&mut self, key: K) -> Result<()> {
        let result = sled::Tree::remove(&self.0, key.borrow());
        if let Err(e) = self.0.flush() {
            tracing::warn!(?e, "Failed to flush sled");
        }
        match result {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(Error::msg("Key not found")),
            Err(e) => {
                tracing::warn!(?e, "Failed to remove from sled");
                Err(Error::msg("Failed to remove from sled"))
            }
        }
    }
}
