use std::borrow::Borrow;

use crate::Result;

pub trait KvsEngine {
    fn set<V: AsRef<str>>(&mut self, key: String, value: V) -> Result<()>;
    // TODO Why does this take &mut self?
    fn get<K: Borrow<str>>(&mut self, key: K) -> Result<Option<String>>;
    fn remove<K: Borrow<str>>(&mut self, key: K) -> Result<()>;
}
