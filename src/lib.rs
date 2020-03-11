//! This library provides a small set of data types for use with the
//! [stm](https://crates.io/crates/stm) crate.

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use stm::{StmResult, TVar, Transaction};

/// A transaction-ready hash set with a configurable but fixed number of buckets.
#[derive(Clone)]
pub struct THashSet<T> {
    contents: Vec<TVar<HashSet<T>>>,
}

impl<T> THashSet<T>
where
    T: Any + Clone + Eq + Hash + Send + Sync,
{
    /// Creates a new transaction-ready HashSet with the given number of buckets.
    pub fn new(bucket_count: usize) -> Self {
        let mut hs = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            hs.push(TVar::new(HashSet::new()));
        }

        THashSet { contents: hs }
    }

    /// Adds a value to the set.
    ///
    /// If the set did not have this value present, `true` is returned. If the value has been
    /// present before, `false` is returned.
    ///
    /// This function must be called inside a `atomically` block.
    pub fn insert(&self, trans: &mut Transaction, value: T) -> StmResult<bool> {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let bucket_no: usize = hasher.finish() as usize % self.contents.len();
        let mut set = self.contents[bucket_no].read(trans)?;

        if set.insert(value) {
            // the element was indeed inserted -- write back
            self.contents[bucket_no].write(trans, set)?;
            Ok(true)
        } else {
            // nothing to be inserted, no change to hashset made
            Ok(false)
        }
    }
}
