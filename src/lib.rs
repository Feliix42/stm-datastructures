//! This library provides a small set of data types for use with the
//! [stm](https://crates.io/crates/stm) crate.

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
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

        let set_ro = self.contents[bucket_no].read_ref_atomic().downcast::<HashSet<T>>().unwrap();

        if set_ro.contains(&value) {
            // nothing to be inserted, no change to hashset made
            Ok(false)
        } else {
            // the element is indeed to be inserted -- write back
            self.contents[bucket_no].modify(trans, |mut hs| { hs.insert(value); hs })?;
            Ok(true)
        }
    }

    /// Empties the HashSet and returns all elements as `VecDequeue`.
    ///
    /// Must be executed as part of a transaction. After calling this function, `self` may be
    /// dropped, as it is empty.
    pub fn as_vec(&self, trans: &mut Transaction) -> StmResult<Vec<T>> {
        let mut result = Vec::new();

        for set in &self.contents {
            let mut inner_set = set.read(trans)?;
            result.append(&mut inner_set.drain().collect());
        }

        Ok(result)
    }
}

/// A transaction-ready hash map with a configurable number of buckets
#[derive(Clone)]
pub struct THashMap<K,V> {
    contents: Vec<TVar<HashMap<K,V>>>,
}

impl<K, V> THashMap<K, V> where
    K: Any + Clone + Eq + Hash + Send + Sync,
    V: Any + Clone + Send + Sync
{
    /// Creates a new transaction-ready HashMap with the given number of buckets.
    pub fn new(bucket_count: usize) -> Self {
        let mut hs = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            hs.push(TVar::new(HashMap::new()));
        }

        THashMap { contents: hs }
    }

    /// Shorthand for more efficient population of a HashMap with data
    pub fn from_hashmap(map: HashMap<K, V>, bucket_count: usize) -> Self {
        let estimated_size = map.len() / bucket_count;
        let mut hs: Vec<HashMap<K, V>> = vec![HashMap::with_capacity(estimated_size); bucket_count];

        for (k, v) in map.into_iter() {
            let mut hasher = DefaultHasher::new();
            k.hash(&mut hasher);
            let bucket_no: usize = hasher.finish() as usize % bucket_count;

            hs[bucket_no].insert(k, v);
        }

        THashMap { contents: hs.into_iter().map(TVar::new).collect() }
    }

    pub fn get_bucket(&self, item: &K) -> &TVar<HashMap<K, V>> {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let bucket_no: usize = hasher.finish() as usize % self.contents.len();

        &self.contents[bucket_no]
    }

    pub fn is_empty(&self, trans: &mut Transaction) -> StmResult<bool> {
        for bucket in &self.contents {
            let content = bucket.read(trans)?;
            if !content.is_empty() {
                return Ok(false)
            }
        }

        Ok(true)
    }

    pub fn get_contents(&self) -> HashMap<K,V> {
        self.contents.iter().map(TVar::read_atomic).flatten().collect()
    }
}

