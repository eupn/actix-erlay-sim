//! Defines set that can be reconciled.

use minisketch_rs;
use minisketch_rs::Minisketch;
use std::collections::HashMap;
use std::hash::Hash;

/// Types that can produce short ID (short hash) can implement this trait.
pub trait ShortId<I> {
    fn short_id(&self) -> I;
}

/// A set that supports reconciliation by using short IDs (`I`) of its elements (`V`)
#[derive(Debug)]
pub struct RecSet<I: Hash + Eq + Copy + From<u64> + Into<u64>, V: ShortId<I> + Clone> {
    capacity: usize,
    seed: Option<u64>,
    sketch: Minisketch,
    map: HashMap<I, V>,
}

impl<I: Hash + Eq + Copy + From<u64> + Into<u64>, V: ShortId<I> + Clone> RecSet<I, V> {
    /// Creates new set with given `capacity`.
    pub fn new(capacity: usize) -> Self {
        let _bits = std::mem::size_of::<I>() * 8;
        let sketch = Self::create_minisketch(capacity, None);

        RecSet {
            seed: None,
            capacity,
            sketch,
            map: HashMap::with_capacity(capacity),
        }
    }

    /// Creates new set with given `capacity` and `seed` for underlying Minisketch math.
    pub fn with_seed(capacity: usize, seed: u64) -> Self {
        let sketch = Self::create_minisketch(capacity, Some(seed));

        RecSet {
            seed: None,
            capacity,
            sketch,
            map: HashMap::with_capacity(capacity),
        }
    }

    /// Adds element to the sketch.
    /// Element will be added only if it's not already in the set.
    pub fn insert(&mut self, v: V) {
        let id = v.short_id();
        if !self.map.contains_key(&id) {
            self.map.insert(id, v);
            self.sketch.add(id.into());
        }
    }

    fn create_minisketch(capacity: usize, seed: Option<u64>) -> Minisketch {
        let bits = std::mem::size_of::<I>() * 8;
        let mut minisketch = Minisketch::try_new(bits as u32, 0, capacity).unwrap();

        if let Some(seed) = seed {
            minisketch.set_seed(seed);
        }

        minisketch
    }

    /// Produces list of IDs that are missing in the set given as its `sketch`.
    pub fn reconcile_with(&mut self, sketch: &[u8]) -> Result<Vec<I>, ()> {
        let mut minisketch = Self::create_minisketch(self.capacity, self.seed);
        minisketch.deserialize(sketch);

        let sketch_clone = self.sketch.clone();
        minisketch.merge(&sketch_clone).expect("Minisketch merge");

        let mut diffs = vec![0u64; self.capacity];
        minisketch.decode(&mut diffs).expect("Minisketch decode");

        let diff_ids = diffs
            .iter()
            .map(|id| Into::<I>::into(*id))
            .collect::<Vec<_>>();

        let num_diffs = diff_ids
            .clone()
            .iter()
            .enumerate()
            .filter(|(_, id)| self.map.contains_key(*id))
            .take_while(|(_, val)| **val != 0.into())
            .count();

        Ok(diff_ids.into_iter().take(num_diffs).collect())
    }

    /// Produces sketch for this set.
    /// It is used in set reconciliation to find out what elements are missing in this set.
    pub fn sketch(&mut self) -> Vec<u8> {
        let mut buf = vec![0u8; self.sketch.serialized_size()];
        self.sketch
            .serialize(&mut buf)
            .expect("Minisketch serialize");

        buf
    }

    /// Looks up for an element with given `id` in this set.
    pub fn get(&self, id: &I) -> Option<V> {
        self.map.get(id).cloned()
    }
}

#[cfg(test)]
mod test {
    use super::{RecSet, ShortId};
    use siphasher::sip::SipHasher;
    use std::hash::Hasher;

    #[derive(Debug, Copy, Clone, PartialEq)]
    pub struct Tx(pub [u8; 32]);

    impl ShortId<u64> for Tx {
        fn short_id(&self) -> u64 {
            let mut hasher = SipHasher::new_with_keys(0xDEu64, 0xADu64);
            hasher.write(&self.0);
            hasher.finish()
        }
    }

    #[test]
    pub fn test_reconciliation() {
        let txs_alice = vec![Tx([1u8; 32]), Tx([2u8; 32]), Tx([3u8; 32]), Tx([4u8; 32])];

        let txs_bob = vec![Tx([1u8; 32]), Tx([2u8; 32])];

        let mut rec_set_alice = RecSet::<u64, Tx>::with_seed(16, 42u64);
        for tx in txs_alice.iter() {
            rec_set_alice.insert(tx.clone());
        }

        let mut rec_set_bob = RecSet::<u64, Tx>::with_seed(16, 42u64);
        for tx in txs_bob {
            rec_set_bob.insert(tx);
        }

        let bob_sketch = rec_set_bob.sketch();
        let missing = rec_set_alice
            .reconcile_with(&bob_sketch)
            .expect("Reconcile with Alice");

        assert_eq!(missing.len(), 2);

        for id in missing {
            assert!(rec_set_alice.get(&id).is_some());
        }
    }
}
