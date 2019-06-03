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

    pub fn reconcile(
        sketch_a: &[u8],
        sketch_b: &[u8],
        capacity: usize,
        seed: Option<u64>,
    ) -> Result<Vec<I>, ()> {
        let mut a = Self::create_minisketch(capacity, seed);
        a.deserialize(sketch_a);

        let mut b = Self::create_minisketch(capacity, seed);
        b.deserialize(sketch_b);

        a.merge(&b).expect("Minisketch merge");

        let mut diffs = vec![0u64; capacity];
        let num_diffs = a.decode(&mut diffs).map_err(|_| ())?;

        let diff_ids = diffs
            .iter()
            .map(|id| Into::<I>::into(*id))
            .collect::<Vec<_>>();

        Ok(diff_ids.into_iter().take(num_diffs).collect())
    }

    /// Produces list of IDs that are missing in the set given as its `sketch`.
    pub fn reconcile_with(&mut self, sketch_b: &[u8]) -> Result<Vec<I>, ()> {
        Self::reconcile(&self.sketch(), sketch_b, self.capacity, self.seed)
    }

    /// Produces sketch for this set.
    /// It is used in set reconciliation to find out what elements are missing in this set.
    pub fn sketch(&self) -> Vec<u8> {
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

    #[test]
    pub fn test_bisect_reconciliation() {
        let d = 16; // You can change it to 24 to not perform bisect and compare results

        // There is exactly 24 differences, but since d = 16, simple set reconciliation will fail
        let a = 0..32;
        let b = 0..8;

        // Take only even elements of a set, so they're uniform,
        // to increase chance of bisect success
        let b_half = b
            .clone()
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i % 2 == 0)
            .map(|(_, n)| n)
            .collect::<Vec<_>>();
        let a_half = a
            .clone()
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i % 2 == 0)
            .map(|(_, n)| n)
            .collect::<Vec<_>>();

        // Creates a set from a range of elements
        pub fn set_from_range(
            range: impl IntoIterator<Item = u8>,
            capacity: usize,
        ) -> RecSet<u64, Tx> {
            let txs = range.into_iter().map(|b| Tx([b; 32]));

            let mut set = RecSet::<u64, Tx>::new(capacity);
            for tx in txs {
                set.insert(tx);
            }

            set
        }

        // Extracts remainder sketch from a difference of two sketches
        pub fn sub_sketches(s1: &[u8], s2: &[u8], d: usize, seed: Option<u64>) -> Vec<u8> {
            let mut a = minisketch_rs::Minisketch::try_new(64, 0, d).unwrap();
            if let Some(seed) = seed {
                a.set_seed(seed);
            }
            a.deserialize(s1);

            let mut b = minisketch_rs::Minisketch::try_new(64, 0, d).unwrap();
            if let Some(seed) = seed {
                b.set_seed(seed);
            }
            b.deserialize(s2);

            a.merge(&b).expect("Sketch sub merge");

            let mut elements = vec![0u64; d];
            let res = a.decode(&mut elements);

            let mut sketch = vec![0u8; a.serialized_size()];
            a.serialize(&mut sketch).expect("Serialize sketch sub");

            sketch
        }

        // Try regular reconciliation

        let mut alice_set_full = set_from_range(a, d);
        let a = alice_set_full.sketch();

        let mut bob_set_full = set_from_range(b, d);
        let b = bob_set_full.sketch();

        let first_try = RecSet::<u64, Tx>::reconcile(&a, &b, d, None);
        if let Err(()) = first_try {
            println!("Set overfull, trying bisect...");

            // Try bisection:
            //
            // res_1 = reconcile(a/2, b/2)
            // res_2 = reconcile(a - a/2, b - b/2)
            //
            // differences = res_1 U res_2
            //
            // b/2 is known to Alice since Bob sent his b/2 sketch to her before bisect

            let mut bob_set_half = set_from_range(b_half, d);
            let b_2 = bob_set_half.sketch();

            let mut alice_set_half = set_from_range(a_half, d);
            let a_2 = alice_set_half.sketch();

            let a_minus_a_2 = sub_sketches(&a, &a_2, d, None);
            let b_minus_b_2 = sub_sketches(&b, &b_2, d, None);

            let res_1 = RecSet::<u64, Tx>::reconcile(&a_2, &b_2, d, None);
            let res_2 = RecSet::<u64, Tx>::reconcile(&a_minus_a_2, &b_minus_b_2, d, None);

            if res_1.is_err() || res_2.is_err() {
                println!("Failed: {:?}, {:?}", res_1, res_2);
            } else {
                let diffs1 = res_1.unwrap();
                let diffs2 = res_2.unwrap();

                let mut diffs = diffs1
                    .into_iter()
                    .chain(diffs2.into_iter())
                    .collect::<Vec<_>>();
                diffs.sort();
                println!("Success: {} diffs {:?}", diffs.len(), diffs);
            }
        } else {
            let mut diffs = first_try.ok().unwrap();
            diffs.sort();
            println!("Success: {} diffs: {:?}", diffs.len(), diffs);
        }
    }
}
