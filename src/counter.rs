//! Counter CRDTs and operations.

extern crate quickcheck;


use std::cmp;
use std::collections::TrieMap;
use std::iter::AdditiveIterator;

use quickcheck::{Arbitrary, Gen, Shrinker};

use Crdt;
use CrdtOperation;
use test::gen_replica_id;

/// A grow-only counter.
///
/// `GCounter` monotonically increases across increment operations.
#[deriving(Show, Clone)]
pub struct GCounter {
    replica_id: uint,
    counts: TrieMap<u64>
}

/// An increment operation over `GCounter` CRDTs.
#[deriving(Show, Clone)]
pub struct GCounterIncrement {
    replica_id: uint,
    amount: u64
}

impl GCounter {

    /// Create a new grow-only counter with the provided replica id and an
    /// initial count of 0.
    ///
    /// ##### Example
    ///
    /// ```
    /// use crdt::counter::GCounter;
    ///
    /// let mut counter = GCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn new(replica_id: uint) -> GCounter {
        GCounter { replica_id: replica_id, counts: TrieMap::new() }
    }

    /// Get the current count of the counter.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::GCounter;
    /// let counter = GCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn count(& self) -> u64 {
        self.counts.values().map(|&x| x).sum()
    }

    /// Increment the counter by `amount`.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::GCounter;
    /// let mut counter = GCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// counter.increment(13);
    /// assert_eq!(13, counter.count());
    /// ```
    ///
    /// ##### Overflow
    ///
    /// Incrementing the count by more than `u64::MAX` is undefined behavior.
    /// The increment limit is globally shared across all replicas, and is not
    /// checked during local operations.
    ///
    /// ```
    /// # use std::u64;
    /// # use crdt::counter::GCounter;
    /// # use crdt::Crdt;
    /// let mut replica1 = GCounter::new(42);
    /// let mut replica2 = GCounter::new(43);
    ///
    /// replica1.increment(u64::MAX);   // OK
    /// replica2.increment(1);          // OK
    ///
    /// replica1.merge(&replica2);      // replica1 is in an undefined state
    /// replica2.merge(&replica1);      // replica2 is in an undefined state
    /// ```
    pub fn increment(&mut self, amount: u64) -> GCounterIncrement {
        let operation = GCounterIncrement { replica_id: self.replica_id, amount: amount };
        self.apply(&operation);
        operation
    }
}

impl Crdt<GCounterIncrement> for GCounter {

    /// Merge a replica into this counter.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::GCounter;
    /// use crdt::Crdt;
    ///
    /// let mut local = GCounter::new(42);
    /// let mut remote = GCounter::new(43);
    ///
    /// local.increment(12);
    /// remote.increment(13);
    ///
    /// local.merge(&remote);
    /// assert_eq!(25, local.count());
    /// ```
    fn merge(&mut self, other: &GCounter) {
        for (replica_id, other_count) in other.counts.iter() {
            let count = match self.counts.find_mut(&replica_id) {
                Some(self_count) => cmp::max(*self_count, *other_count),
                None => *other_count
            };
            self.counts.insert(replica_id, count);
        }
    }

    /// Apply an increment operation to this counter.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::GCounter;
    /// # use crdt::Crdt;
    /// let mut local = GCounter::new(42);
    /// let mut remote = GCounter::new(43);
    ///
    /// let op = remote.increment(13);
    ///
    /// local.apply(&op);
    /// assert_eq!(13, local.count());
    /// ```
    fn apply(&mut self, operation: &GCounterIncrement) {
        let count = match self.counts.find_mut(&operation.replica_id) {
            Some(self_count) => *self_count + operation.amount,
            None => operation.amount
        };
        self.counts.insert(operation.replica_id, count);
    }

    fn replica_id(&self) -> uint {
        self.replica_id
    }
}

impl PartialEq for GCounter {
    fn eq(&self, other: &GCounter) -> bool {
        self.counts == other.counts
    }
}

impl Eq for GCounter {}

impl PartialOrd for GCounter {
    fn partial_cmp(&self, other: &GCounter) -> Option<Ordering> {

        /// Compares `a` to `b` based on replica counts.
        ///
        /// Precondition: `a.counts.len() <= b.counts.len()`
        fn a_gt_b(a: &GCounter, b: &GCounter) -> bool {
            for (replica_id, a_count) in a.counts.iter() {
                match b.counts.find(&replica_id) {
                    Some(b_count) if a_count > b_count => return true,
                    None => return true,
                    _ => ()
                }
            }
            false
        }

        let (self_gt_other, other_gt_self) =
            match self.counts.len().cmp(&other.counts.len()) {
                Less    => (a_gt_b(self, other), true),
                Greater => (true, a_gt_b(other, self)),
                Equal   => (a_gt_b(self, other), a_gt_b(other, self))
            };

        match (self_gt_other, other_gt_self) {
            (true, true)   => None,
            (true, false)  => Some(Greater),
            (false, true)  => Some(Less),
            (false, false) => Some(Equal)
        }
    }
}

impl Arbitrary for GCounter {
    fn arbitrary<G: Gen>(g: &mut G) -> GCounter {
        GCounter { replica_id: gen_replica_id(), counts: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<GCounter>> {
        let replica_id = self.replica_id();
        box self.counts.shrink().map(|counts| GCounter { replica_id: replica_id, counts: counts })
            as Box<Shrinker<GCounter>>
    }
}

impl CrdtOperation for GCounterIncrement {
    fn replica_id(&self) -> uint {
        self.replica_id
    }
}

impl Arbitrary for GCounterIncrement {
    fn arbitrary<G: Gen>(g: &mut G) -> GCounterIncrement {
        GCounterIncrement { replica_id: Arbitrary::arbitrary(g), amount: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<GCounterIncrement>> {
        let replica_id = self.replica_id();
        box self.amount.shrink().map(|amount| GCounterIncrement { replica_id: replica_id, amount: amount })
            as Box<Shrinker<GCounterIncrement>>
    }
}

/// A incrementable and decrementable counter.
#[deriving(Show, Clone)]
pub struct PNCounter {
    replica_id: uint,
    counts: TrieMap<(u64, u64)>
}

/// An increment or decrement operation over `PNCounter` CRDTs.
#[deriving(Show, Clone)]
pub struct PNCounterIncrement {
    replica_id: uint,
    amount: i64
}

impl PNCounter {

    /// Create a new counter with the provided replica id and an initial count
    /// of 0.
    ///
    /// ##### Example
    ///
    /// ```
    /// use crdt::counter::PNCounter;
    ///
    /// let mut counter = PNCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn new(replica_id: uint) -> PNCounter {
        PNCounter { replica_id: replica_id, counts: TrieMap::new() }
    }

    /// Get the current count of the counter.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PNCounter;
    /// let counter = PNCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn count(& self) -> i64 {
        self.counts.values().map(|&(p, n)| p as i64 - n as i64).sum()
    }

    /// Increment the counter by `amount`. If `amount` is negative, then the
    /// counter will be decremented.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PNCounter;
    /// let mut counter = PNCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// counter.increment(-13);
    /// assert_eq!(-13, counter.count());
    /// ```
    ///
    /// ##### Overflow
    ///
    /// Incrementing the count by more than `i64::MAX` or decrementing by more
    /// than `i64::MIN` is undefined behavior. Decrements do not 'cancel out'
    /// increments for the purposes of these limits. The increment and decrement
    /// limit is globally shared across all replicas, and is not checked during
    /// local operations.
    ///
    /// ```
    /// # use std::i64;
    /// # use crdt::counter::PNCounter;
    /// # use crdt::Crdt;
    /// let mut replica1 = PNCounter::new(42);
    /// let mut replica2 = PNCounter::new(43);
    ///
    /// replica1.increment(i64::MAX);   // OK
    /// replica2.increment(1);          // OK
    ///
    /// replica2.merge(&replica1);      // replica2 is in an undefined state
    ///
    /// replica1.increment(i64::MIN);   // OK
    /// replica1.increment(-1);         // replica1 is in an undefined state
    /// ```
    pub fn increment(&mut self, amount: i64) -> PNCounterIncrement {
        let operation = PNCounterIncrement { replica_id: self.replica_id, amount: amount };
        self.apply(&operation);
        operation
    }
}

impl Crdt<PNCounterIncrement> for PNCounter {

    /// Merge a replica into this counter.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PNCounter;
    /// use crdt::Crdt;
    ///
    /// let mut local = PNCounter::new(42);
    /// let mut remote = PNCounter::new(43);
    ///
    /// local.increment(-12);
    /// remote.increment(13);
    ///
    /// local.merge(&remote);
    /// assert_eq!(1, local.count());
    /// ```
    fn merge(&mut self, other: &PNCounter) {
        for (replica_id, &(other_p, other_n)) in other.counts.iter() {
            let count = match self.counts.find(&replica_id) {
                Some(&(self_p, self_n)) => (cmp::max(self_p, other_p), cmp::max(self_n, other_n)),
                None => (other_p, other_n)
            };
            self.counts.insert(replica_id, count);
        }
    }

    /// Apply an increment operation to this counter.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PNCounter;
    /// # use crdt::Crdt;
    /// let mut local = PNCounter::new(42);
    /// let mut remote = PNCounter::new(43);
    ///
    /// let op = remote.increment(-12);
    ///
    /// local.apply(&op);
    /// assert_eq!(-12, local.count());
    /// ```
    fn apply(&mut self, operation: &PNCounterIncrement) {
        let (p_amount, n_amount) =
            if operation.amount > 0 {
                (operation.amount as u64, 0)
            } else {
                (0, operation.amount.abs() as u64)
            };

        let count = match self.counts.find_mut(&operation.replica_id) {
            Some(&(self_p, self_n)) => (self_p + p_amount, self_n + n_amount),
            None => (p_amount, n_amount)
        };

        self.counts.insert(operation.replica_id, count);
    }

    fn replica_id(&self) -> uint {
        self.replica_id
    }
}

impl PartialEq for PNCounter {
    fn eq(&self, other: &PNCounter) -> bool {
        self.counts == other.counts
    }
}

impl Eq for PNCounter {}

impl PartialOrd for PNCounter {
    fn partial_cmp(&self, other: &PNCounter) -> Option<Ordering> {

        /// Compares `a` to `b` based on replica counts.
        ///
        /// Precondition: `a.counts.len() <= b.counts.len()`
        fn a_gt_b(a: &PNCounter, b: &PNCounter) -> bool {
            for (replica_id, &(a_p_count, a_n_count)) in a.counts.iter() {
                match b.counts.find(&replica_id) {
                    Some(&(b_p_count, b_n_count))
                        if a_p_count > b_p_count || a_n_count > b_n_count => return true,
                    None => return true,
                    _ => ()
                }
            }
            false
        }

        let (self_gt_other, other_gt_self) =
            match self.counts.len().cmp(&other.counts.len()) {
                Less    => (a_gt_b(self, other), true),
                Greater => (true, a_gt_b(other, self)),
                Equal   => (a_gt_b(self, other), a_gt_b(other, self))
            };

        match (self_gt_other, other_gt_self) {
            (true, true)   => None,
            (true, false)  => Some(Greater),
            (false, true)  => Some(Less),
            (false, false) => Some(Equal)
        }
    }
}

impl Arbitrary for PNCounter {

    fn arbitrary<G: Gen>(g: &mut G) -> PNCounter {
        PNCounter { replica_id: gen_replica_id(), counts: Arbitrary::arbitrary(g) }
    }

    fn shrink(&self) -> Box<Shrinker<PNCounter>> {
        let replica_id = self.replica_id();

        box self.counts.shrink().map(|counts| PNCounter { replica_id: replica_id, counts: counts })
            as Box<Shrinker<PNCounter>>
    }
}

impl CrdtOperation for PNCounterIncrement {
    fn replica_id(&self) -> uint {
        self.replica_id
    }
}

impl Arbitrary for PNCounterIncrement {
    fn arbitrary<G: Gen>(g: &mut G) -> PNCounterIncrement {
        PNCounterIncrement { replica_id: Arbitrary::arbitrary(g), amount: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<PNCounterIncrement>> {
        let replica_id = self.replica_id();
        box self.amount.shrink().map(|amount| PNCounterIncrement { replica_id: replica_id, amount: amount })
            as Box<Shrinker<PNCounterIncrement>>
    }
}

#[cfg(test)]
mod test {

    #[phase(plugin)]
    extern crate quickcheck_macros;

    use std::iter::AdditiveIterator;

    use Crdt;
    use counter::{GCounter, GCounterIncrement, PNCounter, PNCounterIncrement};

    #[quickcheck]
    fn gcounter_local_increment(increments: Vec<u32>) -> bool {
        let mut counter = GCounter::new(0);
        for &amount in increments.iter() {
            counter.increment(amount as u64);
        }
        increments.move_iter().sum() as u64 == counter.count()
    }

    #[quickcheck]
    fn gcounter_apply_is_commutative(increments: Vec<GCounterIncrement>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<GCounterIncrement> = increments.move_iter().take(5).collect();

        let mut reference = GCounter::new(0);
        for increment in truncated.iter() {
            reference.apply(increment);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(GCounter::new(0), |mut counter, op| {
                         counter.apply(op);
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn gcounter_merge_is_commutative(counters: Vec<GCounter>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<GCounter> = counters.move_iter().take(5).collect();

        let mut reference = GCounter::new(0);
        for counter in truncated.iter() {
            reference.merge(counter);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(GCounter::new(0), |mut counter, other| {
                         counter.merge(other);
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn gcounter_ordering_lte(mut a: GCounter, b: GCounter) -> bool {
        a.merge(&b);
        a >= b && b <= a
    }

    #[quickcheck]
    fn gcounter_ordering_lt(mut a: GCounter, b: GCounter) -> bool {
        a.merge(&b);
        a.increment(1);
        a > b && b < a
    }

    #[quickcheck]
    fn gcounter_ordering_equality(mut a: GCounter, mut b: GCounter) -> bool {
        a.merge(&b);
        b.merge(&a);
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }

    #[quickcheck]
    fn gcounter_ordering_none(mut a: GCounter, mut b: GCounter) -> bool {
        a.increment(1);
        b.increment(1);
        a.partial_cmp(&b) == None && b.partial_cmp(&a) == None
    }

    #[quickcheck]
    fn pncounter_local_increment(increments: Vec<i32>) -> bool {
        let mut counter = PNCounter::new(0);
        for &amount in increments.iter() {
            counter.increment(amount as i64);
        }
        increments.move_iter().sum() as i64 == counter.count()
    }

    #[quickcheck]
    fn pncounter_apply_is_commutative(increments: Vec<PNCounterIncrement>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<PNCounterIncrement> = increments.move_iter().take(5).collect();

        let mut reference = PNCounter::new(0);
        for increment in truncated.iter() {
            reference.apply(increment);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(PNCounter::new(0), |mut counter, op| {
                         counter.apply(op);
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn pncounter_merge_is_commutative(counters: Vec<PNCounter>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<PNCounter> = counters.move_iter().take(5).collect();

        let mut reference = PNCounter::new(0);
        for counter in truncated.iter() {
            reference.merge(counter);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(PNCounter::new(0), |mut counter, other| {
                         counter.merge(other);
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn pncounter_ordering_lte(mut a: PNCounter, b: PNCounter) -> bool {
        a.merge(&b);
        a >= b && b <= a
    }

    #[quickcheck]
    fn pncounter_ordering_lt(mut a: PNCounter, b: PNCounter) -> bool {
        a.merge(&b);
        a.increment(-1);
        a > b && b < a
    }

    #[quickcheck]
    fn pncounter_ordering_equality(mut a: PNCounter, mut b: PNCounter) -> bool {
        a.merge(&b);
        b.merge(&a);
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }

    #[quickcheck]
    fn pncounter_ordering_none(mut a: PNCounter, mut b: PNCounter) -> bool {
        a.increment(1);
        b.increment(-1);
        a.partial_cmp(&b) == None && b.partial_cmp(&a) == None
    }
}
