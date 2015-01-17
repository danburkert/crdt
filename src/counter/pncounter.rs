//! Counter CRDTs.

use std::cmp;
use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::HashMap;
use std::iter::AdditiveIterator;
use std::num::SignedInt;

use Crdt;
use test::gen_replica_id;

use quickcheck::{Arbitrary, Gen, Shrinker};

/// A incrementable and decrementable counter.
#[derive(Show, Clone)]
pub struct PnCounter {
    replica_id: u64,
    counts: HashMap<u64, (u64, u64)>,
}

/// An increment or decrement operation over `PnCounter` CRDTs.
#[derive(Show, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PnCounterIncrement {
    replica_id: u64,
    amount: i64,
}

impl PnCounter {

    /// Create a new counter with the provided replica id and an initial count
    /// of 0.
    ///
    /// Replica IDs **must** be unique among replicas of a counter.
    ///
    /// ##### Example
    ///
    /// ```
    /// use crdt::counter::PnCounter;
    ///
    /// let mut counter = PnCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn new(replica_id: u64) -> PnCounter {
        PnCounter { replica_id: replica_id, counts: HashMap::new() }
    }

    /// Get the current count of the counter.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PnCounter;
    /// let counter = PnCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn count(&self) -> i64 {
        self.counts.values().map(|&(p, n)| p as i64 - n as i64).sum()
    }

    /// Increment the counter by `amount`. If `amount` is negative, then the
    /// counter will be decremented.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PnCounter;
    /// let mut counter = PnCounter::new(42);
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
    /// # use crdt::counter::PnCounter;
    /// # use crdt::Crdt;
    /// let mut replica1 = PnCounter::new(42);
    /// let mut replica2 = PnCounter::new(43);
    ///
    /// replica1.increment(i64::MAX);       // OK
    /// replica2.increment(1);              // OK
    ///
    /// replica2.merge(replica1.clone());   // replica2 is in an undefined state
    ///
    /// replica1.increment(i64::MIN);       // OK
    /// replica1.increment(-1);             // replica1 is in an undefined state
    /// ```
    pub fn increment(&mut self, amount: i64) -> PnCounterIncrement {
        let operation = PnCounterIncrement { replica_id: self.replica_id, amount: amount };
        self.apply(operation);
        operation
    }

    /// Get the replica ID of this counter.
    fn replica_id(&self) -> u64 {
        self.replica_id
    }
}

impl Crdt<PnCounterIncrement> for PnCounter {

    /// Merge a replica into this counter.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::counter::PnCounter;
    /// use crdt::Crdt;
    ///
    /// let mut local = PnCounter::new(42);
    /// let mut remote = PnCounter::new(43);
    ///
    /// local.increment(-12);
    /// remote.increment(13);
    ///
    /// local.merge(remote);
    /// assert_eq!(1, local.count());
    /// ```
    fn merge(&mut self, other: PnCounter) {
        for (&replica_id, &(other_p, other_n)) in other.counts.iter() {
            let count = match self.counts.get(&replica_id) {
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
    /// # use crdt::counter::PnCounter;
    /// # use crdt::Crdt;
    /// let mut local = PnCounter::new(42);
    /// let mut remote = PnCounter::new(43);
    ///
    /// let op = remote.increment(-12);
    ///
    /// local.apply(op);
    /// assert_eq!(-12, local.count());
    /// ```
    fn apply(&mut self, operation: PnCounterIncrement) {
        let (p_amount, n_amount) =
            if operation.amount > 0 {
                (operation.amount as u64, 0)
            } else {
                (0, operation.amount.abs() as u64)
            };

        let count = match self.counts.get_mut(&operation.replica_id) {
            Some(&mut (self_p, self_n)) => (self_p + p_amount, self_n + n_amount),
            None => (p_amount, n_amount)
        };

        self.counts.insert(operation.replica_id, count);
    }
}

impl PartialEq for PnCounter {
    fn eq(&self, other: &PnCounter) -> bool {
        self.counts == other.counts
    }
}

impl Eq for PnCounter {}

impl PartialOrd for PnCounter {
    fn partial_cmp(&self, other: &PnCounter) -> Option<Ordering> {

        /// Compares `a` to `b` based on replica counts.
        ///
        /// Precondition: `a.counts.len() <= b.counts.len()`
        fn a_gt_b(a: &PnCounter, b: &PnCounter) -> bool {
            for (&replica_id, &(a_p_count, a_n_count)) in a.counts.iter() {
                match b.counts.get(&replica_id) {
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

impl Arbitrary for PnCounter {

    fn arbitrary<G: Gen>(g: &mut G) -> PnCounter {
        PnCounter { replica_id: gen_replica_id(), counts: Arbitrary::arbitrary(g) }
    }

    fn shrink(&self) -> Box<Shrinker<PnCounter>+'static> {
        let replica_id = self.replica_id();
        let shrinks = self.counts.shrink().map(|counts| PnCounter { replica_id: replica_id, counts: counts }).collect::<Vec<_>>();
        Box::new(shrinks.into_iter()) as Box<Shrinker<PnCounter>+'static>
    }
}

impl PnCounterIncrement {
    fn replica_id(&self) -> u64 {
        self.replica_id
    }
}

impl Arbitrary for PnCounterIncrement {
    fn arbitrary<G: Gen>(g: &mut G) -> PnCounterIncrement {
        PnCounterIncrement { replica_id: Arbitrary::arbitrary(g), amount: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<PnCounterIncrement>+'static> {
        let replica_id = self.replica_id();
        let shrinks = self.amount.shrink().map(|amount| PnCounterIncrement { replica_id: replica_id, amount: amount }).collect::<Vec<_>>();
        Box::new(shrinks.into_iter()) as Box<Shrinker<PnCounterIncrement>+'static>
    }
}

#[cfg(test)]
mod test {

    use std::cmp::Ordering::Equal;
    use std::iter::AdditiveIterator;

    use Crdt;
    use super::{PnCounter, PnCounterIncrement};

    #[quickcheck]
    fn check_local_increment(increments: Vec<i32>) -> bool {
        let mut counter = PnCounter::new(0);
        for &amount in increments.iter() {
            counter.increment(amount as i64);
        }
        increments.into_iter().sum() as i64 == counter.count()
    }

    #[quickcheck]
    fn check_apply_is_commutative(increments: Vec<PnCounterIncrement>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<PnCounterIncrement> = increments.into_iter().take(5).collect();

        let mut reference = PnCounter::new(0);
        for increment in truncated.clone().into_iter() {
            reference.apply(increment);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(PnCounter::new(0), |mut counter, &op| {
                         counter.apply(op);
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn check_merge_is_commutative(counters: Vec<PnCounter>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<PnCounter> = counters.into_iter().take(5).collect();

        let mut reference = PnCounter::new(0);
        for counter in truncated.clone().into_iter() {
            reference.merge(counter);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(PnCounter::new(0), |mut counter, other| {
                         counter.merge(other.clone());
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn check_ordering_lte(mut a: PnCounter, b: PnCounter) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: PnCounter, b: PnCounter) -> bool {
        a.merge(b.clone());
        a.increment(-1);
        a > b && b < a
    }

    #[quickcheck]
    fn check_ordering_equality(mut a: PnCounter, mut b: PnCounter) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }

    #[quickcheck]
    fn check_ordering_none(mut a: PnCounter, mut b: PnCounter) -> bool {
        a.increment(1);
        b.increment(-1);
        a.partial_cmp(&b) == None && b.partial_cmp(&a) == None
    }
}
