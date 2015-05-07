use std::cmp;
use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::HashMap;

use {Crdt, ReplicaId};

#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

/// A grow-only counter.
///
/// `GCounter` monotonically increases across increment operations.
#[derive(Debug, Clone)]
pub struct GCounter {
    replica_id: ReplicaId,
    counts: HashMap<ReplicaId, u64>
}

/// An increment operation over `GCounter` CRDTs.
#[derive(Debug, Clone, Copy)]
pub struct GCounterOp {
    replica_id: ReplicaId,
    count: u64
}

impl GCounter {

    /// Create a new grow-only counter with the provided replica id and an
    /// initial count of 0.
    ///
    /// Replica IDs **must** be unique among replicas of a counter.
    ///
    /// ##### Example
    ///
    /// ```
    /// use crdt::counter::GCounter;
    ///
    /// let mut counter = GCounter::new(42);
    /// assert_eq!(0, counter.count());
    /// ```
    pub fn new<R>(replica_id: R) -> GCounter
    where R: Into<ReplicaId> {
        GCounter { replica_id: replica_id.into(), counts: HashMap::new() }
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
    pub fn count(&self) -> u64 {
        self.counts.values().map(|&x| x).fold(0, |a, b| a + b)
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
    /// replica1.increment(u64::MAX);     // OK
    /// replica2.increment(1);            // OK
    ///
    /// replica1.merge(replica2.clone()); // replica1 is in an undefined state
    /// replica2.merge(replica1.clone()); // replica2 is in an undefined state
    /// ```
    pub fn increment(&mut self, amount: u64) -> GCounterOp {
        let count = self.counts.entry(self.replica_id).or_insert(0);
        *count += amount;
        GCounterOp { replica_id: self.replica_id, count: *count }
    }

    /// Get the replica ID of this counter.
    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }
}

impl Crdt for GCounter {

    type Operation = GCounterOp;

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
    /// local.merge(remote);
    /// assert_eq!(25, local.count());
    /// ```
    fn merge(&mut self, other: GCounter) {
        for (&replica_id, &other_count) in other.counts.iter() {
            let count = self.counts.entry(replica_id).or_insert(0);
            *count = cmp::max(*count, other_count);
        }
    }

    /// Apply an increment operation to this counter.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// Applying an operation to a `GCounter` is idempotent.
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
    /// local.apply(op);
    /// assert_eq!(13, local.count());
    /// ```
    fn apply(&mut self, op: GCounterOp) {
        let GCounterOp { replica_id, count: other_count } = op;
        let count = self.counts.entry(replica_id).or_insert(0);
        *count = cmp::max(*count, other_count);
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
                match b.counts.get(replica_id) {
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

#[cfg(any(quickcheck, test))]
impl Arbitrary for GCounter {
    fn arbitrary<G>(g: &mut G) -> GCounter where G: Gen {
        use gen_replica_id;
        GCounter { replica_id: gen_replica_id(), counts: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=GCounter> + 'static> {
        let replica_id: ReplicaId = self.replica_id();
        Box::new(self.counts.shrink().map(move |counts| GCounter { replica_id: replica_id, counts: counts }))
    }
}

#[cfg(any(quickcheck, test))]
impl Arbitrary for GCounterOp {
    fn arbitrary<G>(g: &mut G) -> GCounterOp where G: Gen {
        GCounterOp { replica_id: Arbitrary::arbitrary(g), count: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=GCounterOp> + 'static> {
        let replica_id = self.replica_id;
        Box::new(self.count.shrink().map(move |count| GCounterOp { replica_id: replica_id, count: count }))
    }
}

#[cfg(test)]
mod test {

    use quickcheck::quickcheck;

    use {Crdt, ReplicaId, test};
    use counter::{GCounter, GCounterOp};

    type C = GCounter;
    type O = GCounterOp;

    #[test]
    fn check_apply_is_commutative() {
        quickcheck(test::apply_is_commutative::<C> as fn(C, Vec<O>) -> bool);
    }

    #[test]
    fn check_merge_is_commutative() {
        quickcheck(test::merge_is_commutative::<C> as fn(C, Vec<C>) -> bool);
    }

    #[test]
    fn check_ordering_lte() {
        quickcheck(test::ordering_lte::<C> as fn(C, C) -> bool);
    }

    #[test]
    fn check_ordering_equality() {
        quickcheck(test::ordering_equality::<C> as fn(C, C) -> bool);
    }

    #[quickcheck]
    fn check_local_increment(increments: Vec<u32>) -> bool {
        let mut counter = GCounter::new(ReplicaId(0));
        for &amount in increments.iter() {
            counter.increment(amount as u64);
        }
        increments.into_iter().fold(0, |a, b| a + b) as u64 == counter.count()
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: GCounter, b: GCounter) -> bool {
        a.merge(b.clone());
        a.increment(1);
        a > b && b < a
    }

    #[quickcheck]
    fn check_ordering_none(mut a: GCounter, mut b: GCounter) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a.increment(1);
        b.increment(1);
        a.partial_cmp(&b) == None && b.partial_cmp(&a) == None
    }
}
