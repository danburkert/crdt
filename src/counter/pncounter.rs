use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::HashMap;

use {Crdt, ReplicaId};
use pn::Pn;

#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

/// A incrementable and decrementable counter.
#[derive(Clone, Debug, Eq)]
pub struct PnCounter {
    replica_id: ReplicaId,
    counts: HashMap<ReplicaId, Pn>,
}

/// An increment operation on a `PnCounter` CRDT.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PnCounterOp {
    replica_id: ReplicaId,
    pn: Pn,
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
    pub fn new<R>(replica_id: R) -> PnCounter
    where R: Into<ReplicaId> {
        PnCounter { replica_id: replica_id.into(), counts: HashMap::new() }
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
        self.counts.values().fold(0, |a, b| a + b.count())
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
    pub fn increment(&mut self, amount: i64) -> PnCounterOp {
        let pn = self.counts.entry(self.replica_id).or_insert(Pn::new());
        pn.increment(amount);
        PnCounterOp { replica_id: self.replica_id, pn: pn.clone() }
    }

    /// Get the replica ID of this counter.
    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }
}

impl Crdt for PnCounter {

    type Operation = PnCounterOp;

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
        for (replica_id, pn) in other.counts.into_iter() {
            self.counts.entry(replica_id).or_insert(Pn::new()).merge(pn);
        }
    }

    /// Apply an increment operation to this counter.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// Applying an operation to a `PnCounter` is idempotent.
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
    fn apply(&mut self, op: PnCounterOp) {
        let PnCounterOp { replica_id, pn } = op;
        self.counts.entry(replica_id).or_insert(Pn::new()).merge(pn);
    }
}

impl PartialEq for PnCounter {
    fn eq(&self, other: &PnCounter) -> bool {
        self.counts == other.counts
    }
}

impl PartialOrd for PnCounter {
    fn partial_cmp(&self, other: &PnCounter) -> Option<Ordering> {

        /// Compares `a` to `b` based on replica counts.
        ///
        /// Precondition: `a.counts.len() <= b.counts.len()`
        fn a_gt_b(a: &PnCounter, b: &PnCounter) -> bool {
            a.counts.iter().any(|(replica_id, a_pn)| {
                match b.counts.get(replica_id) {
                    Some(b_pn) => a_pn.p > b_pn.p || a_pn.n > b_pn.n,
                    None => true,
                }
            })
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
impl Arbitrary for PnCounter {
    fn arbitrary<G>(g: &mut G) -> PnCounter where G: Gen {
        use gen_replica_id;
        PnCounter { replica_id: gen_replica_id(), counts: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=PnCounter> + 'static> {
        let replica_id = self.replica_id();
        Box::new(self.counts.shrink().map(move |counts| PnCounter { replica_id: replica_id, counts: counts }))
    }
}

#[cfg(any(quickcheck, test))]
impl Arbitrary for PnCounterOp {
    fn arbitrary<G>(g: &mut G) -> PnCounterOp where G: Gen {
        PnCounterOp { replica_id: Arbitrary::arbitrary(g), pn: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=PnCounterOp> + 'static> {
        let replica_id = self.replica_id;
        Box::new(self.pn.shrink().map(move |pn| PnCounterOp { replica_id: replica_id, pn: pn }))
    }
}

#[cfg(test)]
mod test {

    use quickcheck::quickcheck;

    use {Crdt, ReplicaId, test};
    use super::{PnCounter, PnCounterOp};

    type C = PnCounter;
    type O = PnCounterOp;

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
    fn check_local_increment(increments: Vec<i32>) -> bool {
        let mut counter = PnCounter::new(ReplicaId(0));
        for &amount in increments.iter() {
            counter.increment(amount as i64);
        }
        increments.into_iter().fold(0, |a, b| a + b) as i64 == counter.count()
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: PnCounter, b: PnCounter) -> bool {
        a.merge(b.clone());
        a.increment(-1);
        a > b && b < a
    }


    #[quickcheck]
    fn check_ordering_none(mut a: PnCounter, mut b: PnCounter) -> bool {
        a.increment(1);
        b.increment(-1);
        a.partial_cmp(&b) == None && b.partial_cmp(&a) == None
    }
}
