use std::cmp;
use std::collections::TrieMap;

use Crdt;

/// A grow-only counter.
///
/// `GCounter` monotonically increases across increment operations.
#[deriving(Show)]
pub struct GCounter {
    replica_id: uint,
    counts: TrieMap<u64>
}

/// An increment operation over `GCounter` CRDTs.
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
        self.counts.values().fold(0, |a, &b| a + b)
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
    /// The increment limit is globally shared across all replicas, and thus
    /// is not checked during local operations.
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


/// A incrementable and decrementable counter.
#[deriving(Show)]
pub struct PNCounter {
    replica_id: uint,
    counts: TrieMap<(u64, u64)>
}

/// An increment or decrement operation over `PNCounter` CRDTs.
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
        self.counts.values().fold(0, |acc, &(p, n)| acc + (p as i64 - n as i64))
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
    /// increments for the purposes of these limits. The increment and
    /// decrement limit is globally shared across all replicas, and thus is not
    /// checked during local operations.
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
                (0, (-operation.amount) as u64)
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

#[cfg(test)]
mod test {

    use Crdt;
    use counter::GCounter;

    #[test]
    fn g_counter_create() {
        let counter = GCounter::new(0);
        assert_eq!(0, counter.count());
    }

    #[test]
    fn g_counter_increment() {
        let mut counter = GCounter::new(42);
        assert_eq!(0, counter.count());
        counter.increment(1);
        assert_eq!(1, counter.count());
        counter.increment(2);
        assert_eq!(3, counter.count());
        counter.increment(3);
        assert_eq!(6, counter.count());
    }

    #[test]
    fn g_counter_merge() {
        let mut a = GCounter::new(1);
        let mut b = GCounter::new(2);

        a.increment(13);
        b.increment(17);

        a.merge(&b);
        b.merge(&a);
        assert_eq!(30, a.count());
        assert_eq!(30, b.count());
    }

    #[test]
    fn g_counter_apply() {
        let mut a = GCounter::new(1);
        let mut b = GCounter::new(2);

        let ref a_op_1 = a.increment(5);
        let ref a_op_2 = a.increment(8);
        let ref b_op = b.increment(17);

        a.apply(b_op);
        b.apply(a_op_1);
        b.apply(a_op_2);
        assert_eq!(30, a.count());
        assert_eq!(30, b.count());
    }
}
