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
    /// println!("Current counter value: {}", counter.count());
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
    /// `GCounter` can overflow exactly as if it were a `u64`, e.g.
    ///
    /// ```
    /// # use std::u64;
    /// # use crdt::counter::GCounter;
    /// let mut counter = GCounter::new(42);
    /// counter.increment(u64::MAX);
    /// assert_eq!(u64::MAX, counter.count());
    /// counter.increment(1);
    /// assert_eq!(0, counter.count());
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

#[deriving(Show)]
pub struct PNCounter {
    replica_id: uint,
    counts: TrieMap<(u32, u32)>
}

pub struct PNCounterOperation {
    replica_id: uint,
    amount: i64
}

impl PNCounter {

    pub fn new(replica_id: uint) -> PNCounter {
        PNCounter { replica_id: replica_id, counts: TrieMap::new() }
    }

    pub fn count(& self) -> i64 {
        self.counts.values().fold(0, |acc, &(p, n)| acc + (p as i64 - n as i64))
    }

    pub fn increment(&mut self, amount: i64) -> PNCounterOperation {
        let operation = PNCounterOperation { replica_id: self.replica_id, amount: amount };
        self.apply(&operation);
        operation
    }

    pub fn merge(&mut self, other: &PNCounter) {
        for (replica_id, other_count) in other.counts.iter() {
            let count = match self.counts.find_mut(&replica_id) {
                Some(self_count) => cmp::max(*self_count, *other_count),
                None => *other_count
            };
            self.counts.insert(replica_id, count);
        }
    }

    pub fn apply(&mut self, operation: &PNCounterOperation) {
        let (p_amount, n_amount) =
            if operation.amount > 0 { (operation.amount as u32, 0) } else { (0, (-operation.amount) as u32) };

        let count = match self.counts.find_mut(&operation.replica_id) {
            Some(&(self_p, self_n)) => (self_p + p_amount, self_n + n_amount),
            None => (p_amount, n_amount)
        };
        self.counts.insert(operation.replica_id, count);
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

    #[test]
    fn g_counter_overflow() {
        //let mut a = GCounter::new(13);
        //let mut b = GCounter::new(2);

        //let ref a_op_1 = a.increment(5);
        //let ref a_op_2 = a.increment(8);
        //let ref b_op = b.increment(17);

        //a.apply(b_op);
        //b.apply(a_op_1);
        //b.apply(a_op_2);
        //assert_eq!(30, a.count());
        //assert_eq!(30, b.count());
    }
}
