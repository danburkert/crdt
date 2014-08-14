use std::cmp;
use std::collections::TrieMap;

#[deriving(Show)]
pub struct GCounter {
    replica_id: uint,
    counts: TrieMap<u64>
}

pub struct GCounterOperation {
    replica_id: uint,
    amount: u64
}

impl GCounter {

    pub fn new(replica_id: uint) -> GCounter {
        GCounter { replica_id: replica_id, counts: TrieMap::new() }
    }

    pub fn count(& self) -> u64 {
        self.counts.values().fold(0, |a, &b| a + b)
    }

    pub fn increment(&mut self, amount: u64) -> GCounterOperation {
        let operation = GCounterOperation { replica_id: self.replica_id, amount: amount };
        self.apply(&operation);
        operation
    }

    pub fn merge(&mut self, other: &GCounter) {
        for (replica_id, other_count) in other.counts.iter() {
            let count = match self.counts.find_mut(&replica_id) {
                Some(self_count) => cmp::max(*self_count, *other_count),
                None => *other_count
            };
            self.counts.insert(replica_id, count);
        }
    }

    pub fn apply(&mut self, operation: &GCounterOperation) {
        let count = match self.counts.find_mut(&operation.replica_id) {
            Some(self_count) => *self_count + operation.amount,
            None => operation.amount
        };
        self.counts.insert(operation.replica_id, count);
    }
}

#[cfg(test)]
mod test {

    use super::GCounter;

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
