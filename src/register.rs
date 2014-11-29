//! Register CRDTs.

use Crdt;

use quickcheck::{Arbitrary, Gen, Shrinker};

/// A last-writer-wins register.
///
/// `LwwRegister` does not have a separate operation type for operation-based
/// replication. Instead, operation-based replication uses the full state of the
/// register.
///
/// `LwwRegister` keeps the value written with the largest transaction ID.
/// In order to prevent (or limit the period of) lost-writes, transaction
/// IDs **must** be unique and **should** be globally monotonically increasing.
#[deriving(Show, Clone)]
pub struct LwwRegister<T> {
    value: T,
    transaction_id: u64
}

impl <T> LwwRegister<T> {

    /// Create a new last-writer-wins register with the provided initial value
    /// and transaction ID.
    ///
    /// ##### Example
    ///
    /// ```
    /// use crdt::register::LwwRegister;
    ///
    /// let mut register = LwwRegister::new("my-value", 0);
    /// ```
    pub fn new(value: T, transaction_id: u64) -> LwwRegister<T> {
        LwwRegister { value: value, transaction_id: transaction_id }
    }

    /// Get the current value in the register.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// let mut register = LwwRegister::new("my-value", 0);
    /// assert_eq!("my-value", *register.get());
    /// ```
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Get the transaction ID associated with the current value in the
    /// register.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// let mut register = LwwRegister::new("my-value", 0);
    /// assert_eq!(0, register.transaction_id());
    /// ```
    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }
}

impl <T : Clone> LwwRegister<T> {

    /// Set the register to the provided value and transaction ID.
    ///
    /// Returns an operation that can be applied to other replicas if the set
    /// succeeds (by having the latest transation ID).
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// let mut register = LwwRegister::new("my-value", 0);
    /// assert_eq!(0, register.transaction_id());
    /// ```
    pub fn set(&mut self, value: T, transaction_id: u64) -> Option<LwwRegister<T>> {
        if self.transaction_id <= transaction_id {
            self.value = value;
            self.transaction_id = transaction_id;
            Some(self.clone())
        } else { None }
    }
}

impl <T : Clone> Crdt<LwwRegister<T>> for LwwRegister<T> {

    /// Merge a replica into this register.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// use crdt::Crdt;
    ///
    /// let mut local = LwwRegister::new("local", 1);
    /// let mut remote = LwwRegister::new("remote", 2);
    ///
    /// local.merge(remote);
    /// assert_eq!("remote", *local.get());
    /// ```
    fn merge(&mut self, other: LwwRegister<T>) {
        if self.transaction_id <= other.transaction_id {
            self.value = other.value.clone();
            self.transaction_id = other.transaction_id;
        }
    }

    /// Apply a set operation to this counter.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// # use crdt::Crdt;
    /// let mut local = LwwRegister::new("local", 1);
    /// let mut remote = LwwRegister::new("remote-1", 0);
    ///
    /// let op = remote.set("remote-2", 2).expect("Register set failed!");
    ///
    /// local.apply(op);
    /// assert_eq!("remote-2", *local.get());
    /// ```
    fn apply(&mut self, other: LwwRegister<T>) {
        self.merge(other);
    }
}

impl <T> PartialEq for LwwRegister<T> {
    fn eq(&self, other: &LwwRegister<T>) -> bool {
        self.transaction_id == other.transaction_id
    }
}

impl <T> Eq for LwwRegister<T> {}

impl <T> PartialOrd for LwwRegister<T> {
    fn partial_cmp(&self, other: &LwwRegister<T>) -> Option<Ordering> {
        Some(self.transaction_id.cmp(&other.transaction_id))
    }
}

impl <T> Ord for LwwRegister<T> {
    fn cmp(&self, other: &LwwRegister<T>) -> Ordering {
        self.transaction_id.cmp(&other.transaction_id)
    }
}

impl <T : Arbitrary> Arbitrary for LwwRegister<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> LwwRegister<T> {
        LwwRegister { value: Arbitrary::arbitrary(g), transaction_id: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<LwwRegister<T>>+'static> {
        let tuple = (self.value.clone(), self.transaction_id);
        box tuple
            .shrink()
            .map(|(value, tid)| LwwRegister { value: value, transaction_id: tid })
            as Box<Shrinker<LwwRegister<T>>>
    }
}

#[cfg(test)]
mod test {

    #[phase(plugin)]
    extern crate quickcheck_macros;

    extern crate quickcheck;

    use quickcheck::{Arbitrary, Gen, Shrinker};

    use Crdt;
    use register::LwwRegister;

    #[quickcheck]
    fn lwwregister_local_increment(versions: Vec<String>) -> bool {
        let mut register = LwwRegister::new("".to_string(), 0);
        for (transaction_id, value) in versions.iter().enumerate() {
            register.set(value.clone(), transaction_id as u64);
        }
        register.get() == versions.last().unwrap_or(&"".to_string())
    }

    #[quickcheck]
    fn lwwregister_apply_is_commutative(mutations: Vec<LwwRegister<String>>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<LwwRegister<String>> = mutations.into_iter().take(6).collect();

        let mut reference = LwwRegister::new("".to_string(), 0);
        for increment in truncated.clone().into_iter() {
            reference.apply(increment);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(LwwRegister::new("".to_string(), 0), |mut counter, op| {
                         counter.apply(op.clone());
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn lwwregister_merge_is_commutative(counters: Vec<LwwRegister<String>>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<LwwRegister<String>> = counters.into_iter().take(5).collect();

        let mut reference = LwwRegister::new("".to_string(), 0);
        for counter in truncated.clone().into_iter() {
            reference.merge(counter);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(LwwRegister::new("".to_string(), 0), |mut counter, other| {
                         counter.merge(other.clone());
                         counter
                     })
                 })
                 .all(|counter| counter == reference)
    }

    #[quickcheck]
    fn lwwregister_ordering_lte(mut a: LwwRegister<String>, b: LwwRegister<String>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn lwwregister_ordering_lt(mut a: LwwRegister<String>, b: LwwRegister<String>) -> bool {
        a.merge(b.clone());
        let current_tid = a.transaction_id();
        a.set("foo".to_string(), current_tid + 1);
        a > b && b < a
    }

    #[quickcheck]
    fn lwwregister_ordering_equality(mut a: LwwRegister<String>, mut b: LwwRegister<String>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }
}
