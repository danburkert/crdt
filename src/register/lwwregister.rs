#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

use std::cmp::Ordering;
use std::ops::Deref;

use {Crdt, TransactionId};

/// A last-writer-wins register.
#[derive(Debug, Clone)]
pub struct LwwRegister<T> {
    value: T,
    transaction_id: TransactionId,
}

impl <T> LwwRegister<T> where T: Clone {

    /// Create a new last-writer-wins register with the provided initial value
    /// and transaction ID.
    ///
    /// ##### Example
    ///
    /// ```
    /// use crdt::register::LwwRegister;
    /// use crdt::TransactionId;
    ///
    /// let mut register = LwwRegister::new("my-value", TransactionId::from(0));
    /// ```
    pub fn new<I>(value: T, transaction_id: I) -> LwwRegister<T>
    where I: Into<TransactionId> {
        LwwRegister { value: value, transaction_id: transaction_id.into() }
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

    /// Set the register to the provided value and transaction ID.
    ///
    /// Returns an operation that can be applied to other replicas if the set
    /// succeeds (by having the latest transaction ID).
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// # use crdt::TransactionId;
    /// let mut register = LwwRegister::new("my-value", 0);
    /// assert_eq!(TransactionId::from(0), register.transaction_id());
    /// ```
    pub fn set<I>(&mut self, value: T, transaction_id: I) -> Option<LwwRegister<T>>
    where I: Into<TransactionId> {
        let tid: TransactionId = transaction_id.into();
        if self.transaction_id <= tid {
            self.value = value;
            self.transaction_id = tid;
            Some(self.clone())
        } else { None }
    }

    /// Get the transaction ID associated with the current value in the
    /// register.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::register::LwwRegister;
    /// # use crdt::TransactionId;
    /// let mut register = LwwRegister::new("my-value", 0);
    /// assert_eq!(TransactionId::from(0), register.transaction_id());
    /// ```
    pub fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }
}

impl<T> Deref for LwwRegister<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl <T> Crdt for LwwRegister<T> where T: Clone {

    type Operation = LwwRegister<T>;

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
    /// assert_eq!("remote", *local);
    /// ```
    fn merge(&mut self, other: LwwRegister<T>) {
        if self.transaction_id <= other.transaction_id {
            self.value = other.value.clone();
            self.transaction_id = other.transaction_id;
        }
    }

    /// Apply a set operation to this register.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// Applying an operation to a `LwwRegister` is idempotent.
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
    /// assert_eq!("remote-2", *local);
    /// ```
    fn apply(&mut self, op: LwwRegister<T>) {
        self.merge(op);
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

#[cfg(any(quickcheck, test))]
impl <T> Arbitrary for LwwRegister<T> where T: Arbitrary {
    fn arbitrary<G: Gen>(g: &mut G) -> LwwRegister<T> {
        LwwRegister { value: Arbitrary::arbitrary(g), transaction_id: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=LwwRegister<T>> + 'static> {
        let tuple = (self.value.clone(), self.transaction_id);
        Box::new(tuple.shrink().map(|(value, tid)| LwwRegister { value: value, transaction_id: tid }))
    }
}

#[cfg(test)]
mod test {

    use quickcheck::quickcheck;

    use {test, Crdt};
    use register::LwwRegister;

    type C = LwwRegister<u32>;
    type O = LwwRegister<u32>;

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
    fn check_local_increment(versions: Vec<String>) -> bool {
        let mut register = LwwRegister::new("".to_string(), 0);
        for (transaction_id, value) in versions.iter().enumerate() {
            register.set(value.clone(), transaction_id as u64);
        }
        &*register == versions.last().unwrap_or(&"".to_string())
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: LwwRegister<String>, b: LwwRegister<String>) -> bool {
        a.merge(b.clone());
        let current_tid = a.transaction_id();
        a.set("foo".to_string(), current_tid.id() + 1);
        a > b && b < a
    }
}
