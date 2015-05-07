use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::hash_map::{self, HashMap};
use std::hash::Hash;

#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

use {Crdt, ReplicaId};
use pn::Pn;

/// A counting add/remove set.
#[derive(Clone, Debug)]
pub struct PnSet<T> where T: Eq + Hash {
    replica_id: ReplicaId,
    elements: HashMap<T, HashMap<ReplicaId, Pn>>,
}

/// An insert or remove operation over `PnSet` CRDTs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PnSetOp<T> {
    element: T,
    replica_id: ReplicaId,
    pn: Pn,
}

fn count(replica_counts: &HashMap<ReplicaId, Pn>) -> i64 {
    replica_counts.values().fold(0, |sum, pn| sum + pn.count())
}

impl <T> PnSet<T> where T: Clone + Eq + Hash {

    /// Create a new counting add/remove set with the provided replica id.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::PnSet;
    ///
    /// let mut set = PnSet::<i32>::new(0);
    /// assert!(set.is_empty());
    /// ```
    pub fn new<R>(replica_id: R) -> PnSet<T>
    where R: Into<ReplicaId> {
        PnSet { replica_id: replica_id.into(), elements: HashMap::new() }
    }

    /// Insert an element into a counting add/remove set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::PnSet;
    ///
    /// let mut set = PnSet::new(0);
    /// set.insert("first-element");
    /// assert!(set.contains(&"first-element"));
    /// ```
    pub fn insert(&mut self, element: T) -> PnSetOp<T> {
        self.increment_element(element, 1)
    }

    /// Remove an element from a counting add/remove set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::PnSet;
    ///
    /// let mut set = PnSet::new(0);
    /// set.insert("first-element");
    /// assert!(set.contains(&"first-element"));
    /// set.remove("first-element");
    /// assert!(!set.contains(&"first-element"));
    /// ```
    pub fn remove(&mut self, element: T) -> PnSetOp<T> {
        self.increment_element(element, -1)
    }

    /// Increments the count of an element in the set by the given amount.
    fn increment_element(&mut self, element: T, amount: i64) -> PnSetOp<T> {
        let pn = self.elements
                     .entry(element.clone())
                     .or_insert_with(|| HashMap::new())
                     .entry(self.replica_id)
                     .or_insert(Pn::new());
        pn.increment(amount);
        PnSetOp { replica_id: self.replica_id, element: element, pn: pn.clone() }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns true if the set contains the value.
    pub fn contains(&self, element: &T) -> bool {
        self.elements
            .get(element)
            .map_or(false, |replica_counts| count(replica_counts) > 0)
    }

    /// Returns true if the set contains no elements.
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn is_subset(&self, other: &PnSet<T>) -> bool {
        self.iter().all(|element| other.contains(element))
    }

    pub fn is_disjoint(&self, other: &PnSet<T>) -> bool {
        self.iter().all(|element| !other.contains(element))
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, T> {
        Iter { inner: self.elements.iter() }
    }
}

impl <T> Crdt for PnSet<T> where T: Clone + Eq + Hash {

    type Operation = PnSetOp<T>;

    /// Merge a replica into the set.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::PnSet;
    /// use crdt::Crdt;
    ///
    /// let mut local = PnSet::new(0);
    /// let mut remote = PnSet::new(1);
    ///
    /// local.insert(1i32);
    /// remote.insert(1);
    /// remote.insert(2);
    /// remote.remove(1);
    ///
    /// local.merge(remote);
    /// assert!(local.contains(&2));
    /// assert!(local.contains(&1));
    /// assert_eq!(2, local.len());
    /// ```
    fn merge(&mut self, other: PnSet<T>) {
        for (element, other_count) in other.elements.into_iter() {
            let self_count = self.elements.entry(element).or_insert_with(|| HashMap::new());
            for (replica_id, pn) in other_count.into_iter() {
                self_count.entry(replica_id)
                          .or_insert(Pn::new())
                          .merge(pn);
            }
        }
    }

    /// Apply an insert operation to the set.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// Applying an operation to a `PnSet` is idempotent.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::PnSet;
    /// # use crdt::Crdt;
    /// let mut local = PnSet::new(0);
    /// let mut remote = PnSet::new(1);
    ///
    /// let op = remote.insert(13i32);
    ///
    /// local.apply(op);
    /// assert!(local.contains(&13));
    /// ```
    fn apply(&mut self, operation: PnSetOp<T>) {
        let PnSetOp { element, replica_id, pn } = operation;
        self.elements
            .entry(element)
            .or_insert_with(|| HashMap::new())
            .entry(replica_id)
            .or_insert(Pn::new())
            .merge(pn);
    }
}

impl <T : Eq + Hash> PartialEq for PnSet<T> {
    fn eq(&self, other: &PnSet<T>) -> bool {
        self.elements == other.elements
    }
}

impl <T : Eq + Hash> Eq for PnSet<T> {}

impl <T : Eq + Hash> PartialOrd for PnSet<T> {
    fn partial_cmp(&self, other: &PnSet<T>) -> Option<Ordering> {

        fn a_gt_b(a: &HashMap<ReplicaId, Pn>, b: &HashMap<ReplicaId, Pn>) -> bool {
            a.len() > b.len() ||
                a.iter().any(|(replica_id, a_pn)| {
                    b.get(replica_id)
                     .map_or(true, |b_pn| a_pn.p > b_pn.p || a_pn.n > b_pn.n)
                })
        }

        let self_is_greater =
            self.elements
                .iter()
                .any(|(element, counts)| {
                    other.elements
                         .get(element)
                         .map_or(true, |other_counts| a_gt_b(counts, other_counts))
                });

        let other_is_greater =
            other.elements
                 .iter()
                 .any(|(element, counts)| {
                     self.elements
                          .get(element)
                          .map_or(true, |other_counts| a_gt_b(counts, other_counts))
                 });

        if self_is_greater && other_is_greater {
            None
        } else if self_is_greater {
            Some(Greater)
        } else if other_is_greater {
            Some(Less)
        } else {
            Some(Equal)
        }
    }
}

#[cfg(any(quickcheck, test))]
impl <T> Arbitrary for PnSet<T> where T: Arbitrary + Clone + Eq + Hash {
    fn arbitrary<G>(g: &mut G) -> PnSet<T> where G: Gen {
        use gen_replica_id;
        PnSet {
            replica_id: gen_replica_id(),
            elements: Arbitrary::arbitrary(g),
        }
    }
    fn shrink(&self) -> Box<Iterator<Item=PnSet<T>> + 'static> {
        let replica_id: ReplicaId = self.replica_id;
        Box::new(
            self.elements
                .shrink()
                .map(move |es| PnSet { replica_id: replica_id, elements: es }))
    }
}

#[cfg(any(quickcheck, test))]
impl <T> Arbitrary for PnSetOp<T> where T: Arbitrary {
    fn arbitrary<G>(g: &mut G) -> PnSetOp<T> where G: Gen {
        PnSetOp {
            element: Arbitrary::arbitrary(g),
            replica_id: Arbitrary::arbitrary(g),
            pn: Arbitrary::arbitrary(g),
        }
    }
    fn shrink(&self) -> Box<Iterator<Item=PnSetOp<T>> + 'static> {
        let PnSetOp { element, replica_id, pn } = self.clone();
        Box::new(
            (element, replica_id, pn).shrink()
                                     .map(|(element, replica_id, pn)| {
                                         PnSetOp { element: element.clone(),
                                                   replica_id: replica_id.clone(),
                                                   pn: pn.clone() }
                                     }))
    }
}

pub struct Iter<'a, T: 'a> {
    inner: hash_map::Iter<'a, T, HashMap<ReplicaId, Pn>>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        while let Some((ref element, ref replica_counts)) = self.inner.next() {
            if count(replica_counts) > 0 {
                return Some(element)
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod test {

    use quickcheck::quickcheck;

    use {Crdt, ReplicaId, test};
    use super::{PnSet, PnSetOp};

    type C = PnSet<u32>;
    type O = PnSetOp<u32>;

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
    fn check_local_insert(elements: Vec<u8>) -> bool {
        let mut set = PnSet::new(ReplicaId(0));
        for element in elements.clone().into_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: PnSet<u8>, b: PnSet<u8>) -> bool {
        a.merge(b.clone());
        a.insert(0);
        a > b && b < a
    }
}
