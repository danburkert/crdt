use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::fmt::{Debug, Formatter, Error};
use std::hash::Hash;

#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

use Crdt;

/// A last-writer wins set.
#[derive(Clone, Default, Eq)]
pub struct LwwSet<T> where T: Eq + Hash {
    elements: HashMap<T, (bool, u64)>
}

/// An insert or remove operation over `LwwSet` CRDTs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LwwSetOp<T> {
    Insert(T, u64),
    Remove(T, u64),
}

impl <T> LwwSet<T> where T: Clone + Eq + Hash {

    /// Create a new last-writer wins set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::LwwSet;
    ///
    /// let mut set = LwwSet::<i32>::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn new() -> LwwSet<T> {
        LwwSet { elements: HashMap::new() }
    }

    /// Insert an element into a two-phase set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::LwwSet;
    ///
    /// let mut set = LwwSet::new();
    /// set.insert("first-element", 0);
    /// assert!(set.contains(&"first-element"));
    /// ```
    pub fn insert(&mut self, element: T, transaction_id: u64) -> Option<LwwSetOp<T>> {
        match self.elements.entry(element.clone()) {
            Occupied(ref mut entry) if transaction_id >= entry.get().1 => {
                entry.insert((true, transaction_id));
                Some(LwwSetOp::Insert(element, transaction_id))
            },
            Vacant(entry) => {
                entry.insert((true, transaction_id));
                Some(LwwSetOp::Insert(element, transaction_id))
            },
            _ => None,
        }
    }

    /// Remove an element from a two-phase set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::LwwSet;
    ///
    /// let mut set = LwwSet::new();
    /// set.insert("first-element", 0);
    /// assert!(set.contains(&"first-element"));
    /// set.remove("first-element", 1);
    /// assert!(!set.contains(&"first-element"));
    /// ```
    pub fn remove(&mut self, element: T, transaction_id: u64) -> Option<LwwSetOp<T>> {

        let updated = match self.elements.entry(element.clone()) {
            Occupied(ref mut entry) if transaction_id > entry.get().1 => {
                entry.insert((false, transaction_id));
                true
            },
            Vacant(entry) => {
                entry.insert((false, transaction_id));
                true
            },
            _ => false,
        };

        if updated {
            Some(LwwSetOp::Remove(element, transaction_id))
        } else {
            None
        }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.iter().filter(|&(_, &(is_present, _))| is_present).count()
    }

    /// Returns true if the set contains the value.
    pub fn contains(&self, value: &T) -> bool {
        self.elements.get(value).map(|&(is_present, _)| is_present).unwrap_or(false)
    }

    /// Returns true if the set contains no elements.
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn is_subset(&self, other: &LwwSet<T>) -> bool {
        self.elements
            .iter()
            .all(|(element, &(is_present, _))| !is_present || other.contains(element))
    }

    pub fn is_disjoint(&self, other: &LwwSet<T>) -> bool {
        self.elements
            .iter()
            .all(|(element, &(is_present, _))| !is_present || !other.contains(element))
    }
}

impl <T> Crdt for LwwSet<T> where T: Clone + Eq + Hash {

    type Operation = LwwSetOp<T>;

    /// Merge a replica into the set.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::LwwSet;
    /// use crdt::Crdt;
    ///
    /// let mut local = LwwSet::new();
    /// let mut remote = LwwSet::new();
    ///
    /// local.insert(1i32, 0);
    /// remote.insert(1, 1);
    /// remote.insert(2, 2);
    /// remote.remove(1, 3);
    ///
    /// local.merge(remote);
    /// assert!(local.contains(&2));
    /// assert!(!local.contains(&1));
    /// assert_eq!(1, local.len());
    /// ```
    fn merge(&mut self, other: LwwSet<T>) {
        for (element, (is_present, tid)) in other.elements.into_iter() {
            if is_present {
                self.insert(element, tid);
            } else {
                self.remove(element, tid);
            }
        }
    }

    /// Apply an insert operation to the set.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// Applying an operation to a `LwwSet` is idempotent.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::LwwSet;
    /// # use crdt::Crdt;
    /// let mut local = LwwSet::new();
    /// let mut remote = LwwSet::new();
    ///
    /// let op = remote.insert(13i32, 0).expect("LwwSet should be empty.");
    ///
    /// local.apply(op);
    /// assert!(local.contains(&13));
    /// ```
    fn apply(&mut self, op: LwwSetOp<T>) {
        match op {
            LwwSetOp::Insert(element, tid) => { self.insert(element, tid); },
            LwwSetOp::Remove(element, tid) => { self.remove(element, tid); }
        }
    }
}

impl <T : Eq + Hash> PartialEq for LwwSet<T> {
    fn eq(&self, other: &LwwSet<T>) -> bool {
        self.elements == other.elements
    }
}

impl <T> PartialOrd for LwwSet<T> where T: Eq + Hash {
    fn partial_cmp(&self, other: &LwwSet<T>) -> Option<Ordering> {
        if self.elements == other.elements {
            return Some(Equal);
        }
        let self_is_greater =
            self.elements
                .iter()
                .any(|(element, &(_, self_tid))| {
                    other.elements.get(element).map_or(true, |&(_, other_tid)| {
                        self_tid > other_tid
                    })
                });

        let other_is_greater =
            other.elements
                .iter()
                .any(|(element, &(_, other_tid))| {
                        self.elements.get(element).map_or(true, |&(_, self_tid)| {
                        other_tid > self_tid
                    })
                });

        if self_is_greater && other_is_greater {
            None
        } else if self_is_greater {
            Some(Greater)
        } else {
            Some(Less)
        }
    }
}

impl <T> Debug for LwwSet<T> where T: Debug + Eq + Hash {
     fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
         try!(write!(f, "{{present: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &(is_present, _))| is_present)
                           .map(|(e, &(_, tid))| (e, tid))
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{:?}", x))
         }
         try!(write!(f, "}}, removed: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &(is_present, _))| !is_present)
                           .map(|(e, &(_, tid))| (e, tid))
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{:?}", x))
         }
         write!(f, "}}}}")
     }
}

#[cfg(any(quickcheck, test))]
impl <T : Arbitrary + Eq + Hash + Clone> Arbitrary for LwwSet<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> LwwSet<T> {
        LwwSet { elements: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=LwwSet<T>> + 'static> {
        Box::new(self.elements.shrink().map(|es| LwwSet { elements: es }))
    }
}

#[cfg(any(quickcheck, test))]
impl <T : Arbitrary> Arbitrary for LwwSetOp<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> LwwSetOp<T> {
        if Arbitrary::arbitrary(g) {
            LwwSetOp::Insert(Arbitrary::arbitrary(g), Arbitrary::arbitrary(g))
        } else {
            LwwSetOp::Insert(Arbitrary::arbitrary(g), Arbitrary::arbitrary(g))
        }
    }
    fn shrink(&self) -> Box<Iterator<Item=LwwSetOp<T>> + 'static> {
        match self.clone() {
            LwwSetOp::Insert(element, tid) => {
                Box::new((element, tid).shrink().map(|(e, t)| LwwSetOp::Insert(e, t)))
            }
            LwwSetOp::Remove(element, tid) => {
                Box::new((element, tid).shrink().map(|(e, t)| LwwSetOp::Remove(e, t)))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::u64;

    use quickcheck::quickcheck;

    use {test, Crdt};
    use super::{LwwSet, LwwSetOp};

    type C = LwwSet<u32>;
    type O = LwwSetOp<u32>;

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
        let mut set = LwwSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element, 0);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: LwwSet<u8>, b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        a.insert(0, u64::MAX);
        a > b && b < a
    }
}
