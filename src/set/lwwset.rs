use std::collections::HashMap;
use std::collections::hash_map::{Occupied, Vacant};
use std::fmt::{Show, Formatter, Error};
use std::hash::Hash;

use quickcheck::{Arbitrary, Gen, Shrinker};

use Crdt;

/// A last-writer wins set.
pub struct LwwSet<T> {
    elements: HashMap<T, (bool, u64)>
}

/// An insert or remove operation over `LwwSet` CRDTs.
#[deriving(Clone, Show, PartialEq, Eq, Hash)]
pub enum LwwSetOp<T> {
    Insert(T, u64),
    Remove(T, u64),
}

impl <T : Hash + Eq + Clone> LwwSet<T> {

    /// Create a new last-writer wins set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::LwwSet;
    ///
    /// let mut set = LwwSet::<int>::new();
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
        let updated = match self.elements.entry(element.clone()) {
            Occupied(ref mut entry) if transaction_id >= entry.get().val1() => {
                entry.set((true, transaction_id));
                true
            },
            Vacant(entry) => {
                entry.set((true, transaction_id));
                true
            },
            _ => false,
        };

        if updated {
            Some(LwwSetOp::Insert(element, transaction_id))
        } else {
            None
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
            Occupied(ref mut entry) if transaction_id > entry.get().val1() => {
                entry.set((false, transaction_id));
                true
            },
            Vacant(entry) => {
                entry.set((false, transaction_id));
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
    pub fn len(&self) -> uint {
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

impl <T : Hash + Eq + Clone + Show> Crdt<LwwSetOp<T>> for LwwSet<T> {

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
    /// local.insert(1i, 0);
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
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::LwwSet;
    /// # use crdt::Crdt;
    /// let mut local = LwwSet::new();
    /// let mut remote = LwwSet::new();
    ///
    /// let op = remote.insert(13i, 0).expect("LwwSet should be empty.");
    ///
    /// local.apply(op);
    /// assert!(local.contains(&13));
    /// ```
    fn apply(&mut self, operation: LwwSetOp<T>) {
        match operation {
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

impl <T : Eq + Hash> Eq for LwwSet<T> {}

impl <T : Eq + Hash + Show> PartialOrd for LwwSet<T> {
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

impl <T : Eq + Hash + Show> Show for LwwSet<T> {
     fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
         try!(write!(f, "{{present: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &(is_present, _))| is_present)
                           .map(|(e, &(_, tid))| (e, tid))
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{}", x))
         }
         try!(write!(f, "}}, removed: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &(is_present, _))| !is_present)
                           .map(|(e, &(_, tid))| (e, tid))
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{}", x))
         }
         write!(f, "}}}}")
     }
}

impl <T : Clone> Clone for LwwSet<T> {
    fn clone(&self) -> LwwSet<T> {
        LwwSet { elements: self.elements.clone() }
    }
}

impl <T : Arbitrary + Eq + Hash + Clone> Arbitrary for LwwSet<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> LwwSet<T> {
        let elements: Vec<(T, (bool, u64))> = Arbitrary::arbitrary(g);
        LwwSet { elements: elements.into_iter().collect() }
    }
    fn shrink(&self) -> Box<Shrinker<LwwSet<T>>+'static> {
        let elements: Vec<(T, (bool, u64))> = self.elements.clone().into_iter().collect();
        let sets: Vec<LwwSet<T>> = elements.shrink().map(|es| LwwSet { elements: es.into_iter().collect() }).collect();
        box sets.into_iter() as Box<Shrinker<LwwSet<T>>>
    }
}

impl <T : Arbitrary> Arbitrary for LwwSetOp<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> LwwSetOp<T> {
        if Arbitrary::arbitrary(g) {
            LwwSetOp::Insert(Arbitrary::arbitrary(g), Arbitrary::arbitrary(g))
        } else {
            LwwSetOp::Insert(Arbitrary::arbitrary(g), Arbitrary::arbitrary(g))
        }
    }
    fn shrink(&self) -> Box<Shrinker<LwwSetOp<T>>+'static> {
        match *self {
            LwwSetOp::Insert(ref element, tid) => {
                let mut inserts: Vec<LwwSetOp<T>> = element.shrink().map(|e| LwwSetOp::Insert(e, tid)).collect();
                inserts.extend(tid.shrink().map(|t| LwwSetOp::Insert(element.clone(), t)));
                box inserts.into_iter() as Box<Shrinker<LwwSetOp<T>>>
            }
            LwwSetOp::Remove(ref element, tid) => {
                let mut removes: Vec<LwwSetOp<T>> = element.shrink().map(|e| LwwSetOp::Remove(e, tid)).collect();
                removes.extend(tid.shrink().map(|t| LwwSetOp::Remove(element.clone(), t)));
                box removes.into_iter() as Box<Shrinker<LwwSetOp<T>>>
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::u64;

    use Crdt;
    use super::{LwwSet, LwwSetOp};

    #[quickcheck]
    fn check_local_insert(elements: Vec<u8>) -> bool {
        let mut set = LwwSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element, 0);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn check_apply_is_commutative(operations: Vec<LwwSetOp<u8>>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<LwwSetOp<u8>> = operations.into_iter().take(5).collect();

        let mut reference = LwwSet::new();
        for operation in truncated.clone().into_iter() {
            reference.apply(operation);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(LwwSet::new(), |mut set, op| {
                         set.apply(op.clone());
                         set
                     })
                 })
                 .all(|set| set == reference)
    }

    #[quickcheck]
    fn check_merge_is_commutative(counters: Vec<LwwSet<u8>>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<LwwSet<u8>> = counters.into_iter().take(4).collect();

        let mut reference = LwwSet::new();
        for set in truncated.clone().into_iter() {
            reference.merge(set);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(LwwSet::new(), |mut set, other| {
                         set.merge(other.clone());
                         set
                     })
                 })
                 .all(|set| set == reference)
    }

    #[quickcheck]
    fn check_ordering_lte(mut a: LwwSet<u8>, b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: LwwSet<u8>, b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        a.insert(0, u64::MAX);
        a > b && b < a
    }

    #[quickcheck]
    fn check_ordering_equality(mut a: LwwSet<u8>, mut b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }
}
