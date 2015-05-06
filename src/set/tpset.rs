use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap};
use std::fmt::{Debug, Formatter, Error};
use std::hash::Hash;

#[cfg(any(quickcheck, test))]
use quickcheck::{Arbitrary, Gen};

use Crdt;

/// A two-phase set.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TpSet<T> where T: Eq + Hash {
    elements: HashMap<T, bool>,
}

/// An insert or remove operation over `TpSet` CRDTs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TpSetOp<T> {
    Insert(T),
    Remove(T),
}

impl <T : Hash + Eq + Clone> TpSet<T> {

    /// Create a new two-phase set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::TpSet;
    ///
    /// let mut set = TpSet::<i32>::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn new() -> TpSet<T> {
        TpSet { elements: HashMap::new() }
    }

    /// Insert an element into a two-phase set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::TpSet;
    ///
    /// let mut set = TpSet::new();
    /// set.insert("first-element");
    /// assert!(set.contains(&"first-element"));
    /// ```
    pub fn insert(&mut self, element: T) -> Option<TpSetOp<T>> {
        if self.elements.contains_key(&element) {
            None
        } else {
            self.elements.insert(element.clone(), true);
            Some(TpSetOp::Insert(element))
        }
    }

    /// Remove an element from a two-phase set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::TpSet;
    ///
    /// let mut set = TpSet::new();
    /// set.insert("first-element");
    /// assert!(set.contains(&"first-element"));
    /// set.remove("first-element");
    /// assert!(!set.contains(&"first-element"));
    /// ```
    pub fn remove(&mut self, element: T) -> Option<TpSetOp<T>> {
        match self.elements.entry(element.clone()) {
            Vacant(entry) => {
                entry.insert(false);
                Some(TpSetOp::Remove(element))
            },
            Occupied(ref mut entry) if *entry.get() => {
                entry.insert(false);
                Some(TpSetOp::Remove(element))
            },
            Occupied(_) => None,
        }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.iter().filter(|&(_, &is_present)| is_present).count()
    }

    /// Returns true if the set contains the value.
    pub fn contains(&self, value: &T) -> bool {
        *self.elements.get(value).unwrap_or(&false)
    }

    /// Returns true if the set contains no elements.
    pub fn is_empty(&self) -> bool{ self.len() == 0 }

    pub fn is_subset(&self, other: &TpSet<T>) -> bool {
        for (element, &is_present) in self.elements.iter() {
            if is_present && !other.contains(element) { return false; }
        }
        true
    }

    pub fn is_disjoint(&self, other: &TpSet<T>) -> bool {
        for (element, &is_present) in self.elements.iter() {
            if is_present && other.contains(element) { return false; }
        }
        true
    }
}

impl <T> Crdt for TpSet<T> where T: Clone + Eq + Hash {

    type Operation = TpSetOp<T>;

    /// Merge a replica into the set.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::TpSet;
    /// use crdt::Crdt;
    ///
    /// let mut local = TpSet::new();
    /// let mut remote = TpSet::new();
    ///
    /// local.insert(1i32);
    /// remote.insert(1);
    /// remote.insert(2);
    /// remote.remove(1);
    ///
    /// local.merge(remote);
    /// assert!(local.contains(&2));
    /// assert_eq!(1, local.len());
    /// ```
    fn merge(&mut self, other: TpSet<T>) {
        for (element, is_present) in other.elements.into_iter() {
            if is_present {
                match self.elements.entry(element) {
                    Occupied(_) => (),
                    Vacant(entry) => { entry.insert(is_present); },
                }
            } else {
                self.elements.insert(element, is_present);
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
    /// # use crdt::set::TpSet;
    /// # use crdt::Crdt;
    /// let mut local = TpSet::new();
    /// let mut remote = TpSet::new();
    ///
    /// let op = remote.insert(13i32).expect("TpSet should be empty.");
    ///
    /// local.apply(op);
    /// assert!(local.contains(&13));
    /// ```
    fn apply(&mut self, operation: TpSetOp<T>) {
        match operation {
            TpSetOp::Insert(element) => { self.insert(element); },
            TpSetOp::Remove(element) => { self.remove(element); }
        }
    }
}

impl <T : Eq + Hash> PartialOrd for TpSet<T> {
    fn partial_cmp(&self, other: &TpSet<T>) -> Option<Ordering> {
        if self.elements == other.elements {
            return Some(Equal);
        }
        let mut self_is_greater = true;
        let mut other_is_greater = true;
        for (element, &is_present) in other.elements.iter() {
            if is_present {
                if !self.elements.contains_key(element) {
                    self_is_greater = false;
                    break;
                }
            } else {
                match self.elements.get(element) {
                    Some(&false) => (),
                    _ => {
                        self_is_greater = false;
                        break;
                    }
                }
            }
        }
        for (element, &is_present) in self.elements.iter() {
            if is_present {
                if !other.elements.contains_key(element) {
                    other_is_greater = false;
                    break;
                }
            } else {
                match other.elements.get(element) {
                    Some(&false) => (),
                    _ => {
                        other_is_greater = false;
                        break;
                    }
                }
            }
        }
        if self_is_greater && other_is_greater {
            None
        } else if self_is_greater {
            Some(Greater)
        } else {
            Some(Less)
        }
    }
}

impl <T : Eq + Hash + Debug> Debug for TpSet<T> {
     fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
         try!(write!(f, "{{present: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &is_present)| is_present)
                           .map(|(e, _)| e)
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{:?}", *x))
         }
         try!(write!(f, "}}, removed: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &is_present)| !is_present)
                           .map(|(e, _)| e)
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{:?}", *x))
         }
         write!(f, "}}}}")
     }
}

#[cfg(any(quickcheck, test))]
impl <T : Arbitrary + Eq + Hash + Clone> Arbitrary for TpSet<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> TpSet<T> {
        TpSet { elements: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Iterator<Item=TpSet<T>> + 'static> {
        Box::new(self.elements.shrink().map(|elements| TpSet { elements: elements }))
    }
}

#[cfg(any(quickcheck, test))]
impl <T> Arbitrary for TpSetOp<T> where T: Arbitrary {
    fn arbitrary<G: Gen>(g: &mut G) -> TpSetOp<T> {
        if Arbitrary::arbitrary(g) {
            TpSetOp::Insert(Arbitrary::arbitrary(g))
        } else {
            TpSetOp::Insert(Arbitrary::arbitrary(g))
        }
    }
    fn shrink(&self) -> Box<Iterator<Item=TpSetOp<T>> + 'static> {
        match *self {
            TpSetOp::Insert(ref element) => {
                Box::new(element.shrink().map(|e| TpSetOp::Insert(e)))
            }
            TpSetOp::Remove(ref element) => {
                Box::new(element.shrink().map(|e| TpSetOp::Remove(e)))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use quickcheck::{TestResult, quickcheck};

    use {test, Crdt};
    use super::{TpSet, TpSetOp};

    type C = TpSet<u32>;
    type O = TpSetOp<u32>;

    #[test]
    fn check_apply_is_commutative() {
        quickcheck(test::apply_is_commutative::<C> as fn(C, Vec<O>) -> TestResult);
    }

    #[test]
    fn check_merge_is_commutative() {
        quickcheck(test::merge_is_commutative::<C> as fn(C, Vec<C>) -> TestResult);
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
        let mut set = TpSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: TpSet<u8>, b: TpSet<u8>) -> bool {
        a.merge(b.clone());
        let mut i = 0;
        let mut success = None;
        while success.is_none() {
            success = a.insert(i);
            i += 1;
        }
        a > b && b < a
    }
}
