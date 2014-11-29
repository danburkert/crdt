//! Set CRDTs.
//!
//! The `add` and `remove` operations on sets do not commute, so a traditional
//! set cannot be a CRDT. Instead, some approximations of sets are provided
//! whose operations are commutative. These set approximations differ primarily
//! by how concurrent add and remove operations are resolved.
//!
//! ##### Set Types
//!
//! ###### `GSet`
//!
//! A grow-only set. `GSet` disallows remove operations altogether, so no
//! concurrent add and remove operations are possible. `GSet` should be
//! preferred to other set CRDTs when the remove operation is not needed.
//!
//! ###### `TpSet`
//!
//! A two-phase set. Elements may be added and subsequently removed, but once
//! removed, an element may never be added again. `2PSet` should be preferred
//! when the application has logical monotonicity in its interactions with the
//! set, and will never need to add an element to the set after it has been
//! removed.
//!
//! ######  `LwwSet`
//!
//! A last-writer-wins set. Add and remove operations take a transaction ID,
//! which is used to resolve concurrent write and remove operations. The
//! 'winner' in the case of concurrent add and remove operations is therefore
//! non-deterministic. `LwwSet` should be preferred when the rate of operations
//! on an element is small compared to the resolution of transaction IDs.
//!
//! ###### `PnSet`
//!
//! A counting add/remove set. Every element has an associated counter which is
//! incremented and decremented for each add and remove operation, respectively.
//! If the counter is positive, the element is a member of the set. If the
//! counter is 0 or negative, the element is not a member of the set. `PnSet`
//! breaks set semantics by allowing the counter to become greater than 1
//! (less than 0), at which point a single remove (add) operation will not be
//! locally observable.
//!
//! ###### `OrSet`
//!
//! An observed-remove set. Clients may only remove elements from the set which
//! are in the local replica. The outcome of a sequence of add and remove
//! operations depends only on the causal history of the operations. In the
//! event of concurrent add and remove operations, add will take precedence.
//! `OrSet` should be used in most cases where typical set semantics are
//! needed.

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::{Occupied, Vacant};
use std::fmt::{Show, Formatter, Error};
use std::hash::Hash;

use quickcheck::{Arbitrary, Gen, Shrinker};

use Crdt;
use counter::PnCounter;

/// A grow-only set.
pub struct GSet<T> {
    elements: HashSet<T>
}

/// An insert operation over `GSet` CRDTs.
#[deriving(Clone, Show, PartialEq, Eq, Hash)]
pub struct GSetInsert<T> {
    element: T
}

impl <T : Hash + Eq + Clone> GSet<T> {

    /// Create a new grow-only set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::GSet;
    ///
    /// let mut set = GSet::<int>::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn new() -> GSet<T> {
        GSet { elements: HashSet::new() }
    }

    /// Insert an element into a grow-only set.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::GSet;
    ///
    /// let mut set = GSet::new();
    /// set.insert("first-element");
    /// assert!(set.contains(&"first-element"));
    /// ```
    pub fn insert(&mut self, element: T) -> Option<GSetInsert<T>> {
        if self.elements.insert(element.clone()) {
            Some(GSetInsert { element: element })
        } else {
            None
        }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> uint {
        self.elements.len()
    }

    /// Returns true if the set contains the value.
    pub fn contains(&self, value: &T) -> bool {
        self.elements.contains(value)
    }

    /// Returns true if the set contains no elements.
    pub fn is_empty(&self) -> bool{ self.len() == 0 }

    pub fn is_subset(&self, other: &GSet<T>) -> bool {
        self.elements.is_subset(&other.elements)
    }

    pub fn is_disjoint(&self, other: &GSet<T>) -> bool {
        self.elements.is_disjoint(&other.elements)
    }
}

impl <T : Hash + Eq + Clone> Crdt<GSetInsert<T>> for GSet<T> {

    /// Merge a replica into the set.
    ///
    /// This method is used to perform state-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::GSet;
    /// use crdt::Crdt;
    ///
    /// let mut local = GSet::new();
    /// let mut remote = GSet::new();
    ///
    /// local.insert(1i);
    /// remote.insert(2);
    ///
    /// local.merge(remote);
    /// assert!(local.contains(&2));
    /// ```
    fn merge(&mut self, other: GSet<T>) {
        for element in other.elements.into_iter() {
            self.insert(element);
        }
    }

    /// Apply an insert operation to the set.
    ///
    /// This method is used to perform operation-based replication.
    ///
    /// ##### Example
    ///
    /// ```
    /// # use crdt::set::GSet;
    /// # use crdt::Crdt;
    /// let mut local = GSet::new();
    /// let mut remote = GSet::new();
    ///
    /// let op = remote.insert(13i).expect("GSet should be empty.");
    ///
    /// local.apply(op);
    /// assert!(local.contains(&13));
    /// ```
    fn apply(&mut self, operation: GSetInsert<T>) {
        self.insert(operation.element);
    }
}

impl <T : Eq + Hash> PartialEq for GSet<T> {
    fn eq(&self, other: &GSet<T>) -> bool {
        self.elements == other.elements
    }
}

impl <T : Eq + Hash> Eq for GSet<T> {}

impl <T : Eq + Hash> PartialOrd for GSet<T> {
    fn partial_cmp(&self, other: &GSet<T>) -> Option<Ordering> {
        if self.elements == other.elements {
            Some(Equal)
        } else if self.elements.is_subset(&other.elements) {
            Some(Less)
        } else if self.elements.is_superset(&other.elements) {
            Some(Greater)
        } else {
            None
        }
    }
}

impl <T : Eq + Hash + Show> Show for GSet<T> {
     fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
         self.elements.fmt(f)
     }
}

impl <T : Clone> Clone for GSet<T> {
    fn clone(&self) -> GSet<T> {
        GSet { elements: self.elements.clone() }
    }
}

impl <T : Arbitrary + Eq + Hash> Arbitrary for GSet<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> GSet<T> {
        let elements: Vec<T> = Arbitrary::arbitrary(g);
        GSet { elements: elements.into_iter().collect() }
    }
    fn shrink(&self) -> Box<Shrinker<GSet<T>>+'static> {
        let elements: Vec<T> = self.elements.clone().into_iter().collect();
        let sets: Vec<GSet<T>> = elements.shrink()
                                         .map(|es| GSet { elements: es.into_iter().collect() })
                                         .collect();
        box sets.into_iter() as Box<Shrinker<GSet<T>>>
    }
}

impl <T : Arbitrary> Arbitrary for GSetInsert<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> GSetInsert<T> {
        GSetInsert { element: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<GSetInsert<T>>+'static> {
        let inserts: Vec<GSetInsert<T>> = self.element
                                              .shrink()
                                              .map(|e| GSetInsert { element: e })
                                              .collect();
        box inserts.into_iter() as Box<Shrinker<GSetInsert<T>>>
    }
}


/// A two-phase set.
pub struct TpSet<T> {
    elements: HashMap<T, bool>
}

/// An insert or remove operation over `TpSet` CRDTs.
#[deriving(Clone, Show, PartialEq, Eq, Hash)]
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
    /// let mut set = TpSet::<int>::new();
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
                entry.set(false);
                Some(TpSetOp::Remove(element))
            },
            Occupied(ref mut entry) if *entry.get() => {
                entry.set(false);
                Some(TpSetOp::Remove(element))
            },
            Occupied(_) => None,
        }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> uint {
        self.elements.iter().filter(|&(_, &is_present)| is_present).count()
    }

    /// Returns true if the set contains the value.
    pub fn contains(&self, value: &T) -> bool {
        *self.elements.find(value).unwrap_or(&false)
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

impl <T : Hash + Eq + Clone> Crdt<TpSetOp<T>> for TpSet<T> {

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
    /// local.insert(1i);
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
                    Occupied(entry) => (),
                    Vacant(entry) => { entry.set(is_present); },
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
    /// let op = remote.insert(13i).expect("TpSet should be empty.");
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

impl <T : Eq + Hash> PartialEq for TpSet<T> {
    fn eq(&self, other: &TpSet<T>) -> bool {
        self.elements == other.elements
    }
}

impl <T : Eq + Hash> Eq for TpSet<T> {}

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
                match self.elements.find(element) {
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
                match other.elements.find(element) {
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

impl <T : Eq + Hash + Show> Show for TpSet<T> {
     fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
         try!(write!(f, "{{present: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &is_present)| is_present)
                           .map(|(e, _)| e)
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{}", *x))
         }
         try!(write!(f, "}}, removed: {{"));
         for (i, x) in self.elements
                           .iter()
                           .filter(|&(_, &is_present)| !is_present)
                           .map(|(e, _)| e)
                           .enumerate() {
             if i != 0 { try!(write!(f, ", ")); }
             try!(write!(f, "{}", *x))
         }
         write!(f, "}}}}")
     }
}

impl <T : Clone> Clone for TpSet<T> {
    fn clone(&self) -> TpSet<T> {
        TpSet { elements: self.elements.clone() }
    }
}

impl <T : Arbitrary + Eq + Hash + Clone> Arbitrary for TpSet<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> TpSet<T> {
        let elements: Vec<(T, bool)> = Arbitrary::arbitrary(g);
        TpSet { elements: elements.into_iter().collect() }
    }
    fn shrink(&self) -> Box<Shrinker<TpSet<T>>+'static> {
        let elements: Vec<(T, bool)> = self.elements.clone().into_iter().collect();
        let sets: Vec<TpSet<T>> = elements.shrink().map(|es| TpSet { elements: es.into_iter().collect() }).collect();
        box sets.into_iter() as Box<Shrinker<TpSet<T>>>
    }
}

impl <T : Arbitrary> Arbitrary for TpSetOp<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> TpSetOp<T> {
        if Arbitrary::arbitrary(g) {
            TpSetOp::Insert(Arbitrary::arbitrary(g))
        } else {
            TpSetOp::Insert(Arbitrary::arbitrary(g))
        }
    }
    fn shrink(&self) -> Box<Shrinker<TpSetOp<T>>+'static> {
        match *self {
            TpSetOp::Insert(ref element) => {
                let inserts: Vec<TpSetOp<T>> = element.shrink().map(|e| TpSetOp::Insert(e)).collect();
                box inserts.into_iter() as Box<Shrinker<TpSetOp<T>>>
            }
            TpSetOp::Remove(ref element) => {
                let removes: Vec<TpSetOp<T>> = element.shrink().map(|e| TpSetOp::Remove(e)).collect();
                box removes.into_iter() as Box<Shrinker<TpSetOp<T>>>
            }
        }
    }
}

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
            other => false,
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
            other => false,
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
        self.elements.find(value).map(|&(is_present, _)| is_present).unwrap_or(false)
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
                    other.elements.find(element).map_or(true, |&(_, other_tid)| {
                        self_tid > other_tid
                    })
                });

        let other_is_greater =
            other.elements
                .iter()
                .any(|(element, &(_, other_tid))| {
                        self.elements.find(element).map_or(true, |&(_, self_tid)| {
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

    #[phase(plugin)]
    extern crate quickcheck_macros;
    extern crate quickcheck;

    use quickcheck::{Arbitrary, Gen, Shrinker};

    use Crdt;
    use set::{GSet, GSetInsert, TpSet, TpSetOp, LwwSet, LwwSetOp};
    use std::u64;

    #[quickcheck]
    fn gset_local_insert(elements: Vec<u8>) -> bool {
        let mut set = GSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn gset_apply_is_commutative(inserts: Vec<GSetInsert<u8>>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<GSetInsert<u8>> = inserts.into_iter().take(5).collect();

        let mut reference = GSet::new();
        for insert in truncated.clone().into_iter() {
            reference.apply(insert);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(GSet::new(), |mut set, op| {
                         set.apply(op.clone());
                         set
                     })
                 })
                 .all(|set| set == reference)
    }

    #[quickcheck]
    fn gset_merge_is_commutative(counters: Vec<GSet<u8>>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<GSet<u8>> = counters.into_iter().take(4).collect();

        let mut reference = GSet::new();
        for set in truncated.clone().into_iter() {
            reference.merge(set);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(GSet::new(), |mut set, other| {
                         set.merge(other.clone());
                         set
                     })
                 })
                 .all(|set| set == reference)
    }

    #[quickcheck]
    fn gset_ordering_lte(mut a: GSet<u8>, b: GSet<u8>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn gset_ordering_lt(mut a: GSet<u8>, b: GSet<u8>) -> bool {
        a.merge(b.clone());

        let mut i = 0;
        let mut success = None;
        while success.is_none() {
            success = a.insert(i);
            i += 1;
        }
        a > b && b < a
    }

    #[quickcheck]
    fn gset_ordering_equality(mut a: GSet<u8>, mut b: GSet<u8>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }

    #[quickcheck]
    fn tpset_local_insert(elements: Vec<u8>) -> bool {
        let mut set = TpSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn tpset_apply_is_commutative(operations: Vec<TpSetOp<u8>>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<TpSetOp<u8>> = operations.into_iter().take(5).collect();

        let mut reference = TpSet::new();
        for operation in truncated.clone().into_iter() {
            reference.apply(operation);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(TpSet::new(), |mut set, op| {
                         set.apply(op.clone());
                         set
                     })
                 })
                 .all(|set| set == reference)
    }

    #[quickcheck]
    fn tpset_merge_is_commutative(counters: Vec<TpSet<u8>>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<TpSet<u8>> = counters.into_iter().take(4).collect();

        let mut reference = TpSet::new();
        for set in truncated.clone().into_iter() {
            reference.merge(set);
        }

        truncated.as_slice()
                 .permutations()
                 .map(|permutation| {
                     permutation.iter().fold(TpSet::new(), |mut set, other| {
                         set.merge(other.clone());
                         set
                     })
                 })
                 .all(|set| set == reference)
    }

    #[quickcheck]
    fn tpset_ordering_lte(mut a: TpSet<u8>, b: TpSet<u8>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn tpset_ordering_lt(mut a: TpSet<u8>, b: TpSet<u8>) -> bool {
        a.merge(b.clone());
        let mut i = 0;
        let mut success = None;
        while success.is_none() {
            success = a.insert(i);
            i += 1;
        }
        a > b && b < a
    }

    #[quickcheck]
    fn tpset_ordering_equality(mut a: TpSet<u8>, mut b: TpSet<u8>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }

    #[quickcheck]
    fn lwwset_local_insert(elements: Vec<u8>) -> bool {
        let mut set = LwwSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element, 0);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn lwwset_apply_is_commutative(operations: Vec<LwwSetOp<u8>>) -> bool {
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
    fn lwwset_merge_is_commutative(counters: Vec<LwwSet<u8>>) -> bool {
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
    fn lwwset_ordering_lte(mut a: LwwSet<u8>, b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn lwwset_ordering_lt(mut a: LwwSet<u8>, b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        a.insert(0, u64::MAX);
        a > b && b < a
    }

    #[quickcheck]
    fn lwwset_ordering_equality(mut a: LwwSet<u8>, mut b: LwwSet<u8>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }
}
