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
use std::fmt::{Show, Formatter, FormatError};
use std::hash::Hash;

use quickcheck::{Arbitrary, Gen, Shrinker};

use Crdt;

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
        for element in other.elements.move_iter() {
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

impl <T : Hash + Eq> Collection for GSet<T> {
    fn len(&self) -> uint {
        self.elements.len()
    }
}

impl <T : Hash + Eq> Set<T> for GSet<T> {
    fn contains(&self, value: &T) -> bool {
        self.elements.contains(value)
    }
    fn is_subset(&self, other: &GSet<T>) -> bool {
        self.elements.is_subset(&other.elements)
    }
    fn is_disjoint(&self, other: &GSet<T>) -> bool {
        self.elements.is_disjoint(&other.elements)
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
     fn fmt(&self, f: &mut Formatter) -> Result<(), FormatError> {
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
        GSet { elements: elements.move_iter().collect() }
    }
    fn shrink(&self) -> Box<Shrinker<GSet<T>>> {
        let elements: Vec<T> = self.elements.clone().move_iter().collect();
        let sets: Vec<GSet<T>> = elements.shrink()
                                         .map(|es| GSet { elements: es.move_iter().collect() })
                                         .collect();
        box sets.move_iter() as Box<Shrinker<GSet<T>>>
    }
}

impl <T : Arbitrary> Arbitrary for GSetInsert<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> GSetInsert<T> {
        GSetInsert { element: Arbitrary::arbitrary(g) }
    }
    fn shrink(&self) -> Box<Shrinker<GSetInsert<T>>> {
        let inserts: Vec<GSetInsert<T>> = self.element
                                              .shrink()
                                              .map(|e| GSetInsert { element: e })
                                              .collect();
        box inserts.move_iter() as Box<Shrinker<GSetInsert<T>>>
    }
}


/// A two-phase set.
pub struct TpSet<T> {
    elements: HashMap<T, bool>
}

/// An insert or remove operation over `TpSet` CRDTs.
#[deriving(Clone, Show, PartialEq, Eq, Hash)]
pub enum TpSetOperation<T> {
    TpSetInsert(T),
    TpSetRemove(T)
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
    pub fn insert(&mut self, element: T) -> Option<TpSetOperation<T>> {
        if self.elements.contains_key(&element) {
            None
        } else {
            self.elements.insert(element.clone(), true);
            Some(TpSetInsert(element))
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
    pub fn remove(&mut self, element: T) -> Option<TpSetOperation<T>> {
        if self.elements.insert(element.clone(), false) {
            Some(TpSetRemove(element))
        } else {
            None
        }
    }
}

impl <T : Hash + Eq + Clone> Crdt<TpSetOperation<T>> for TpSet<T> {

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
        for (element, is_present) in other.elements.move_iter() {
            if is_present {
                self.elements.find_or_insert(element, is_present);
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
    fn apply(&mut self, operation: TpSetOperation<T>) {
        match operation {
            TpSetInsert(element) => { self.insert(element); },
            TpSetRemove(element) => { self.remove(element); }
        }
    }
}

impl <T : Hash + Eq> Collection for TpSet<T> {
    fn len(&self) -> uint {
        self.elements.iter().filter(|&(_, &is_present)| is_present).count()
    }
}

impl <T : Hash + Eq> Set<T> for TpSet<T> {
    fn contains(&self, value: &T) -> bool {
        *self.elements.find(value).unwrap_or(&false)
    }
    fn is_subset(&self, other: &TpSet<T>) -> bool {
        for (element, &is_present) in self.elements.iter() {
            if is_present && !other.contains(element) { return false; }
        }
        true
    }
    fn is_disjoint(&self, other: &TpSet<T>) -> bool {
        for (element, &is_present) in self.elements.iter() {
            if is_present && other.contains(element) { return false; }
        }
        true
    }
}

impl <T : Eq + Hash> PartialEq for TpSet<T> {
    fn eq(&self, other: &TpSet<T>) -> bool {
        self.elements == other.elements
    }
}

impl <T : Eq + Hash> Eq for TpSet<T> {}

impl <T : Eq + Hash> PartialOrd for TpSet<T> {

    // TODO: This isn't right because it doesn't take into account the phases.
    // A TpSet > another TpSet if it's seen all the same events: so it must have
    // all the same elements in the remove set, and all of the same elements in the add set
    // either in its add set or its remove set.


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
     fn fmt(&self, f: &mut Formatter) -> Result<(), FormatError> {
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
        TpSet { elements: elements.move_iter().collect() }
    }
    fn shrink(&self) -> Box<Shrinker<TpSet<T>>> {
        let elements: Vec<(T, bool)> = self.elements.clone().move_iter().collect();
        let sets: Vec<TpSet<T>> = elements.shrink().map(|es| TpSet { elements: es.move_iter().collect() }).collect();
        box sets.move_iter() as Box<Shrinker<TpSet<T>>>
    }
}

impl <T : Arbitrary> Arbitrary for TpSetOperation<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> TpSetOperation<T> {
        if Arbitrary::arbitrary(g) {
            TpSetInsert(Arbitrary::arbitrary(g))
        } else {
            TpSetInsert(Arbitrary::arbitrary(g))
        }
    }
    fn shrink(&self) -> Box<Shrinker<TpSetOperation<T>>> {
        match *self {
            TpSetInsert(ref element) => {
                let inserts: Vec<TpSetOperation<T>> = element.shrink().map(|e| TpSetInsert(e)).collect();
                box inserts.move_iter() as Box<Shrinker<TpSetOperation<T>>>
            }
            TpSetRemove(ref element) => {
                let removes: Vec<TpSetOperation<T>> = element.shrink().map(|e| TpSetRemove(e)).collect();
                box removes.move_iter() as Box<Shrinker<TpSetOperation<T>>>
            }
        }
    }
}

#[cfg(test)]
mod test {

    #[phase(plugin)]
    extern crate quickcheck_macros;

    #[phase(plugin, link)] extern crate log;

    use Crdt;
    use set::{GSet, GSetInsert, TpSet, TpSetOperation};

    #[quickcheck]
    fn gset_local_insert(elements: Vec<String>) -> bool {
        let mut set = GSet::new();
        for element in elements.clone().move_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn gset_apply_is_commutative(inserts: Vec<GSetInsert<String>>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<GSetInsert<String>> = inserts.move_iter().take(5).collect();

        let mut reference = GSet::new();
        for insert in truncated.clone().move_iter() {
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
    fn gset_merge_is_commutative(counters: Vec<GSet<String>>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<GSet<String>> = counters.move_iter().take(4).collect();

        let mut reference = GSet::new();
        for set in truncated.clone().move_iter() {
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
    fn gset_ordering_lte(mut a: GSet<String>, b: GSet<String>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn gset_ordering_lt(mut a: GSet<String>, b: GSet<String>) -> bool {
        a.merge(b.clone());
        a.insert("foo".to_string());
        a > b && b < a
    }

    #[quickcheck]
    fn gset_ordering_equality(mut a: GSet<String>, mut b: GSet<String>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }

    #[quickcheck]
    fn tpset_local_insert(elements: Vec<String>) -> bool {
        let mut set = TpSet::new();
        for element in elements.clone().move_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn tpset_apply_is_commutative(operations: Vec<TpSetOperation<String>>) -> bool {
        // This test takes too long with too many operations, so we truncate
        let truncated: Vec<TpSetOperation<String>> = operations.move_iter().take(5).collect();

        let mut reference = TpSet::new();
        for operation in truncated.clone().move_iter() {
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
    fn tpset_merge_is_commutative(counters: Vec<TpSet<String>>) -> bool {
        // This test takes too long with too many counters, so we truncate
        let truncated: Vec<TpSet<String>> = counters.move_iter().take(4).collect();

        let mut reference = TpSet::new();
        for set in truncated.clone().move_iter() {
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
    fn tpset_ordering_lt(mut a: TpSet<String>, b: TpSet<String>) -> bool {
        a.merge(b.clone());
        a.insert("foo".to_string());
        a > b && b < a
    }

    #[quickcheck]
    fn tpset_ordering_equality(mut a: TpSet<String>, mut b: TpSet<String>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }
}
