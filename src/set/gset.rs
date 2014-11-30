use std::collections::{HashSet};
use std::fmt::{Show, Formatter, Error};
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

#[cfg(test)]
mod test {

    use Crdt;
    use super::{GSet, GSetInsert};

    #[quickcheck]
    fn check_local_insert(elements: Vec<u8>) -> bool {
        let mut set = GSet::new();
        for element in elements.clone().into_iter() {
            set.insert(element);
        }

        elements.iter().all(|element| set.contains(element))
    }

    #[quickcheck]
    fn check_apply_is_commutative(inserts: Vec<GSetInsert<u8>>) -> bool {
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
    fn check_merge_is_commutative(counters: Vec<GSet<u8>>) -> bool {
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
    fn check_ordering_lte(mut a: GSet<u8>, b: GSet<u8>) -> bool {
        a.merge(b.clone());
        a >= b && b <= a
    }

    #[quickcheck]
    fn check_ordering_lt(mut a: GSet<u8>, b: GSet<u8>) -> bool {
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
    fn check_ordering_equality(mut a: GSet<u8>, mut b: GSet<u8>) -> bool {
        a.merge(b.clone());
        b.merge(a.clone());
        a == b
            && b == a
            && a.partial_cmp(&b) == Some(Equal)
            && b.partial_cmp(&a) == Some(Equal)
    }
}
