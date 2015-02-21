use std::cmp::Ordering::{self, Greater, Less, Equal};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map;
use std::fmt::Debug;
use std::hash::Hash;

#[cfg(any(test, quickcheck_generators))]
use quickcheck::{Arbitrary, Gen, Shrinker};

use Crdt;
use counter::{PnCounter, PnCounterIncrement};

/// A counting add/remove set.
#[derive(Clone, Debug)]
pub struct PnSet<T : Hash + Eq> {
    replica_id: u64,
    elements: HashMap<T, PnCounter>
}

/// An insert or remove operation over `PnSet` CRDTs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PnSetOp<T> {
    element: T,
    counter_op: PnCounterIncrement,
}

impl <T : Hash + Eq + Clone> PnSet<T> {

    /// Create a new counting add/remove set with the provided replica id.
    ///
    /// ### Example
    ///
    /// ```
    /// use crdt::set::PnSet;
    ///
    /// let mut set = PnSet::<int>::new(0);
    /// assert!(set.is_empty());
    /// ```
    pub fn new(replica_id: u64) -> PnSet<T> {
        PnSet { replica_id: replica_id, elements: HashMap::new() }
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
        let counter_op = match self.elements.entry(element.clone()) {
            Occupied(ref mut entry) => entry.get_mut().increment(amount),
            Vacant(entry) => {
                let mut counter = PnCounter::new(self.replica_id);
                let counter_op = counter.increment(amount);
                entry.insert(counter);
                counter_op
            },
        };
        PnSetOp { element: element, counter_op: counter_op }
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns true if the set contains the value.
    pub fn contains(&self, value: &T) -> bool {
        self.elements.get(value).map_or(false, |counter| counter.count() > 0)
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

impl <T : Hash + Eq + Clone + Debug> Crdt<PnSetOp<T>> for PnSet<T> {

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
    /// local.insert(1i);
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
        for (element, counter) in other.elements.into_iter() {
            match self.elements.entry(element) {
                Occupied(ref mut entry) => {
                    entry.get_mut().merge(counter);
                },
                Vacant(entry) => {
                    entry.insert(counter);
                },
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
    /// # use crdt::set::PnSet;
    /// # use crdt::Crdt;
    /// let mut local = PnSet::new(0);
    /// let mut remote = PnSet::new(1);
    ///
    /// let op = remote.insert(13i);
    ///
    /// local.apply(op);
    /// assert!(local.contains(&13));
    /// ```
    fn apply(&mut self, operation: PnSetOp<T>) {
        match self.elements.entry(operation.element) {
            Occupied(ref mut entry) => entry.get_mut().apply(operation.counter_op),
            Vacant(entry) => {
                let mut counter = PnCounter::new(self.replica_id);
                counter.apply(operation.counter_op);
                entry.insert(counter);
            },
        }
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
        if self.elements == other.elements {
            return Some(Equal);
        }
        let self_is_greater =
            self.elements
                .iter()
                .any(|(element, counter)| {
                    other.elements.get(element).map_or(true, |other_counter| counter > other_counter)
                });

        let other_is_greater =
            other.elements
                .iter()
                .any(|(element, other_counter)| {
                    self.elements.get(element).map_or(true, |counter| other_counter > counter)
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

#[cfg(any(test, quickcheck_generators))]
impl <T : Arbitrary + Eq + Hash + Clone> Arbitrary for PnSet<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> PnSet<T> {
        let elements: Vec<(T, PnCounter)> = Arbitrary::arbitrary(g);
        PnSet {
            replica_id: Arbitrary::arbitrary(g),
            elements: elements.into_iter().collect(),
        }
    }
    fn shrink(&self) -> Box<Shrinker<PnSet<T>>+'static> {
        let elements: Vec<(T, PnCounter)> = self.elements.clone().into_iter().collect();
        let mut shrinks: Vec<PnSet<T>> = elements.shrink().map(|es| {
            PnSet {
                replica_id: self.replica_id,
                elements: es.into_iter().collect(),
            }
        }).collect();
        shrinks.extend(self.replica_id.shrink().map(|id| {
            PnSet {
                replica_id: id,
                elements: self.elements.clone()
            }
        }));
        Box::new(shrinks.into_iter()) as Box<Shrinker<PnSet<T>>+'static>
    }
}

#[cfg(any(test, quickcheck_generators))]
impl <T : Arbitrary> Arbitrary for PnSetOp<T> {
    fn arbitrary<G: Gen>(g: &mut G) -> PnSetOp<T> {
        PnSetOp {
            element: Arbitrary::arbitrary(g),
            counter_op: Arbitrary::arbitrary(g),
        }
    }
    fn shrink(&self) -> Box<Shrinker<PnSetOp<T>>+'static> {
        let element = self.element.clone();
        let mut shrinks: Vec<PnSetOp<T>> = element.shrink().map(|elem| PnSetOp { element: elem, counter_op: self.counter_op }).collect();
        shrinks.extend(self.counter_op.shrink().map(|op| PnSetOp { element: element.clone(), counter_op: op }));
        Box::new(shrinks.into_iter()) as Box<Shrinker<PnSetOp<T>>+'static>
    }
}

pub struct Iter<'a, T: 'a> {
    inner: hash_map::Iter<'a, T, PnCounter>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        while let Some(item) = self.inner.next() {
            if item.1.count() > 0 {
                return Some(item.0)
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
