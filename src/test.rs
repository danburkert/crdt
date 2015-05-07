//! Utility functions for using CRDTs in tests.

use std::cmp::Ordering::Equal;

use rand::{thread_rng, Rng};

use Crdt;

pub fn apply_is_commutative<C>(crdt: C, mut ops: Vec<C::Operation>) -> bool where C: Crdt {
    let expected = ops.iter()
                      .cloned()
                      .fold(crdt.clone(), |mut crdt, op| {
                          crdt.apply(op);
                          crdt
                      });

    thread_rng().shuffle(&mut ops[..]);

    expected == ops.into_iter()
                   .fold(crdt.clone(), |mut crdt, op| {
                       crdt.apply(op);
                       crdt
                   })
}

pub fn merge_is_commutative<C>(crdt: C, mut crdts: Vec<C>) -> bool where C: Crdt {
    let expected: C = crdts.iter()
                           .cloned()
                           .fold(crdt.clone(), |mut crdt, other| {
                               crdt.merge(other);
                               crdt
                           });

    thread_rng().shuffle(&mut crdts[..]);

    expected == crdts.into_iter()
                     .fold(crdt.clone(), |mut crdt, other| {
                         crdt.merge(other);
                         crdt
                     })
}

pub fn ordering_lte<C>(mut a: C, b: C) -> bool where C: Crdt {
    a.merge(b.clone());
    a >= b && b <= a
}

pub fn ordering_equality<C>(mut a: C, mut b: C) -> bool where C: Crdt {
    a.merge(b.clone());
    b.merge(a.clone());
    a == b
        && b == a
        && a.partial_cmp(&b) == Some(Equal)
        && b.partial_cmp(&a) == Some(Equal)
}
