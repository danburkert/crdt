//! Utility functions for using CRDTs in tests.

use std::cmp::Ordering::Equal;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

use quickcheck::TestResult;

use Crdt;

static mut REPLICA_COUNT: AtomicUsize = ATOMIC_USIZE_INIT;

/// Generate a replica ID suitable for local testing.
///
/// The replica ID is guaranteed to be unique within the processes. This
/// function should **not** be used for generating replica IDs in a distributed
/// system.
pub fn gen_replica_id() -> u64 {
    unsafe { REPLICA_COUNT.fetch_add(1, SeqCst) as u64 }
}

pub fn apply_is_commutative<C>(crdt: C, ops: Vec<C::Operation>) -> TestResult where C: Crdt {
    // This test takes too long with too many operations
    if ops.len() > 5 { return TestResult::discard(); }

    let expected: C = ops.iter()
                         .cloned()
                         .fold(crdt.clone(), |mut crdt, op| {
                             crdt.apply(op);
                             crdt
                         });

    TestResult::from_bool(
        ops[..].permutations()
               .map(|permutation| {
                   permutation.iter().cloned().fold(crdt.clone(), |mut crdt, op| {
                       crdt.apply(op);
                       crdt
                   })
               })
               .all(|crdt| crdt == expected))
}

pub fn merge_is_commutative<C>(crdt: C, crdts: Vec<C>) -> TestResult where C: Crdt {
    // This test takes too long with too many crdts
    if crdts.len() > 5 { return TestResult::discard(); }

    let expected: C = crdts.iter()
                           .cloned()
                           .fold(crdt.clone(), |mut crdt, other| {
                               crdt.merge(other);
                               crdt
                           });

    TestResult::from_bool(
        crdts[..].permutations()
                 .map(|permutation| {
                     permutation.iter().cloned().fold(crdt.clone(), |mut crdt, other| {
                         crdt.merge(other);
                         crdt
                     })
                 })
                 .all(|crdt| crdt == expected))
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
