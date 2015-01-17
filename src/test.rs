//! Utility functions for using CRDTs in tests.

use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

static mut REPLICA_COUNT: AtomicUsize = ATOMIC_USIZE_INIT;

/// Generate a replica ID suitable for local testing.
///
/// The replica ID is guaranteed to be unique within the processes. This
/// function should **not** be used for generating replica IDs in a distributed
/// system.
pub fn gen_replica_id() -> u64 {
    unsafe { REPLICA_COUNT.fetch_add(1, SeqCst) as u64 }
}
