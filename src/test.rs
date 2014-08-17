//! Utility functions for using CRDTs in tests.

use std::sync::atomic::{AtomicUint, SeqCst, INIT_ATOMIC_UINT};

static mut REPLICA_COUNT: AtomicUint = INIT_ATOMIC_UINT;

/// Generate a replica ID suitable for local testing.
///
/// The replica ID is guaranteed to be unique within the processes. This
/// function should **not** be used for generating replica IDs in a distributed
/// system.
pub fn gen_replica_id() -> uint {
    unsafe { REPLICA_COUNT.fetch_add(1, SeqCst) }
}
