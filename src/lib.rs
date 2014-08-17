//! A library of Conflict-free Replicated Data Types.
//!
//! Conflict-free replicated data types (also called convergent and commutative
//! replicated data types) allow for concurrent updates to distributed replicas
//! with strong eventual consistency and without coordination.
//!
//! ###### Further Reading
//!
//! 1. [A comprehensive study of Convergent and Commutative Replicated Data Types](http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf) (Shapiro, et al.)
//! 2. [An Optimized Conflict-free Replicated Set](http://arxiv.org/pdf/1210.3368.pdf) (Bieniusa, et al.)

#![feature(phase)]
extern crate quickcheck;

#[phase(plugin, link)]
extern crate log;

pub mod counter;
pub mod test;

/// A Conflict-free Replicated Data Type.
///
/// Conflict-free replicated data types (also called convergent and commutative
/// replicated data types) allow for concurrent updates to distributed replicas
/// with strong eventual consistency and without coordination.
///
/// ###### Replication
///
/// Updates to CRDTs can be shared with replicas in two ways: state-based
/// replication and operation-based replication. With state-based replication,
/// the entire state of the mutated CRDT is merged into remote replicas in order
/// to restore consistency. With operation-based replication, only the mutating
/// operation is applied to remote replicas in order to restore consistency.
/// Accordingly, operation-based replication is relatively lighter weight, but
/// has the requirement that all operations must be reliably broadcast and
/// applied to remote replicas. State-based replication schemes can maintain
/// (eventual) consistency merely by guaranteeing that state based replication
/// will (eventually) happen.
///
/// ###### Replica ID
///
/// All CRDT replicas are required to have a `uint` identifier, or replica ID.
/// The replica ID **must** be unique among replicas, so it should be taken from
/// unique per-replica configuration, or from a source of strong coordination
/// such as [ZooKeeper](http://zookeeper.apache.org/) or
/// [etcd](https://github.com/coreos/etcd) (implementing a Rust client for
/// these services is left as an exercise for the reader).
///
/// ###### Transaction IDs
///
/// Many CRDTs require the user to provide a transaction ID when performing
/// mutating operations. Transaction IDs provided to an individual replica
/// **must** be monotonically increasing across operations. Transaction IDs
/// across replicas **must** be unique, and **should** be as close to globally
/// monotonically increasing as possible. Unlike replicas IDs, these
/// requirements do not require strong coordination among replicas. See
/// [Snowflake](https://github.com/twitter/snowflake) for an example of
/// distributed, uncoordinated ID generation which meets the requirements.
///
/// ###### Partial Ordering
///
/// Replicas of a CRDT are partially-ordered over the set of possible
/// operations. If all operations applied to replica `B` have been applied to
/// `A` (or, somewhat equivalently, if `B` has been merged into `A`), then
/// `A <= B`.
///
/// ###### Equality
///
/// Equality among CRDT replicas does not take into account the replica ID;
/// only the operation history is taken into account.
///
/// ###### Further Reading
///
/// 1. [A comprehensive study of Convergent and Commutative Replicated Data Types](http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf) (Shapiro, et al.)
/// 2. [An Optimized Conflict-free Replicated Set](http://arxiv.org/pdf/1210.3368.pdf) (Bieniusa, et al.)
pub trait Crdt<Operation : CrdtOperation> : PartialOrd {

    /// Merge a replica into this CRDT.
    ///
    /// This method is used to perform state-based replication.
    fn merge(&mut self, other: &Self);

    /// Apply an increment operation to this CRDT.
    ///
    /// This method is used to perform operation-based replication.
    fn apply(&mut self, operation: &Operation);

    /// Get the replica ID of this CRDT.
    ///
    /// Replica IDs **must** be unique among replicas of a CRDT.
    fn replica_id(&self) -> uint;
}

/// An operation on a CRDT.
pub trait CrdtOperation {

    /// Get the replica ID of the origin CRDT.
    fn replica_id(&self) -> uint;
}
