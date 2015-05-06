//! A library of Conflict-free Replicated Data Types.
//!
//! ###### Further Reading
//!
//! 1. [_A comprehensive study of Convergent and Commutative Replicated Data Types_](http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf) (Shapiro, et al.)
//! 2. [_An Optimized Conflict-free Replicated Set_](http://arxiv.org/pdf/1210.3368.pdf) (Bieniusa, et al.)

#![cfg_attr(test, feature(collections, custom_attribute, plugin))]
#![cfg_attr(test, plugin(quickcheck_macros))]

#[cfg(any(quickcheck, test))]
extern crate quickcheck;

pub mod counter;
pub mod register;
pub mod set;

#[cfg(any(quickcheck, test))]
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
/// Operation-based replication is lighter weight in terms of the amount of
/// data which must be transmitted to replicas per mutation, but has the
/// requirement that all operations must be reliably broadcast and applied to
/// remote replicas. State-based replication schemes can maintain (eventual)
/// consistency merely by guaranteeing that state based replication will
/// (eventually) happen. Shapiro, et al. have shown that state-based CRDTs are
/// equivalent to operation-based CRDTs. The CRDTs exposed by this library
/// allow for either state-based or operation-based replication, or a mix of
/// both.
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
/// Equality among CRDT replicas does not take into account the replica ID, if
/// it exists. Only the operation history is taken into account.
pub trait Crdt : Clone + Eq + PartialOrd {

    type Operation: Clone;

    /// Merge a replica into this CRDT.
    ///
    /// This method is used to perform state-based replication.
    fn merge(&mut self, other: Self);

    /// Apply an operation to this CRDT.
    ///
    /// This method is used to perform operation-based replication.
    fn apply(&mut self, operation: Self::Operation);
}

/// The Id of an individual replica of a Crdt.
///
/// Some CRDTs require a `u64` replica ID upon creation. The replica ID **must**
/// be unique among replicas, so it should be taken from unique per-replica
/// configuration, or from a source of strong coordination such as
/// [ZooKeeper](http://zookeeper.apache.org/) or [etcd](https://github.com/coreos/etcd).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaId(u64);

impl ReplicaId {
    pub fn id(self) -> u64 {
        self.0
    }
}

impl Into<ReplicaId> for u64 {
    fn into(self) -> ReplicaId {
        ReplicaId(self)
    }
}

#[cfg(any(quickcheck, test))]
impl quickcheck::Arbitrary for ReplicaId {
    fn arbitrary<G: quickcheck::Gen>(g: &mut G) -> ReplicaId {
        ReplicaId(quickcheck::Arbitrary::arbitrary(g))
    }
    fn shrink(&self) -> Box<Iterator<Item=ReplicaId> + 'static> {
        Box::new(self.id().shrink().map(|id| ReplicaId(id)))
    }
}

/// The Id for an individual operation on a CRDT.
///
/// Some CRDTs require the user to provide a transaction ID when performing
/// mutating operations. Transaction IDs provided to an individual replica
/// **must** be monotonically increasing across operations. Transaction IDs
/// across replicas **must** be unique, and **should** be as close to globally
/// monotonically increasing as possible. Unlike replicas IDs, these
/// requirements do not require strong coordination among replicas. See
/// [Snowflake](https://github.com/twitter/snowflake) for an example of
/// distributed, uncoordinated ID generation which meets the requirements.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn id(self) -> u64 {
        self.0
    }
}

impl Into<TransactionId> for u64 {
    fn into(self) -> TransactionId {
        TransactionId(self)
    }
}

#[cfg(any(quickcheck, test))]
impl quickcheck::Arbitrary for TransactionId {
    fn arbitrary<G: quickcheck::Gen>(g: &mut G) -> TransactionId {
        TransactionId(quickcheck::Arbitrary::arbitrary(g))
    }
    fn shrink(&self) -> Box<Iterator<Item=TransactionId> + 'static> {
        Box::new(self.id().shrink().map(|id| TransactionId(id)))
    }
}
