//! Register CRDTs.
//!
//! The `set` operation on a register does not commute, so a traditional
//! register cannot be a CRDT. Instead, some approximations of registers
//! are provided whose operations are commutative. These registers types
//! differ primarily by how concurrent set operations are resolved.
//!
//! ##### Register Types
//!
/// ###### `LwwRegister`
///
/// A last-writer wins register. `LwwRegister` does not have a separate
/// operation type for operation-based replication. Instead, operation-based
/// replication uses the full state of the register.
///
/// `LwwRegister` keeps the value written with the largest transaction ID.
/// In order to prevent (or limit the period of) lost-writes, transaction
/// IDs **must** be unique and **should** be globally monotonically increasing.

pub use self::lwwregister::LwwRegister;

mod lwwregister;
