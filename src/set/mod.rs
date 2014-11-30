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

pub use self::gset::{GSet, GSetInsert};
pub use self::tpset::{TpSet, TpSetOp};
pub use self::lwwset::{LwwSet, LwwSetOp};

mod gset;
mod tpset;
mod lwwset;
