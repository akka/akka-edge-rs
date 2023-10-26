//! An offset store keeps track of the last offset used in relation to
//! an entity, including its entity type. It can be used in various places,
//! for example, when consuming event envelopes from a remote projection, where
//! they be emitted by the consumer out of order. This entity can track such situations.

use akka_persistence_rs::{Offset, PersistenceId};
use tokio::sync::oneshot;

/// A last offset can comprise of the same offset type value for a number
/// of persistence identifiers.
pub type LastOffset = (Vec<PersistenceId>, Offset);

pub enum Command {
    GetLastOffset {
        reply_to: oneshot::Sender<Option<LastOffset>>,
    },
    GetOffset {
        persistence_id: PersistenceId,
        reply_to: oneshot::Sender<Option<Offset>>,
    },
    SaveOffset {
        persistence_id: PersistenceId,
        offset: Offset,
    },
}
