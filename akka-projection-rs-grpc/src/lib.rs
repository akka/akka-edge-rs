#![doc = include_str!("../README.md")]

use akka_persistence_rs::EntityId;

pub mod consumer;

/// An envelope wraps a gRPC event associated with a specific entity.
/// FIXME: This will have gRPC specific fields.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub entity_id: EntityId,
    pub event: E,
}

impl<E> EventEnvelope<E> {
    pub fn new<EI>(entity_id: EI, event: E) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            event,
        }
    }
}
