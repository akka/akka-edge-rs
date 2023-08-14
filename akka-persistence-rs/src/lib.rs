#![doc = include_str!("../README.md")]

pub mod effect;
pub mod entity;
pub mod entity_manager;

/// Uniquely identifies an entity, or entity instance.
pub type EntityId = smol_str::SmolStr;

/// A message encapsulates a command that is addressed to a specific entity.
#[derive(Debug, PartialEq)]
pub struct Message<C> {
    pub entity_id: EntityId,
    pub command: C,
}

impl<C> Message<C> {
    pub fn new<EI>(entity_id: EI, command: C) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            command,
        }
    }
}

/// Additional information associated with a record.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordMetadata {
    /// Flags whether the associated event is to be considered
    /// as one that represents an entity instance being deleted.
    pub deletion_event: bool,
}

/// A record is an event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct Record<E> {
    pub entity_id: EntityId,
    pub event: E,
    pub metadata: RecordMetadata,
}

impl<E> Record<E> {
    pub fn new<EI>(entity_id: EI, event: E) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            event,
            metadata: RecordMetadata {
                deletion_event: false,
            },
        }
    }
}
