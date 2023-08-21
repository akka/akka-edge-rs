#![doc = include_str!("../README.md")]

pub mod effect;
pub mod entity;
pub mod entity_manager;

/// Uniquely identifies the type of an Entity.
pub type EntityType = smol_str::SmolStr;

/// Uniquely identifies an entity, or entity instance.
pub type EntityId = smol_str::SmolStr;

/// A namespaced entity id given an entity type.
pub struct PersistenceId {
    pub entity_type: EntityType,
    pub entity_id: EntityId,
}

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
