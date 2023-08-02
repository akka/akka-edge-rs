#![doc = include_str!("../README.md")]

use std::{io, pin::Pin};

use async_trait::async_trait;
use tokio_stream::Stream;

pub mod effect;
pub mod entity;
pub mod entity_manager;

/// Uniquely identifies an entity, or entity instance.
pub type EntityId = String;

/// A message encapsulates a command that is addressed to a specific entity.
#[derive(Debug, PartialEq)]
pub struct Message<C> {
    entity_id: EntityId,
    command: C,
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

/// A record is an event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct Record<E> {
    entity_id: EntityId,
    event: E,
}

impl<E> Record<E> {
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

/// A trait for producing a source of events. An entity id
/// is passed to the source method so that the source can be
/// discriminate regarding the entity events to supply. However,
/// the source can also decide to provide events for other
/// entities. Whether it does so or not depends on the capabilities
/// of the source e.g. it may be more efficient to return all
/// entities that can be sourced.
pub trait RecordSource<E> {
    fn produce(
        &mut self,
        entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = Record<E>> + Send>>>;
}

/// A trait for consuming a record, performing some processing
/// e.g. persisting a record, and then returning the same record
/// if all went well.
#[async_trait]
pub trait RecordFlow<E> {
    async fn process(&mut self, record: Record<E>) -> io::Result<Record<E>>;
}
