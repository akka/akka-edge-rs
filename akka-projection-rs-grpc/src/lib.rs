#![doc = include_str!("../README.md")]

use akka_persistence_rs::{EntityId, Offset, PersistenceId, WithOffset};
use chrono::{DateTime, Utc};
use smol_str::SmolStr;

pub mod consumer;
mod delayer;

/// An envelope wraps a gRPC event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub entity_id: EntityId,
    pub event: E,
    pub timestamp: DateTime<Utc>,
    pub seen: Vec<(PersistenceId, u64)>,
}

impl<E> EventEnvelope<E> {
    pub fn new<EI>(
        entity_id: EI,
        event: E,
        timestamp: DateTime<Utc>,
        seen: Vec<(PersistenceId, u64)>,
    ) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            event,
            timestamp,
            seen,
        }
    }
}

impl<E> WithOffset for EventEnvelope<E> {
    fn offset(&self) -> Offset {
        Offset::Timestamp(self.timestamp, self.seen.clone())
    }
}

/// The logical stream identifier, mapped to a specific internal entity type by
/// the producer settings
pub type StreamId = SmolStr;

pub mod proto {
    // Note when using Rust Analyzier, you may get a `non_snake_case` warning.
    // This warning is benign and a bug of Rust Analyzer.
    // https://github.com/rust-lang/rust-analyzer/issues/15344
    // https://github.com/rust-lang/rust-analyzer/issues/15394
    tonic::include_proto!("akka.projection.grpc");
}
