#![doc = include_str!("../README.md")]

use akka_persistence_rs::{Offset, PersistenceId, TimestampOffset, WithOffset};
use smol_str::SmolStr;

pub mod consumer;
mod delayer;
pub mod producer;

/// An envelope wraps a gRPC event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub persistence_id: PersistenceId,
    pub event: E,
    pub offset: TimestampOffset,
}

impl<E> WithOffset for EventEnvelope<E> {
    fn offset(&self) -> Offset {
        Offset::Timestamp(self.offset.clone())
    }
}

/// Identifies an event producer to a consumer
pub type OriginId = SmolStr;

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
