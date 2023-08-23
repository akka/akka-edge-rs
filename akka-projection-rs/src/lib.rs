//! In Akka Projections you process a stream of event envelopes from a source to a projected model or external system.
//! Each envelope is associated with an offset representing the position in the stream. This offset is used for resuming
//! the stream from that position when the projection is restarted.

use std::pin::Pin;

use akka_persistence_rs::{EntityType, Offset};
use async_trait::async_trait;
use tokio_stream::Stream;

/// Errors for event processing by a handler.
pub struct HandlerError;

/// Handle event envelopes in any way that an application requires.
#[async_trait]
pub trait Handler {
    /// The envelope processed by the handler.
    type Envelope;

    /// Process an envelope.
    async fn process(&self, envelope: Self::Envelope) -> Result<(), HandlerError>;
}

/// Errors for event processing by a handler.
pub struct SourceProviderError;

/// Provides a source of envelopes.
#[async_trait]
pub trait SourceProvider {
    /// The envelope processed by the provider.
    type Envelope;

    /// Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
    /// evenly distribute all persistence ids over the slices.
    ///
    /// The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
    /// query from a given `offset` after a crash/restart.
    ///
    /// The exact meaning of the `offset` depends on the journal and must be documented by it. It may
    /// be a sequential id number that uniquely identifies the position of each event within the event stream. Distributed
    /// data stores cannot easily support those semantics and they may use a weaker meaning. For example it may be a
    /// timestamp (taken when the event was created or stored). Timestamps are not unique and not strictly ordered, since
    /// clocks on different machines may not be synchronized.
    ///
    /// In strongly consistent stores, where the `offset` is unique and strictly ordered, the stream should start from the
    /// next event after the `offset`. Otherwise, the read journal should ensure that between an invocation that returned
    /// an event with the given `offset`, and this invocation, no events are missed. Depending on the journal
    /// implementation, this may mean that this invocation will return events that were already returned by the previous
    /// invocation, including the event with the passed in `offset`.
    ///
    /// The returned event stream should be ordered by `offset` if possible, but this can also be difficult to fulfill for
    /// a distributed data store. The order must be documented by the journal implementation.
    ///
    /// The stream is not completed when it reaches the end of the currently stored events, but it continues to push new
    /// events when new events are persisted.
    async fn events_by_slices(
        &self,
        entity_type: EntityType,
        min_slice: u32,
        max_slice: u32,
        offset: Option<Offset>,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>;
}
