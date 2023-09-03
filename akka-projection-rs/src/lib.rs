//! In Akka Projections you process a stream of event envelopes from a source to a projected model or external system.
//! Each envelope is associated with an offset representing the position in the stream. This offset is used for resuming
//! the stream from that position when the projection is restarted.

use std::{future::Future, marker::PhantomData, pin::Pin};

use akka_persistence_rs::Offset;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::Stream;

/// Captures the various types of handlers and the way they are performed.
pub enum Handlers<A, B>
where
    A: Handler,
    B: FlowingHandler,
{
    Sequential(A, B),
    Flowing(B, usize, A),
}

/// Convenience functions for creating handlers.
pub mod handlers {
    use super::*;

    /// A handler is sequential where envelopes are processed upon the previous one
    /// having been processed successfully.
    pub fn sequential<A, E>(handler: A) -> Handlers<A, UnusedFlowingHandler<E>>
    where
        A: Handler,
        E: Send,
    {
        Handlers::Sequential(
            handler,
            UnusedFlowingHandler {
                phantom: PhantomData,
            },
        )
    }

    /// A handler is "flowing" when envelopes can be passed through and the
    /// result of processing one is not immediately known. Meanwhile, more
    /// envelopes can be passed though. Flows have an upper limit on the
    /// number that may be in-flight at any one time.
    pub fn flowing<B, E>(handler: B, max_in_flight: usize) -> Handlers<UnusedHandler<E>, B>
    where
        B: FlowingHandler,
        E: Send,
    {
        Handlers::Flowing(
            handler,
            max_in_flight,
            UnusedHandler {
                phantom: PhantomData,
            },
        )
    }
}

/// Errors for event processing by a handler.
pub struct HandlerError;

/// Handle event envelopes in any way that an application requires.
#[async_trait]
pub trait Handler {
    /// The envelope processed by the handler.
    type Envelope: Send;

    /// Process an envelope. Implement when not overriding the `process_pending` method.
    async fn process(&mut self, _envelope: Self::Envelope) -> Result<(), HandlerError>;
}

/// For the purposes of constructing unused handlers.
pub struct UnusedHandler<E> {
    pub phantom: PhantomData<E>,
}

#[async_trait]
impl<E> Handler for UnusedHandler<E>
where
    E: Send,
{
    type Envelope = E;

    async fn process(&mut self, _envelope: Self::Envelope) -> Result<(), HandlerError> {
        Err(HandlerError)
    }
}

/// Handle event envelopes in any way that an application requires.
#[async_trait]
pub trait FlowingHandler {
    /// The envelope processed by the handler.
    type Envelope: Send;

    /// Process an envelope with a pending result.
    async fn process_pending(
        &mut self,
        envelope: Self::Envelope,
    ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError>;
}

/// For the purposes of constructing unused handlers.
pub struct UnusedFlowingHandler<E> {
    pub phantom: PhantomData<E>,
}

#[async_trait]
impl<E> FlowingHandler for UnusedFlowingHandler<E>
where
    E: Send,
{
    type Envelope = E;

    async fn process_pending(
        &mut self,
        _envelope: Self::Envelope,
    ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError> {
        Err(HandlerError)
    }
}

/// Errors for event processing by a handler.
pub struct SourceProviderError;

/// Provides a source of envelopes using slices as a query.
///
/// A slice is deterministically defined based on the persistence id. The purpose is to
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
#[async_trait]
pub trait SourceProvider {
    /// The envelope processed by the provider.
    type Envelope;

    /// Given a closure that returns an offset, source envelopes.
    async fn source<F, FR>(
        &mut self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<Offset>> + Send;
}

/// Provides a sink of envelopes where each envelope must
/// be processed before more envelopes are consumed.
#[async_trait]
pub trait SinkProvider {
    /// The envelope processed by the provider.
    type Envelope;

    /// Send envelopes along with a reply channel.
    async fn sink(&mut self, envelopes: mpsc::Receiver<(Self::Envelope, oneshot::Sender<()>)>);
}
