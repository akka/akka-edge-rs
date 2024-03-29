#![doc = include_str!("../README.md")]

use std::{future::Future, marker::PhantomData, pin::Pin};

use akka_persistence_rs::PersistenceId;
use async_trait::async_trait;
use offset_store::LastOffset;
use tokio_stream::Stream;

pub mod consumer;
pub mod consumer_filter;
pub mod offset_store;
pub mod volatile_offset_store;

/// Captures the various types of handlers and the way they are performed.
pub enum Handlers<A, B>
where
    A: Handler,
    B: PendingHandler,
{
    Ready(A, B),
    Pending(B, A),
}

impl<A, E> From<A> for Handlers<A, UnusedPendingHandler<E>>
where
    A: Handler,
    E: Send,
{
    fn from(handler: A) -> Self {
        Handlers::Ready(
            handler,
            UnusedPendingHandler {
                phantom: PhantomData,
            },
        )
    }
}

pub struct FnHandler<A, AR, E>
where
    A: FnMut(E) -> AR,
    AR: Future<Output = Result<(), HandlerError>>,
{
    f: A,
    phantom: PhantomData<E>,
}

#[async_trait]
impl<A, AR, E> Handler for FnHandler<A, AR, E>
where
    A: FnMut(E) -> AR + Send,
    AR: Future<Output = Result<(), HandlerError>> + Send,
    E: Send,
{
    type Envelope = E;

    async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        (self.f)(envelope).await
    }
}

impl<A, AR, E> From<A> for Handlers<FnHandler<A, AR, E>, UnusedPendingHandler<E>>
where
    A: FnMut(E) -> AR + Send,
    AR: Future<Output = Result<(), HandlerError>> + Send,
    E: Send,
{
    fn from(handler: A) -> Self {
        Handlers::Ready(
            FnHandler {
                f: handler,
                phantom: PhantomData,
            },
            UnusedPendingHandler {
                phantom: PhantomData,
            },
        )
    }
}

impl<B, E> From<B> for Handlers<UnusedHandler<E>, B>
where
    B: PendingHandler,
    E: Send,
{
    fn from(handler: B) -> Self {
        Handlers::Pending(
            handler,
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

    /// Process an envelope.
    /// A handler's result is "completed" where envelopes are processed upon the previous one
    /// having been processed successfully.
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
pub trait PendingHandler {
    /// The envelope processed by the handler.
    type Envelope: Send;

    /// The maximum number of envelopes that can be pending at any time.
    const MAX_PENDING: usize;

    /// Process an envelope with a pending result.
    /// A handler's result is "pending" when envelopes can be passed through and the
    /// result of processing one is not immediately known. Meanwhile, more
    /// envelopes can be passed though.
    async fn process_pending(
        &mut self,
        envelope: Self::Envelope,
    ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError>;
}

/// For the purposes of constructing unused handlers.
pub struct UnusedPendingHandler<E> {
    pub phantom: PhantomData<E>,
}

#[async_trait]
impl<E> PendingHandler for UnusedPendingHandler<E>
where
    E: Send,
{
    type Envelope = E;

    const MAX_PENDING: usize = 0;

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
        &self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<LastOffset>> + Send;

    /// Load a single event on demand. A None is returned if the event for the
    /// given `persistence_id` and `seq_nr` doesn't exist.
    async fn load_envelope(
        &self,
        persistence_id: PersistenceId,
        seq_nr: u64,
    ) -> Option<Self::Envelope>;
}
