//! Effects that are lazily performed as a result of performing a command
//! of an entity. Effects can be chained with other effects.

use std::{future::Future, io, marker::PhantomData};

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::{entity::EventSourcedBehavior, EntityId, Record, RecordFlow};

/// Errors that can occur when applying effects.
pub enum Error {
    IoError(io::Error),
}

pub type Result = std::result::Result<(), Error>;

/// The trait that effect types implement.
#[async_trait]
pub trait Effect<B>: Send
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    /// Consume the effect asynchronously. This operation may
    /// be performed multiple times, but only the first time
    /// is expected to perform the effect.
    async fn process(
        &mut self,
        behavior: &B,
        flow: &mut (dyn RecordFlow<B::Event> + Send),
        entity_id: EntityId,
        prev_result: Result,
        update_entity: &mut (dyn FnMut(Record<B::Event>) + Send),
    ) -> Result;
}

/// An effect to chain one effect with another.
pub struct And<E, L, R> {
    _l: L,
    _r: R,
    phantom: PhantomData<E>,
}

#[async_trait]
impl<B, L, R> Effect<B> for And<B, L, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    L: Effect<B>,
    R: Effect<B>,
{
    async fn process(
        &mut self,
        behavior: &B,
        flow: &mut (dyn RecordFlow<B::Event> + Send),
        entity_id: EntityId,
        prev_result: Result,
        update_entity: &mut (dyn FnMut(Record<B::Event>) + Send),
    ) -> Result {
        let r = self
            ._l
            .process(
                behavior,
                flow,
                entity_id.clone(),
                prev_result,
                update_entity,
            )
            .await;
        if r.is_ok() {
            self._r
                .process(behavior, flow, entity_id, r, update_entity)
                .await
        } else {
            r
        }
    }
}

impl<B, L, R> EffectExt<B> for And<B, L, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    L: Effect<B>,
    R: Effect<B>,
{
}

/// Combinators for use with effects.
pub trait EffectExt<B>: Effect<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    /// Perform the provided effect after this current one.
    fn and<R>(self, r: R) -> And<B, Self, R>
    where
        Self: Sized,
        R: Effect<B>,
    {
        And {
            _l: self,
            _r: r,
            phantom: PhantomData,
        }
    }

    /// Box the effect for the purposes of returning it.
    fn boxed(self) -> Box<dyn Effect<B>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

// EmitEvent

pub struct EmitEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    deletion_event: bool,
    event: Option<B::Event>,
}

#[async_trait]
impl<B> Effect<B> for EmitEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::Event: Send,
{
    async fn process(
        &mut self,
        _behavior: &B,
        flow: &mut (dyn RecordFlow<B::Event> + Send),
        entity_id: EntityId,
        prev_result: Result,
        update_entity: &mut (dyn FnMut(Record<B::Event>) + Send),
    ) -> Result {
        if prev_result.is_ok() {
            if let Some(event) = self.event.take() {
                let record = Record {
                    entity_id,
                    event,
                    metadata: crate::RecordMetadata {
                        deletion_event: self.deletion_event,
                    },
                };
                let result = flow.process(record).await.map_err(Error::IoError);
                if let Ok(record) = result {
                    update_entity(record);
                    Ok(())
                } else {
                    result.map(|_| ())
                }
            } else {
                prev_result
            }
        } else {
            prev_result
        }
    }
}

impl<B> EffectExt<B> for EmitEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::Event: Send,
{
}

/// An effect to emit an event upon having successfully handed it off to
/// be persisted.
pub fn emit_event<B>(event: B::Event) -> EmitEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    EmitEvent {
        deletion_event: false,
        event: Some(event),
    }
}

// EmitDeletionEvent

/// An effect to emit an event upon having successfully handed it off to
/// be persisted. The event will be flagged to represent the deletion of
/// an entity instance.
pub fn emit_deletion_event<B>(event: B::Event) -> EmitEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    EmitEvent {
        deletion_event: true,
        event: Some(event),
    }
}

// Reply

pub struct Reply<B, T> {
    replier: Option<(oneshot::Sender<T>, T)>,
    phantom: PhantomData<B>,
}

#[async_trait]
impl<B, T> Effect<B> for Reply<B, T>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    T: Send,
{
    async fn process(
        &mut self,
        _behavior: &B,
        _flow: &mut (dyn RecordFlow<B::Event> + Send),
        _entity_id: EntityId,
        prev_result: Result,
        _update_entity: &mut (dyn FnMut(Record<B::Event>) + Send),
    ) -> Result {
        if prev_result.is_ok() {
            if let Some((reply_to, reply)) = self.replier.take() {
                // Reply is best-effort
                let _ = reply_to.send(reply);
            }
        }
        prev_result
    }
}

impl<B, T> EffectExt<B> for Reply<B, T>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    T: Send,
{
}

/// An effect to reply a record if the previous effect completed
/// successfully.
pub fn reply<B, T>(reply_to: oneshot::Sender<T>, reply: T) -> Reply<B, T> {
    Reply {
        replier: Some((reply_to, reply)),
        phantom: PhantomData,
    }
}

// Then

pub struct Then<B, F, R> {
    f: Option<F>,
    phantom: PhantomData<(B, R)>,
}

#[async_trait]
impl<B, F, R> Effect<B> for Then<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    F: FnOnce(&B, Result) -> R + Send,
    R: Future<Output = Result> + Send,
{
    async fn process(
        &mut self,
        behavior: &B,
        _flow: &mut (dyn RecordFlow<B::Event> + Send),
        _entity_id: EntityId,
        prev_result: Result,
        _update_entity: &mut (dyn FnMut(Record<B::Event>) + Send),
    ) -> Result {
        let f = self.f.take();
        if let Some(f) = f {
            f(behavior, prev_result).await
        } else {
            Ok(())
        }
    }
}

impl<B, F, R> EffectExt<B> for Then<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    F: FnOnce(&B, Result) -> R + Send,
    R: Future<Output = Result> + Send,
{
}

/// A side effect to run a function asynchronously. The associated
/// behavior is available so that communication channels, for
/// example, can be accessed by the side-effect.
pub fn then<B, F, R>(f: F) -> Then<B, F, R>
where
    F: FnOnce(&B, Result) -> R + Send,
    R: Future<Output = Result>,
{
    Then {
        f: Some(f),
        phantom: PhantomData,
    }
}

// Unhandled

pub struct Unhandled<E> {
    phantom: PhantomData<E>,
}

#[async_trait]
impl<B> Effect<B> for Unhandled<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    async fn process(
        &mut self,
        _behavior: &B,
        _flow: &mut (dyn RecordFlow<B::Event> + Send),
        _entity_id: EntityId,
        _prev_result: Result,
        _update_entity: &mut (dyn FnMut(Record<B::Event>) + Send),
    ) -> Result {
        Ok(())
    }
}

/// An unhandled command producing no effect
pub fn unhandled<B>() -> Box<Unhandled<B>> {
    Box::new(Unhandled {
        phantom: PhantomData,
    })
}
