//! Effects that are lazily performed as a result of performing a command
//! of an entity. Effects can be chained with other effects and are guaranteed
//! to be applied (run) before the next command for an entity id is processed.
//!
//! Convience methods are providing for commonly chained operations, and take
//! the form of `and_then` as the prefix. By Rust convention, `and_then`
//! provides the result of the previous operation and expects a result provided
//! given some closure.
//!
//! In the case where there is no convenience method, a generalized `and`
//! operation can be used to chain any effect found here, or a customized
//! effect.

use async_trait::async_trait;
use chrono::Utc;
use std::result::Result as StdResult;
use std::{future::Future, io, marker::PhantomData};
use tokio::sync::oneshot;

use crate::{
    entity::EventSourcedBehavior,
    entity_manager::{EntityOps, EventEnvelope, Handler},
    EntityId,
};

/// Errors that can occur when applying effects.
#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
}

pub type Result = StdResult<(), Error>;

/// The trait that effect types implement.
#[async_trait]
pub trait Effect<B>: Send
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    /// Consume the effect asynchronously. This operation may
    /// be performed multiple times, but only the first time
    /// is expected to perform the effect.
    #[allow(clippy::too_many_arguments)]
    async fn process(
        &mut self,
        behavior: &B,
        handler: &mut (dyn Handler<B::Event> + Send),
        entities: &mut (dyn EntityOps<B> + Send + Sync),
        entity_id: &EntityId,
        last_seq_nr: &mut u64,
        prev_result: Result,
    ) -> Result;
}

/// The return type of [EffectExt::and].
pub struct And<E, L, R> {
    _l: L,
    _r: R,
    phantom: PhantomData<E>,
}

#[async_trait]
impl<B, L, R> Effect<B> for And<B, L, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send,
    L: Effect<B>,
    R: Effect<B>,
{
    async fn process(
        &mut self,
        behavior: &B,
        handler: &mut (dyn Handler<B::Event> + Send),
        entities: &mut (dyn EntityOps<B> + Send + Sync),
        entity_id: &EntityId,
        last_seq_nr: &mut u64,
        prev_result: Result,
    ) -> Result {
        let r = self
            ._l
            .process(
                behavior,
                handler,
                entities,
                entity_id,
                last_seq_nr,
                prev_result,
            )
            .await;
        if r.is_ok() {
            self._r
                .process(behavior, handler, entities, entity_id, last_seq_nr, r)
                .await
        } else {
            r
        }
    }
}

impl<B, L, R> EffectExt<B> for And<B, L, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send,
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

    /// Perform a side effect to run a function asynchronously after this current one.
    /// The associated behavior is available so that communication channels, for
    /// example, can be accessed by the side-effect. Additionally, the
    /// latest state given any previous effect having emitted an event,
    /// or else the state at the outset of the effects being applied,
    /// is also available.
    fn and_then<F, R>(self, f: F) -> And<B, Self, Then<B, F, R>>
    where
        Self: Sized,
        B::State: Send + Sync,
        F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
        R: Future<Output = Result>,
    {
        And {
            _l: self,
            _r: Then {
                f: Some(f),
                phantom: PhantomData,
            },
            phantom: PhantomData,
        }
    }

    /// An effect to emit an event. The latest state given any previous effect
    /// having emitted an event, or else the state at the outset of the effects
    /// being applied, is also available.
    fn and_then_emit_event<F, R>(self, f: F) -> And<B, Self, ThenEmitEvent<B, F, R>>
    where
        Self: Sized,
        B::State: Send + Sync,
        F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
        R: Future<Output = StdResult<Option<B::Event>, Error>> + Send,
    {
        And {
            _l: self,
            _r: ThenEmitEvent {
                deletion_event: false,
                f: Some(f),
                phantom: PhantomData,
            },
            phantom: PhantomData,
        }
    }

    /// An effect to emit a deletion event. The latest state given any previous effect
    /// having emitted an event, or else the state at the outset of the effects
    /// being applied, is also available.
    fn and_then_emit_deletion_event<F, R>(self, f: F) -> And<B, Self, ThenEmitEvent<B, F, R>>
    where
        Self: Sized,
        B::State: Send + Sync,
        F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
        R: Future<Output = StdResult<Option<B::Event>, Error>> + Send,
    {
        And {
            _l: self,
            _r: ThenEmitEvent {
                deletion_event: true,
                f: Some(f),
                phantom: PhantomData,
            },
            phantom: PhantomData,
        }
    }

    /// An effect to reply an envelope. The latest state given any previous effect
    /// having emitted an event, or else the state at the outset of the effects
    /// being applied, is also available.
    fn and_then_reply<F, R, T>(self, f: F) -> And<B, Self, ThenReply<B, F, R, T>>
    where
        Self: Sized,
        B::State: Send + Sync,
        F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
        R: Future<Output = StdResult<Option<(oneshot::Sender<T>, T)>, Error>> + Send,
        T: Send,
    {
        And {
            _l: self,
            _r: ThenReply {
                f: Some(f),
                phantom: PhantomData,
            },
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

/// The return type of [emit_event] and [emit_deletion_event].
pub struct EmitEvent<B>
where
    B: EventSourcedBehavior,
{
    deletion_event: bool,
    event: Option<B::Event>,
}

#[async_trait]
impl<B> Effect<B> for EmitEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send,
    B::Event: Send,
{
    async fn process(
        &mut self,
        _behavior: &B,
        handler: &mut (dyn Handler<B::Event> + Send),
        entities: &mut (dyn EntityOps<B> + Send + Sync),
        entity_id: &EntityId,
        last_seq_nr: &mut u64,
        prev_result: Result,
    ) -> Result {
        if prev_result.is_ok() {
            if let Some(event) = self.event.take() {
                let seq_nr = last_seq_nr.wrapping_add(1);
                let envelope = EventEnvelope {
                    entity_id: entity_id.clone(),
                    seq_nr,
                    timestamp: Utc::now(),
                    event,
                    deletion_event: self.deletion_event,
                };
                let result = handler.process(envelope).await.map_err(Error::IoError);
                if let Ok(envelope) = result {
                    *last_seq_nr = entities.update(envelope);
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
    B::State: Send,
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

/// The return type of [reply].
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
        _handler: &mut (dyn Handler<B::Event> + Send),
        _entities: &mut (dyn EntityOps<B> + Send + Sync),
        _entity_id: &EntityId,
        _last_seq_nr: &mut u64,
        prev_result: Result,
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

/// An effect to reply an envelope if the previous effect completed
/// successfully. Note that if state from having emitted an event
/// via a prior effect is required, then use a [then] effect instead.
pub fn reply<B, T>(reply_to: oneshot::Sender<T>, reply: T) -> Reply<B, T> {
    Reply {
        replier: Some((reply_to, reply)),
        phantom: PhantomData,
    }
}

/// The return type of [then].
pub struct Then<B, F, R> {
    f: Option<F>,
    phantom: PhantomData<(B, R)>,
}

#[async_trait]
impl<B, F, R> Effect<B> for Then<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = Result> + Send,
{
    async fn process(
        &mut self,
        behavior: &B,
        _handler: &mut (dyn Handler<B::Event> + Send),
        entities: &mut (dyn EntityOps<B> + Send + Sync),
        entity_id: &EntityId,
        _last_seq_nr: &mut u64,
        prev_result: Result,
    ) -> Result {
        let f = self.f.take();
        if let Some(f) = f {
            f(behavior, entities.get(entity_id), prev_result).await
        } else {
            Ok(())
        }
    }
}

impl<B, F, R> EffectExt<B> for Then<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = Result> + Send,
{
}

/// A side effect to run a function asynchronously. The associated
/// behavior is available so that communication channels, for
/// example, can be accessed by the side-effect. Additionally, the
/// latest state given any previous effect having emitted an event,
/// or else the state at the outset of the effects being applied,
/// is also available.
pub fn then<B, F, R>(f: F) -> Then<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = Result>,
{
    Then {
        f: Some(f),
        phantom: PhantomData,
    }
}

/// The return type of [EffectExt::and_then_emit_event] and  [EffectExt::and_then_emit_deletion_event].
pub struct ThenEmitEvent<B, F, R>
where
    B: EventSourcedBehavior,
{
    deletion_event: bool,
    f: Option<F>,
    phantom: PhantomData<(B, R)>,
}

#[async_trait]
impl<B, F, R> Effect<B> for ThenEmitEvent<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    B::Event: Send,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = StdResult<Option<B::Event>, Error>> + Send,
{
    async fn process(
        &mut self,
        behavior: &B,
        handler: &mut (dyn Handler<B::Event> + Send),
        entities: &mut (dyn EntityOps<B> + Send + Sync),
        entity_id: &EntityId,
        last_seq_nr: &mut u64,
        prev_result: Result,
    ) -> Result {
        let f = self.f.take();
        if let Some(f) = f {
            let result = f(behavior, entities.get(entity_id), prev_result).await;
            if let Ok(event) = result {
                let mut effect = EmitEvent::<B> {
                    deletion_event: self.deletion_event,
                    event,
                };
                effect
                    .process(behavior, handler, entities, entity_id, last_seq_nr, Ok(()))
                    .await
            } else {
                result.map(|_| ())
            }
        } else {
            Ok(())
        }
    }
}

impl<B, F, R> EffectExt<B> for ThenEmitEvent<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    B::Event: Send,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = StdResult<Option<B::Event>, Error>> + Send,
{
}

/// The return type of [EffectExt::and_then_reply].
pub struct ThenReply<B, F, R, T> {
    f: Option<F>,
    phantom: PhantomData<(B, R, T)>,
}

#[async_trait]
impl<B, F, R, T> Effect<B> for ThenReply<B, F, R, T>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = StdResult<Option<(oneshot::Sender<T>, T)>, Error>> + Send,
    T: Send,
{
    async fn process(
        &mut self,
        behavior: &B,
        handler: &mut (dyn Handler<B::Event> + Send),
        entities: &mut (dyn EntityOps<B> + Send + Sync),
        entity_id: &EntityId,
        last_seq_nr: &mut u64,
        prev_result: Result,
    ) -> Result {
        let f = self.f.take();
        if let Some(f) = f {
            let result = f(behavior, entities.get(entity_id), prev_result).await;
            if let Ok(replier) = result {
                let mut effect = Reply {
                    replier,
                    phantom: PhantomData,
                };
                effect
                    .process(behavior, handler, entities, entity_id, last_seq_nr, Ok(()))
                    .await
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

impl<B, F, R, T> EffectExt<B> for ThenReply<B, F, R, T>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = StdResult<Option<(oneshot::Sender<T>, T)>, Error>> + Send,
    T: Send,
{
}

/// The return type of [unhandled].
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
        _handler: &mut (dyn Handler<B::Event> + Send),
        _entities: &mut (dyn EntityOps<B> + Send + Sync),
        _entity_id: &EntityId,
        _last_seq_nr: &mut u64,
        _prev_result: Result,
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
