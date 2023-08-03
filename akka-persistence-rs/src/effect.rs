//! Effects that are lazily performed as a result of performing a command
//! of an entity. Effects can be chained with other effects.

use std::{future::Future, io, marker::PhantomData};

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::{EntityId, Record, RecordFlow};

/// Errors that can occur when applying effects.
pub enum Error {
    IoError(io::Error),
}

pub type Result<E> = std::result::Result<Option<Record<E>>, Error>;

/// The trait that effect types implement.
#[async_trait]
pub trait Effect<E: Send>: Send {
    /// Consume the effect asynchronously. This operation may
    /// be performed multiple times, but only the first time
    /// is expected to perform the effect.
    async fn take(
        &mut self,
        flow: &mut (dyn RecordFlow<E> + Send + Sync),
        entity_id: EntityId,
        prev_result: Result<E>,
    ) -> Result<E>;
}

/// An effect to chain one effect with another.
pub struct And<E, L, R> {
    _l: L,
    _r: R,
    phantom: PhantomData<E>,
}

#[async_trait]
impl<E, L, R> Effect<E> for And<E, L, R>
where
    E: Send + Sync,
    L: Effect<E> + Sync,
    R: Effect<E> + Sync,
{
    async fn take(
        &mut self,
        flow: &mut (dyn RecordFlow<E> + Send + Sync),
        entity_id: EntityId,
        prev_result: Result<E>,
    ) -> Result<E> {
        let r = self._l.take(flow, entity_id.clone(), prev_result).await;
        if r.is_ok() {
            self._r.take(flow, entity_id, r).await
        } else {
            r
        }
    }
}

impl<E, L, R> EffectExt<E> for And<E, L, R>
where
    E: Send + Sync,
    L: Effect<E> + Sync,
    R: Effect<E> + Sync,
{
}

/// Combinators for use with effects.
pub trait EffectExt<E>: Effect<E>
where
    E: Send + Sync,
{
    /// Perform the provided effect after this current one.
    fn and<R>(self, r: R) -> And<E, Self, R>
    where
        Self: Sized + Sync,
        R: Effect<E> + Sync,
    {
        And {
            _l: self,
            _r: r,
            phantom: PhantomData,
        }
    }

    /// Box the effect for the purposes of returning it.
    fn boxed(self) -> Box<dyn Effect<E>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

// EmitEvent

pub struct EmitEvent<E> {
    event: Option<E>,
}

#[async_trait]
impl<E> Effect<E> for EmitEvent<E>
where
    E: Send + Sync,
{
    async fn take(
        &mut self,
        flow: &mut (dyn RecordFlow<E> + Send + Sync),
        entity_id: EntityId,
        prev_result: Result<E>,
    ) -> Result<E> {
        if prev_result.is_ok() {
            if let Some(event) = self.event.take() {
                let record = Record::new(entity_id, event);
                flow.process(record).await.map(Some).map_err(Error::IoError)
            } else {
                prev_result
            }
        } else {
            prev_result
        }
    }
}

impl<E> EffectExt<E> for EmitEvent<E> where E: Send + Sync {}

/// An effect to emit an event upon having successfully handed it off to
/// be persisted.
pub fn emit_event<E>(event: E) -> EmitEvent<E> {
    EmitEvent { event: Some(event) }
}

// Reply

pub struct Reply<E, T> {
    replier: Option<(oneshot::Sender<T>, T)>,
    phantom: PhantomData<E>,
}

#[async_trait]
impl<E, T> Effect<E> for Reply<E, T>
where
    E: Send + Sync,
    T: Send,
{
    async fn take(
        &mut self,
        _flow: &mut (dyn RecordFlow<E> + Send + Sync),
        _entity_id: EntityId,
        prev_result: Result<E>,
    ) -> Result<E> {
        if prev_result.is_ok() {
            if let Some((reply_to, reply)) = self.replier.take() {
                // Reply is best-effort
                let _ = reply_to.send(reply);
            }
        }
        prev_result
    }
}

impl<E, T> EffectExt<E> for Reply<E, T>
where
    E: Send + Sync,
    T: Send,
{
}

/// An effect to reply a record.
pub fn reply<E, T>(reply_to: oneshot::Sender<T>, reply: T) -> Reply<E, T> {
    Reply {
        replier: Some((reply_to, reply)),
        phantom: PhantomData,
    }
}

// Then

pub struct Then<E, F, R> {
    f: Option<F>,
    phantom: PhantomData<(E, R)>,
}

#[async_trait]
impl<E, F, R> Effect<E> for Then<E, F, R>
where
    E: Send + Sync,
    F: FnOnce(Result<E>) -> R + Send + Sync,
    R: Future<Output = Result<E>> + Send,
{
    async fn take(
        &mut self,
        _flow: &mut (dyn RecordFlow<E> + Send + Sync),
        _entity_id: EntityId,
        prev_result: Result<E>,
    ) -> Result<E> {
        let f = self.f.take();
        if let Some(f) = f {
            f(prev_result).await
        } else {
            Ok(None)
        }
    }
}

impl<E, F, R> EffectExt<E> for Then<E, F, R>
where
    E: Send + Sync,
    F: FnOnce(Result<E>) -> R + Send + Sync,
    R: Future<Output = Result<E>> + Send,
{
}

/// An effect to run a function asynchronously.
pub fn then<E, F, R>(f: F) -> Then<E, F, R>
where
    F: FnOnce(Result<E>) -> R + Send,
    R: Future<Output = Result<E>>,
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
impl<E> Effect<E> for Unhandled<E>
where
    E: Send + Sync,
{
    async fn take(
        &mut self,
        _flow: &mut (dyn RecordFlow<E> + Send + Sync),
        _entity_id: EntityId,
        _prev_result: Result<E>,
    ) -> Result<E> {
        Ok(None)
    }
}

/// An unhandled command producing no effect
pub fn unhandled<E>() -> Box<Unhandled<E>> {
    Box::new(Unhandled {
        phantom: PhantomData,
    })
}
