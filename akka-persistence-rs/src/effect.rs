//! Effects express how the state of an entity is to be updated, and how various side-effects
//! can be performed; all in response to an entity's commands. Effects can be chained with other effects and are guaranteed
//! to be applied (run) before the next command for an entity id is processed.
//!
//! Effects are applied
//! on being returned from a command handler. Command handlers are "pure" and deliberately constrained
//! from performing side effects. Using effects this way helps us reason the code of a command handler, and facilitate testing
//! outside of running an entity manager.
//!
//! An example as used within a command handler:
//!
//! ```no_run
//! use akka_persistence_rs::effect::{EffectExt, persist_event};
//!
//! // ...
//! # struct SomeEvent;
//! # let (reply_to, _) = tokio::sync::oneshot::channel();
//! # let reply_value = 1;
//! # struct MyBehavior;
//! # let persist_event = persist_event::<MyBehavior>;
//! # impl akka_persistence_rs::entity::EventSourcedBehavior for MyBehavior {
//! # type State = ();
//! # type Command = ();
//! # type Event = SomeEvent;
//! # fn for_command(_: &akka_persistence_rs::entity::Context, _: &Self::State, _: Self::Command) -> Box<dyn akka_persistence_rs::effect::Effect<Self>> {todo!()}
//! # fn on_event(_: &akka_persistence_rs::entity::Context, _: &mut Self::State, _: Self::Event) {todo!()}
//! # }
//!
//! persist_event(SomeEvent)
//!     .then_reply(move |_s| Some((reply_to, reply_value)))
//!     .boxed();
//! ```
//!
//! Convenience methods are provided for commonly chained operations and often provide any state that
//! has been updated by a previous operation.
//! These conveniences take the form of `and_then` and `then` as the prefix. By Rust convention, `and_then`
//! provides the result of the previous operation and expects a result provided
//! given some closure. Also by convention, `then` is applied only when the previous operation
//! completed successfully.
//!
//! The [EffectExt::boxed] method is a convenience for "boxing" the chain of effects into a concrete
//! type that can be returned from a command handler. For more information on boxing, please refer
//! to [the Rust documentation](https://doc.rust-lang.org/std/boxed/index.html).
//!
//! Custom effects may also be provided by implementing the [Effect] and [EffectExt]
//! traits. See the [Reply] type to illustrate how.
//!
//! In the case where there is no convenience method, such as with custom ones, a generalized `and`
//! operation can be used to chain any effect. Using the previous `then_reply` example:
//!
//! ```no_run
//! use akka_persistence_rs::effect::{EffectExt, reply, persist_event};
//!
//! // ...
//! # struct SomeEvent;
//! # let (reply_to, _) = tokio::sync::oneshot::channel();
//! # let reply_value = 1;
//! # struct MyBehavior;
//! # let persist_event = persist_event::<MyBehavior>;
//! # impl akka_persistence_rs::entity::EventSourcedBehavior for MyBehavior {
//! # type State = ();
//! # type Command = ();
//! # type Event = SomeEvent;
//! # fn for_command(_: &akka_persistence_rs::entity::Context, _: &Self::State, _: Self::Command) -> Box<dyn akka_persistence_rs::effect::Effect<Self>> {todo!()}
//! # fn on_event(_: &akka_persistence_rs::entity::Context, _: &mut Self::State, _: Self::Event) {todo!()}
//! # }
//!
//! persist_event(SomeEvent)
//!     .and(reply(reply_to, reply_value))
//!     .boxed();
//! ```

use async_trait::async_trait;
use chrono::Utc;
use std::future::{self, Ready};
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
        self._r
            .process(behavior, handler, entities, entity_id, last_seq_nr, r)
            .await
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
    /// latest state given any previous effect having persisted an event,
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

    /// An effect to persist an event. The latest state given any previous effect
    /// having persisted an event, or else the state at the outset of the effects
    /// being applied, is also available.
    ///
    /// Only applied when the previous result succeeded.
    #[allow(clippy::type_complexity)]
    fn then_persist_event<F>(
        self,
        f: F,
    ) -> And<
        B,
        Self,
        ThenPersistEvent<
            B,
            Box<
                dyn FnOnce(
                        &B,
                        Option<&B::State>,
                        Result,
                    ) -> Ready<StdResult<Option<B::Event>, Error>>
                    + Send,
            >,
            Ready<StdResult<Option<B::Event>, Error>>,
        >,
    >
    where
        Self: Sized,
        B::State: Send + Sync,
        F: FnOnce(Option<&B::State>) -> Option<B::Event> + Send + 'static,
    {
        let f = Box::new(|_b: &B, s: Option<&B::State>, r: Result| {
            let r = if let Err(e) = r { Err(e) } else { Ok(f(s)) };
            future::ready(r)
        });
        And {
            _l: self,
            _r: ThenPersistEvent {
                deletion_event: false,
                f: Some(f),
                phantom: PhantomData,
            },
            phantom: PhantomData,
        }
    }

    /// An effect to reply an envelope. The latest state given any previous effect
    /// having persisted an event, or else the state at the outset of the effects
    /// being applied, is also available.
    ///
    /// Only applied when the previous result succeeded.
    #[allow(clippy::type_complexity)]
    fn then_reply<F, T>(
        self,
        f: F,
    ) -> And<
        B,
        Self,
        ThenReply<
            B,
            Box<dyn FnOnce(&B, Option<&B::State>, Result) -> Ready<ReplyResult<T>> + Send>,
            Ready<ReplyResult<T>>,
            T,
        >,
    >
    where
        Self: Sized,
        B::State: Send + Sync,
        F: FnOnce(Option<&B::State>) -> Option<ReplyTo<T>> + Send + 'static,
        T: Send,
    {
        let f = Box::new(|_b: &B, s: Option<&B::State>, r: Result| {
            let r = if let Err(e) = r { Err(e) } else { Ok(f(s)) };
            future::ready(r)
        });
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

/// The return type of [persist_event] and [persist_deletion_event].
pub struct PersistEvent<B>
where
    B: EventSourcedBehavior,
{
    deletion_event: bool,
    event: Option<B::Event>,
}

#[async_trait]
impl<B> Effect<B> for PersistEvent<B>
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

impl<B> EffectExt<B> for PersistEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send,
    B::Event: Send,
{
}

/// An effect to persist an event.
pub fn persist_event<B>(event: B::Event) -> PersistEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    PersistEvent {
        deletion_event: false,
        event: Some(event),
    }
}

/// An effect to persist an event upon having successfully handed it off to
/// be persisted. The event will be flagged to represent the deletion of
/// an entity instance.
pub fn persist_deletion_event<B>(event: B::Event) -> PersistEvent<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    PersistEvent {
        deletion_event: true,
        event: Some(event),
    }
}

/// The reply-to [oneshot::Sender] and the value to send as a result of some
/// tuple.
pub type ReplyTo<T> = (oneshot::Sender<T>, T);

/// A result type for [ReplyTo] which is used to reply if wrapped with [Some].
pub type ReplyResult<T> = StdResult<Option<ReplyTo<T>>, Error>;

/// The return type of [reply].
pub struct Reply<B, T> {
    replier: Option<ReplyTo<T>>,
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
/// successfully. Note that if state from having persisted an event
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
/// latest state given any previous effect having persisted an event,
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

/// The return type of [EffectExt::then_persist_event].
pub struct ThenPersistEvent<B, F, R>
where
    B: EventSourcedBehavior,
{
    deletion_event: bool,
    f: Option<F>,
    phantom: PhantomData<(B, R)>,
}

#[async_trait]
impl<B, F, R> Effect<B> for ThenPersistEvent<B, F, R>
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
                let mut effect = PersistEvent::<B> {
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

impl<B, F, R> EffectExt<B> for ThenPersistEvent<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    B::Event: Send,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = StdResult<Option<B::Event>, Error>> + Send,
{
}

/// A side effect to run a function asynchronously and then, if ok,
/// persist an event. The associated
/// behavior is available so that communication channels, for
/// example, can be accessed by the side-effect. Additionally, the
/// latest state given any previous effect having persisted an event,
/// or else the state at the outset of the effects being applied,
/// is also available.
pub fn persist_event_if<B, F, R>(f: F) -> ThenPersistEvent<B, F, R>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Send + Sync,
    F: FnOnce(&B, Option<&B::State>, Result) -> R + Send,
    R: Future<Output = StdResult<Option<B::Event>, Error>> + Send,
    <B as EventSourcedBehavior>::Event: Send,
{
    ThenPersistEvent {
        deletion_event: false,
        f: Some(f),
        phantom: PhantomData,
    }
}

/// The return type of [EffectExt::then_reply].
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
    R: Future<Output = ReplyResult<T>> + Send,
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
    R: Future<Output = ReplyResult<T>> + Send,
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

#[cfg(test)]
mod tests {

    use super::*;

    use crate::entity::Context;
    use test_log::test;

    #[derive(Default)]
    struct TestState;

    struct TestCommand;

    #[derive(Copy, Clone, Debug, PartialEq)]
    struct TestEvent;

    struct TestBehavior;

    impl EventSourcedBehavior for TestBehavior {
        type State = TestState;
        type Command = TestCommand;
        type Event = TestEvent;

        fn for_command(
            _context: &Context,
            _state: &Self::State,
            _command: Self::Command,
        ) -> Box<dyn Effect<Self>> {
            todo!()
        }

        fn on_event(_context: &Context, _state: &mut Self::State, _event: Self::Event) {
            todo!()
        }
    }

    struct TestHandler {
        expected: EventEnvelope<TestEvent>,
    }

    #[async_trait]
    impl Handler<TestEvent> for TestHandler {
        async fn process(
            &mut self,
            envelope: EventEnvelope<TestEvent>,
        ) -> io::Result<EventEnvelope<TestEvent>> {
            assert_eq!(envelope.deletion_event, self.expected.deletion_event);
            assert_eq!(envelope.entity_id, self.expected.entity_id);
            assert_eq!(envelope.seq_nr, self.expected.seq_nr);
            assert_eq!(envelope.event, self.expected.event);
            Ok(envelope)
        }
    }

    struct TestEntityOps {
        expected_get_entity_id: EntityId,
        get_result: TestState,
        expected_update: EventEnvelope<TestEvent>,
    }

    impl EntityOps<TestBehavior> for TestEntityOps {
        fn get(&mut self, entity_id: &EntityId) -> Option<&TestState> {
            assert_eq!(entity_id, &self.expected_get_entity_id);
            Some(&self.get_result)
        }

        fn update(&mut self, envelope: EventEnvelope<TestEvent>) -> u64 {
            assert_eq!(envelope.deletion_event, self.expected_update.deletion_event);
            assert_eq!(envelope.entity_id, self.expected_update.entity_id);
            assert_eq!(envelope.seq_nr, self.expected_update.seq_nr);
            assert_eq!(envelope.event, self.expected_update.event);
            envelope.seq_nr
        }
    }

    #[test(tokio::test)]
    async fn test_persist_then_reply() {
        let entity_id = EntityId::from("entity-id");
        let expected = EventEnvelope {
            deletion_event: false,
            entity_id: entity_id.clone(),
            seq_nr: 1,
            event: TestEvent,
            timestamp: Utc::now(),
        };
        let mut handler = TestHandler {
            expected: expected.clone(),
        };
        let mut entity_ops = TestEntityOps {
            expected_get_entity_id: entity_id.clone(),
            get_result: TestState,
            expected_update: expected,
        };
        let (reply_to, reply_to_receiver) = oneshot::channel();
        let reply_value = 1;

        assert!(persist_event(TestEvent)
            .then_reply(move |_s| Some((reply_to, reply_value)))
            .process(
                &TestBehavior,
                &mut handler,
                &mut entity_ops,
                &entity_id,
                &mut 0,
                Ok(()),
            )
            .await
            .is_ok());

        assert_eq!(reply_to_receiver.await, Ok(reply_value));
    }

    #[test(tokio::test)]
    async fn test_reply_then_persist() {
        let entity_id = EntityId::from("entity-id");
        let expected = EventEnvelope {
            deletion_event: false,
            entity_id: entity_id.clone(),
            seq_nr: 1,
            event: TestEvent,
            timestamp: Utc::now(),
        };
        let mut handler = TestHandler {
            expected: expected.clone(),
        };
        let mut entity_ops = TestEntityOps {
            expected_get_entity_id: entity_id.clone(),
            get_result: TestState,
            expected_update: expected,
        };
        let (reply_to, reply_to_receiver) = oneshot::channel();
        let reply_value = 1;

        assert!(reply(reply_to, reply_value)
            .then_persist_event(|_s| Some(TestEvent))
            .process(
                &TestBehavior,
                &mut handler,
                &mut entity_ops,
                &entity_id,
                &mut 0,
                Ok(()),
            )
            .await
            .is_ok());

        assert_eq!(reply_to_receiver.await, Ok(reply_value));
    }

    #[test(tokio::test)]
    async fn test_persist_event_if() {
        let entity_id = EntityId::from("entity-id");
        let expected = EventEnvelope {
            deletion_event: false,
            entity_id: entity_id.clone(),
            seq_nr: 1,
            event: TestEvent,
            timestamp: Utc::now(),
        };
        let mut handler = TestHandler {
            expected: expected.clone(),
        };
        let mut entity_ops = TestEntityOps {
            expected_get_entity_id: entity_id.clone(),
            get_result: TestState,
            expected_update: expected,
        };

        assert!(persist_event_if(|_b, _, _| async { Ok(Some(TestEvent)) })
            .process(
                &TestBehavior,
                &mut handler,
                &mut entity_ops,
                &entity_id,
                &mut 0,
                Ok(()),
            )
            .await
            .is_ok());
    }
}
