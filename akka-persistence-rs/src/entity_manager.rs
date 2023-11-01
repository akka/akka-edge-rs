//! An entity manager task handles the lifecycle and routing of messages for
//! an entity type. One EntityManager per entity type.
//! The EntityManager will generally instantiate the entities on demand, i.e. when first
//! message is sent to a specific entity. It will passivate least used
//! entities to have a bounded number of entities in memory.
//! The entities will recover their state from a stream of events.

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use log::debug;
use log::warn;
use lru::LruCache;
use std::hash::BuildHasher;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use tokio::sync::mpsc::Receiver;
use tokio_stream::{Stream, StreamExt};

use crate::entity::Context;
use crate::entity::EventSourcedBehavior;
use crate::{EntityId, Message};

/// An envelope wraps an event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    /// Flags whether the associated event is to be considered
    /// as one that represents an entity instance being deleted.
    pub deletion_event: bool,
    pub entity_id: EntityId,
    pub seq_nr: u64,
    pub event: E,
    pub timestamp: DateTime<Utc>,
}

impl<E> EventEnvelope<E> {
    pub fn new<EI>(entity_id: EI, seq_nr: u64, timestamp: DateTime<Utc>, event: E) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            deletion_event: false,
            entity_id: entity_id.into(),
            event,
            seq_nr,
            timestamp,
        }
    }
}

/// Sources events, in form of event envelopes, to another type of envelope e.g. those
/// that can be sourced using a storage technology.
#[async_trait]
pub trait SourceProvider<E> {
    /// Produce an initial source of events, which is called upon an entity
    /// manager task starting up. Any error from this method is considered fatal
    /// and will terminate the entity manager.
    async fn source_initial(
        &mut self,
    ) -> io::Result<Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send + 'async_trait>>>;

    /// Produce a source of events. An entity id
    /// is passed to the source method so that the source is
    /// discriminate regarding the entity events to supply.
    async fn source(
        &mut self,
        entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send + 'async_trait>>>;
}

/// Handles events, in form of event envelopes, to another type of envelope e.g. those
/// that can be persisted using a storage technology.
#[async_trait]
pub trait Handler<E> {
    /// Consume an envelope, performing some processing
    /// e.g. persisting an envelope, and then returning the same envelope
    /// if all went well.
    async fn process(&mut self, envelope: EventEnvelope<E>) -> io::Result<EventEnvelope<E>>;
}

// An opaque type internal to the library. The type records the public and private
// state of an entity in the context of the entity manager and friends.
#[derive(Default)]
struct EntityStatus<S> {
    pub(crate) state: S,
    pub(crate) last_seq_nr: u64,
}

/// An internal structure for the purposes of operating on the cache of entities.
pub trait EntityOps<B>
where
    B: EventSourcedBehavior,
{
    fn get(&mut self, entity_id: &EntityId) -> Option<&B::State>;
    fn update(&mut self, envelope: EventEnvelope<B::Event>) -> u64;
}

struct EntityLruCache<A, S>
where
    S: BuildHasher,
{
    cache: LruCache<EntityId, EntityStatus<A>, S>,
}

impl<B, S> EntityOps<B> for EntityLruCache<B::State, S>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Default,
    S: BuildHasher,
{
    fn get(&mut self, entity_id: &EntityId) -> Option<&<B as EventSourcedBehavior>::State> {
        self.cache.get(entity_id).map(|status| &status.state)
    }

    fn update(&mut self, envelope: EventEnvelope<<B as EventSourcedBehavior>::Event>) -> u64 {
        update_entity::<B, S>(&mut self.cache, envelope)
    }
}

/// Manages the lifecycle of entities given a specific behavior.
/// Entity managers are established given a source of events associated
/// with an entity type. That source is consumed by subsequently telling
/// the entity manager to run, generally on its own task.
///
/// Commands are sent to a channel established for the entity manager.
/// Effects may be produced as a result of performing a command, which may,
/// in turn, perform side effects and yield events.
pub async fn run<A, B>(
    behavior: B,
    adapter: A,
    receiver: Receiver<Message<B::Command>>,
    capacity: NonZeroUsize,
) -> io::Result<()>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::Command: Send,
    B::State: Send + Sync,
    A: SourceProvider<B::Event> + Handler<B::Event> + Send + 'static,
{
    run_with_hasher(
        behavior,
        adapter,
        receiver,
        capacity,
        lru::DefaultHasher::default(),
    )
    .await
}

/// Manages the lifecycle of entities given a specific behavior.
/// Entity managers are established given a source of events associated
/// with an entity type. That source is consumed by subsequently telling
/// the entity manager to run, generally on its own task.
///
/// Commands are sent to a channel established for the entity manager.
/// Effects may be produced as a result of performing a command, which may,
/// in turn, perform side effects and yield events.
///
/// A hasher for entity ids can also be supplied which will be used to control the
/// internal caching of entities.
pub async fn run_with_hasher<A, B, S>(
    behavior: B,
    mut adapter: A,
    mut receiver: Receiver<Message<B::Command>>,
    capacity: NonZeroUsize,
    hash_builder: S,
) -> io::Result<()>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::Command: Send,
    B::State: Send + Sync,
    A: SourceProvider<B::Event> + Handler<B::Event> + Send + 'static,
    S: BuildHasher + Send + Sync,
{
    // Source our initial events and populate our internal entities map.

    let mut entities = EntityLruCache {
        cache: LruCache::with_hasher(capacity, hash_builder),
    };

    let envelopes = adapter.source_initial().await?;

    {
        tokio::pin!(envelopes);
        while let Some(envelope) = envelopes.next().await {
            update_entity::<B, S>(&mut entities.cache, envelope);
        }
        for (entity_id, entity_status) in entities.cache.iter() {
            let context = Context { entity_id };
            behavior
                .on_recovery_completed(&context, &entity_status.state)
                .await;
        }
    }

    // Receive commands for the entities and process them.

    while let Some(message) = receiver.recv().await {
        // Source entity if we don't have it.

        let mut entity_status = entities.cache.get(&message.entity_id);

        if entity_status.is_none() {
            let envelopes = adapter.source(&message.entity_id).await?;

            tokio::pin!(envelopes);
            while let Some(envelope) = envelopes.next().await {
                update_entity::<B, S>(&mut entities.cache, envelope);
            }
            entity_status = entities.cache.get(&message.entity_id);
            let context = Context {
                entity_id: &message.entity_id,
            };
            behavior
                .on_recovery_completed(
                    &context,
                    &entity_status
                        .unwrap_or(&EntityStatus::<B::State>::default())
                        .state,
                )
                .await;
        }

        // Given an entity, send it the command, possibly producing an effect.
        // Effects may emit events that will update state on success.

        let context = Context {
            entity_id: &message.entity_id,
        };
        let (mut effect, mut last_seq_nr) = if let Some(entity_status) = entity_status {
            let effect = B::for_command(&context, &entity_status.state, message.command);
            let last_seq_nr = entity_status.last_seq_nr;
            (effect, last_seq_nr)
        } else {
            let entity_status = EntityStatus::<B::State>::default();
            let effect = B::for_command(&context, &entity_status.state, message.command);
            let last_seq_nr = entity_status.last_seq_nr;
            (effect, last_seq_nr)
        };
        let result = effect
            .process(
                &behavior,
                &mut adapter,
                &mut entities,
                context.entity_id,
                &mut last_seq_nr,
                Ok(()),
            )
            .await;
        if result.is_err() {
            warn!(
                "An error occurred when processing an effect for {}. Result: {result:?} Evicting it.",
                context.entity_id
            );
            entities.cache.pop(context.entity_id);
        }
    }

    Ok(())
}

fn update_entity<B, S>(
    entities: &mut LruCache<EntityId, EntityStatus<B::State>, S>,
    envelope: EventEnvelope<B::Event>,
) -> u64
where
    B: EventSourcedBehavior + Send + Sync + 'static,
    B::State: Default,
    S: BuildHasher,
{
    if !envelope.deletion_event {
        // Apply an event to state, creating the entity entry if necessary.
        let context = Context {
            entity_id: &envelope.entity_id,
        };
        let entity_state = if let Some(entity_state) = entities.get_mut(&envelope.entity_id) {
            entity_state.last_seq_nr = envelope.seq_nr;
            entity_state
        } else {
            debug!("Inserting new entity: {}", envelope.entity_id);

            // We're avoiding the use of get_or_insert so that we can avoid
            // cloning the entity id unless necessary.
            entities.push(
                envelope.entity_id.clone(),
                EntityStatus::<B::State> {
                    state: B::State::default(),
                    last_seq_nr: envelope.seq_nr,
                },
            );
            entities.get_mut(&envelope.entity_id).unwrap()
        };
        B::on_event(&context, &mut entity_state.state, envelope.event);
    } else {
        debug!("Removing entity: {}", envelope.entity_id);

        entities.pop(&envelope.entity_id);
    }
    envelope.seq_nr
}

#[cfg(test)]
mod tests {
    use std::{io, pin::Pin, sync::Arc};

    use super::*;
    use crate::{
        effect::{emit_deletion_event, emit_event, reply, unhandled, Effect, EffectExt},
        entity::Context,
    };
    use async_trait::async_trait;
    use test_log::test;
    use tokio::sync::{mpsc, oneshot, Notify};
    use tokio_stream::Stream;

    // Declare an entity behavior. We do this by declaring state, commands, events and then the
    // behavior itself. For our example, we are going to share a notifier object with our
    // behavior so that we can illustrate how things from outside can be passed in. This becomes
    // useful when needing to perform side-effects e.g. communicating with a sensor via a
    // connection over MODBUS, or sending something on a socket etc.

    #[derive(Default)]
    struct TempState {
        registered: bool,
        temp: u32,
    }

    enum TempCommand {
        Deregister,
        GetTemperature { reply_to: oneshot::Sender<u32> },
        Register,
        UpdateTemperature { temp: u32 },
    }

    #[derive(Clone, Debug, PartialEq)]
    enum TempEvent {
        Deregistered,
        Registered,
        TemperatureUpdated { temp: u32 },
    }

    struct TempSensorBehavior {
        recovered_1: Arc<Notify>,
        recovered_2: Arc<Notify>,
        updated: Arc<Notify>,
    }

    #[async_trait]
    impl EventSourcedBehavior for TempSensorBehavior {
        type State = TempState;

        type Command = TempCommand;

        type Event = TempEvent;

        fn for_command(
            _context: &Context,
            state: &Self::State,
            command: Self::Command,
        ) -> Box<dyn Effect<Self>> {
            match command {
                TempCommand::Register if !state.registered => {
                    emit_event(TempEvent::Registered).boxed()
                }

                TempCommand::Deregister if state.registered => {
                    emit_deletion_event(TempEvent::Deregistered).boxed()
                }

                TempCommand::GetTemperature { reply_to } if state.registered => {
                    reply(reply_to, state.temp).boxed()
                }

                TempCommand::UpdateTemperature { temp } if state.registered => {
                    emit_event(TempEvent::TemperatureUpdated { temp })
                        .and_then(|behavior: &Self, new_state, prev_result| {
                            let updated = behavior.updated.clone();
                            let temp = new_state.map_or(0, |s| s.temp);
                            async move {
                                if prev_result.is_ok() {
                                    updated.notify_one();
                                    println!("Updated with {}!", temp);
                                }
                                prev_result
                            }
                        })
                        .boxed()
                }

                _ => unhandled(),
            }
        }

        fn on_event(_context: &Context, state: &mut Self::State, event: Self::Event) {
            match event {
                TempEvent::Deregistered => state.registered = false,
                TempEvent::Registered => state.registered = true,
                TempEvent::TemperatureUpdated { temp } => state.temp = temp,
            }
        }

        async fn on_recovery_completed(&self, context: &Context, state: &Self::State) {
            if context.entity_id == "id-1" {
                self.recovered_1.notify_one();
            } else {
                self.recovered_2.notify_one();
            };
            println!("Recovered {} with {}!", context.entity_id, state.temp);
        }
    }

    // The following adapter is not normally created by a developer, but we
    // declare one here so that we can provide a source of events and capture
    // ones emitted by the entity manager.
    struct VecEventEnvelopeAdapter {
        initial_events: Option<Vec<EventEnvelope<TempEvent>>>,
        captured_events: mpsc::Sender<EventEnvelope<TempEvent>>,
    }

    #[async_trait]
    impl SourceProvider<TempEvent> for VecEventEnvelopeAdapter {
        async fn source_initial(
            &mut self,
        ) -> io::Result<Pin<Box<dyn Stream<Item = EventEnvelope<TempEvent>> + Send + 'async_trait>>>
        {
            if let Some(events) = self.initial_events.take() {
                Ok(Box::pin(tokio_stream::iter(events)))
            } else {
                Ok(Box::pin(tokio_stream::empty()))
            }
        }

        async fn source(
            &mut self,
            _entity_id: &EntityId,
        ) -> io::Result<Pin<Box<dyn Stream<Item = EventEnvelope<TempEvent>> + Send + 'async_trait>>>
        {
            Ok(Box::pin(tokio_stream::empty()))
        }
    }

    #[async_trait]
    impl Handler<TempEvent> for VecEventEnvelopeAdapter {
        async fn process(
            &mut self,
            envelope: EventEnvelope<TempEvent>,
        ) -> io::Result<EventEnvelope<TempEvent>> {
            self.captured_events
                .send(envelope.clone())
                .await
                .map(|_| envelope)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        "A problem occurred processing an envelope",
                    )
                })
        }
    }

    // We now set up and run the entity manager, send a few commands, and consume a
    // few events.

    #[test(tokio::test)]
    async fn new_manager_with_one_update_and_a_message_reply() {
        // Set up the behavior and entity manager.

        let temp_sensor_recovered_id_1 = Arc::new(Notify::new());
        let temp_sensor_recovered_id_2 = Arc::new(Notify::new());
        let temp_sensor_updated = Arc::new(Notify::new());

        let temp_sensor_behavior = TempSensorBehavior {
            recovered_1: temp_sensor_recovered_id_1.clone(),
            recovered_2: temp_sensor_recovered_id_2.clone(),
            updated: temp_sensor_updated.clone(),
        };

        let (temp_sensor_events, mut temp_sensor_events_captured) = mpsc::channel(4);
        let temp_sensor_event_adapter = VecEventEnvelopeAdapter {
            initial_events: Some(vec![
                EventEnvelope::new("id-1", 1, Utc::now(), TempEvent::Registered),
                EventEnvelope::new(
                    "id-1",
                    2,
                    Utc::now(),
                    TempEvent::TemperatureUpdated { temp: 10 },
                ),
            ]),
            captured_events: temp_sensor_events,
        };

        let (temp_sensor, temp_sensor_receiver) = mpsc::channel(10);

        let entity_manager_task = tokio::spawn(run(
            temp_sensor_behavior,
            temp_sensor_event_adapter,
            temp_sensor_receiver,
            NonZeroUsize::new(1).unwrap(),
        ));

        // Send a command to update the temperature and wait until it is done. We then wait
        // on a noification from within our entity that the update has occurred. Waiting on
        // this notification demonstrates side-effect behavior. Side-effects can be anything
        // e.g. updating something on a sensor using the MODBUS protocol...

        assert!(temp_sensor
            .send(Message::new(
                "id-1",
                TempCommand::UpdateTemperature { temp: 32 },
            ))
            .await
            .is_ok());

        temp_sensor_recovered_id_1.notified().await;
        temp_sensor_updated.notified().await;

        let (reply_to, reply) = oneshot::channel();
        assert!(temp_sensor
            .send(Message::new(
                "id-1",
                TempCommand::GetTemperature { reply_to }
            ))
            .await
            .is_ok());
        assert_eq!(reply.await.unwrap(), 32);

        // Update the temperature again so we will be able to see a couple of events
        // when we come to consume the entity manager's source.

        assert!(temp_sensor
            .send(Message::new(
                "id-1",
                TempCommand::UpdateTemperature { temp: 64 },
            ))
            .await
            .is_ok());

        temp_sensor_updated.notified().await;

        // Delete the entity

        assert!(temp_sensor
            .send(Message::new("id-1", TempCommand::Deregister,))
            .await
            .is_ok());

        // Create another entity. This should cause cache eviction as the cache is
        // size for a capacity of 1 when we created the entity manager.

        assert!(temp_sensor
            .send(Message::new("id-2", TempCommand::Register,))
            .await
            .is_ok());

        temp_sensor_recovered_id_2.notified().await;

        // We test eviction by querying for id-1 again. This should
        // fail as we have an empty produce method in our adapter.

        let (reply_to, reply) = oneshot::channel();
        assert!(temp_sensor
            .send(Message::new(
                "id-1",
                TempCommand::GetTemperature { reply_to }
            ))
            .await
            .is_ok());
        assert!(reply.await.is_err());

        temp_sensor_recovered_id_1.notified().await;

        // Drop our command sender so that the entity manager stops.

        drop(temp_sensor);

        assert!(entity_manager_task.await.is_ok());

        // We now consume our entity manager as a source of events.

        let envelope = temp_sensor_events_captured.recv().await.unwrap();
        assert_eq!(envelope.entity_id, EntityId::from("id-1"));
        assert_eq!(envelope.seq_nr, 3);
        assert_eq!(envelope.event, TempEvent::TemperatureUpdated { temp: 32 });

        let envelope = temp_sensor_events_captured.recv().await.unwrap();
        assert_eq!(envelope.event, TempEvent::TemperatureUpdated { temp: 64 });

        let envelope = temp_sensor_events_captured.recv().await.unwrap();
        assert!(envelope.deletion_event,);
        assert_eq!(envelope.event, TempEvent::Deregistered,);

        let envelope = temp_sensor_events_captured.recv().await.unwrap();
        assert_eq!(envelope.entity_id, EntityId::from("id-2"));
        assert_eq!(envelope.seq_nr, 1);
        assert_eq!(envelope.event, TempEvent::Registered);

        assert!(temp_sensor_events_captured.recv().await.is_none());
    }
}
