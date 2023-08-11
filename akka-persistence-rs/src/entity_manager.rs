//! The [EntityManager] handles the lifecycle and routing of messages for
//! an entity type. One EntityManager per entity type.
//! The EntityManager will spawn the entities on demand, i.e. when first
//! message is sent to a specific entity. It will passivate least used
//! entites to have a bounded number of entities in memory.
//! The entities will recover their state from a stream of events.

use async_trait::async_trait;
use lru::LruCache;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use tokio::sync::mpsc::Receiver;
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::{Stream, StreamExt};

use crate::entity::Context;
use crate::entity::EventSourcedBehavior;
use crate::{EntityId, Message, Record};

/// Enables the interaction between data storage and an entity manager.
#[async_trait]
pub trait RecordAdapter<E> {
    /// Produce an initial source of events, which is called upon an entity
    /// manager starting up. Any error from this method is considered fatal
    /// and will terminate the entity manager.
    async fn produce_initial(
        &mut self,
    ) -> io::Result<Pin<Box<dyn Stream<Item = Record<E>> + Send + 'async_trait>>>;

    /// Produce a source of events. An entity id
    /// is passed to the source method so that the source is
    /// discriminate regarding the entity events to supply.
    async fn produce(
        &mut self,
        entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = Record<E>> + Send + 'async_trait>>>;

    /// Consume a record, performing some processing
    /// e.g. persisting a record, and then returning the same record
    /// if all went well.
    async fn process(&mut self, record: Record<E>) -> io::Result<Record<E>>;
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
/// Yielded events can be consumed by using the entity manager as a source
/// of a [Stream].
pub struct EntityManager<B> {
    join_handle: JoinHandle<()>,
    phantom: PhantomData<B>,
}

impl<B> EntityManager<B>
where
    B: EventSourcedBehavior,
{
    /// This method can be used to wait for an entity manager to complete.
    pub async fn join(self) -> Result<(), JoinError> {
        self.join_handle.await
    }

    fn update_entity(entities: &mut LruCache<EntityId, B::State>, record: Record<B::Event>)
    where
        B::State: Default,
    {
        if !record.metadata.deletion_event {
            // Apply an event to state, creating the entity entry if necessary.
            let context = Context {
                entity_id: &record.entity_id,
            };
            let state = entities.get_or_insert_mut(record.entity_id.clone(), B::State::default);
            B::on_event(&context, state, &record.event);
        } else {
            entities.pop(&record.entity_id);
        }
    }
}

impl<B> EntityManager<B>
where
    B: EventSourcedBehavior + Send + Sync + 'static,
{
    /// Establish a new entity manager with [DEFAULT_ACTIVE_STATE] instances
    /// active at a time.
    ///
    /// A task will also be spawned to source events and process
    /// commands using the stream and receiver channel passed in.
    pub fn new<A>(behavior: B, adapter: A, receiver: Receiver<Message<B::Command>>) -> Self
    where
        B::Command: Send,
        B::State: Send + Sync,
        A: RecordAdapter<B::Event> + Send + 'static,
    {
        const DEFAULT_ACTIVE_STATE: usize = 10;
        Self::with_capacity(
            behavior,
            adapter,
            receiver,
            NonZeroUsize::new(DEFAULT_ACTIVE_STATE).unwrap(),
        )
    }

    /// Establish a new entity manager with capacity for a number of instances
    /// active at a time. This capacity is not a limit and memory can grow to
    /// accommodate more instances. However, dimensioning capacity in accordance
    /// with an application's working set needs is important. In particular,
    /// edge-based applications tend to retain all entities in memory.
    ///
    /// A task will also be spawned to source events and process
    /// commands using the stream and receiver channel passed in.
    pub fn with_capacity<A>(
        behavior: B,
        mut adapter: A,
        mut receiver: Receiver<Message<B::Command>>,
        capacity: NonZeroUsize,
    ) -> Self
    where
        B::Command: Send,
        B::State: Send + Sync,
        A: RecordAdapter<B::Event> + Send + 'static,
    {
        let join_handle = tokio::spawn(async move {
            // Source our initial events and populate our internal entities map.

            let mut entities = LruCache::new(capacity);

            if let Ok(records) = adapter.produce_initial().await {
                tokio::pin!(records);
                while let Some(record) = records.next().await {
                    Self::update_entity(&mut entities, record);
                }
                for (entity_id, state) in entities.iter() {
                    let context = Context { entity_id };
                    behavior.on_recovery_completed(&context, state).await;
                }
            } else {
                // A problem sourcing initial events is regarded as fatal.
                return;
            }

            // Receive commands for the entities and process them.

            while let Some(message) = receiver.recv().await {
                // Source entity if we don't have it.

                let mut state = entities.get(&message.entity_id);

                if state.is_none() {
                    if let Ok(records) = adapter.produce(&message.entity_id).await {
                        tokio::pin!(records);
                        while let Some(record) = records.next().await {
                            Self::update_entity(&mut entities, record);
                        }
                        state = entities.get(&message.entity_id);
                        let context = Context {
                            entity_id: &message.entity_id,
                        };
                        behavior
                            .on_recovery_completed(&context, state.unwrap_or(&B::State::default()))
                            .await;
                    } else {
                        continue;
                    }
                }

                // Given an entity, send it the command, possibly producing an effect.
                // Effects may emit events that will update state on success.

                let context = Context {
                    entity_id: &message.entity_id,
                };
                let mut effect = B::for_command(
                    &context,
                    state.unwrap_or(&B::State::default()),
                    message.command,
                );
                let result = effect
                    .process(
                        &behavior,
                        &mut adapter,
                        &mut entities,
                        context.entity_id,
                        Ok(()),
                        &mut |entities, record| Self::update_entity(entities, record),
                    )
                    .await;
                if result.is_err() {
                    entities.pop(context.entity_id);
                }
            }
        });

        Self {
            join_handle,
            phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, pin::Pin, sync::Arc};

    use super::*;
    use crate::{
        effect::{emit_deletion_event, emit_event, reply, then, unhandled, Effect, EffectExt},
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
                        .and(then(|behavior: &Self, new_state, prev_result| {
                            let updated = behavior.updated.clone();
                            let temp = new_state.map_or(0, |s| s.temp);
                            async move {
                                if prev_result.is_ok() {
                                    updated.notify_one();
                                    println!("Updated with {}!", temp);
                                }
                                prev_result
                            }
                        }))
                        .boxed()
                }

                _ => unhandled(),
            }
        }

        fn on_event(_context: &Context, state: &mut Self::State, event: &Self::Event) {
            match event {
                TempEvent::Deregistered => state.registered = false,
                TempEvent::Registered => state.registered = true,
                TempEvent::TemperatureUpdated { temp } => state.temp = *temp,
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
    // declare one here so that we can provide a source of records and capture
    // ones emitted by the entity manager.
    struct VecRecordAdapter {
        initial_records: Option<Vec<Record<TempEvent>>>,
        captured_records: mpsc::Sender<Record<TempEvent>>,
    }

    #[async_trait]
    impl RecordAdapter<TempEvent> for VecRecordAdapter {
        async fn produce_initial(
            &mut self,
        ) -> io::Result<Pin<Box<dyn Stream<Item = Record<TempEvent>> + Send + 'async_trait>>>
        {
            if let Some(records) = self.initial_records.take() {
                Ok(Box::pin(tokio_stream::iter(records)))
            } else {
                Ok(Box::pin(tokio_stream::empty()))
            }
        }

        async fn produce(
            &mut self,
            _entity_id: &EntityId,
        ) -> io::Result<Pin<Box<dyn Stream<Item = Record<TempEvent>> + Send + 'async_trait>>>
        {
            Ok(Box::pin(tokio_stream::empty()))
        }

        async fn process(&mut self, record: Record<TempEvent>) -> io::Result<Record<TempEvent>> {
            self.captured_records
                .send(record.clone())
                .await
                .map(|_| record)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        "A problem occurred processing a record",
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
        let temp_sensor_record_adapter = VecRecordAdapter {
            initial_records: Some(vec![
                Record::new("id-1", TempEvent::Registered),
                Record::new("id-1", TempEvent::TemperatureUpdated { temp: 10 }),
            ]),
            captured_records: temp_sensor_events,
        };

        let (temp_sensor, temp_sensor_receiver) = mpsc::channel(10);

        EntityManager::with_capacity(
            temp_sensor_behavior,
            temp_sensor_record_adapter,
            temp_sensor_receiver,
            NonZeroUsize::new(1).unwrap(),
        );

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

        // We now consume our entity manager as a source of events.

        assert_eq!(
            temp_sensor_events_captured.recv().await.unwrap(),
            Record::new("id-1", TempEvent::TemperatureUpdated { temp: 32 })
        );
        assert_eq!(
            temp_sensor_events_captured.recv().await.unwrap(),
            Record::new("id-1", TempEvent::TemperatureUpdated { temp: 64 })
        );
        assert_eq!(
            temp_sensor_events_captured.recv().await.unwrap(),
            Record {
                entity_id: "id-1".to_string(),
                event: TempEvent::Deregistered,
                metadata: crate::RecordMetadata {
                    deletion_event: true
                }
            }
        );
        assert_eq!(
            temp_sensor_events_captured.recv().await.unwrap(),
            Record::new("id-2", TempEvent::Registered)
        );
        assert!(temp_sensor_events_captured.recv().await.is_none());
    }
}
