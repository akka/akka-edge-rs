//! The [EntityManager] handles the lifecycle and routing of messages for
//! an entity type. One EntityManager per entity type.
//! The EntityManager will spawn the entities on demand, i.e. when first
//! message is sent to a specific entity. It will passivate least used
//! entites to have a bounded number of entities in memory.
//! The entities will recover their state from a stream of events.

use std::{collections::HashMap, marker::PhantomData};
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt;

use crate::entity::Context;
use crate::entity::EventSourcedBehavior;
use crate::RecordSource;
use crate::{EntityId, Message, Record, RecordFlow};

/// The default amount of state (instances of an entity) that we
/// retain at any one time.
pub const DEFAULT_ACTIVE_STATE: usize = 1;

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
pub struct EntityManager<B>
where
    B: EventSourcedBehavior + Send,
{
    phantom: PhantomData<B>,
}

impl<B> EntityManager<B>
where
    B: EventSourcedBehavior + Send + 'static,
{
    fn update_entity(entities: &mut HashMap<EntityId, B::State>, record: Record<B::Event>)
    where
        B::State: Default,
    {
        if !record.metadata.deletion_event {
            // Apply an event to state, creating the entity entry if necessary.
            let context = Context {
                entity_id: record.entity_id.clone(),
            };
            let state = entities
                .entry(record.entity_id)
                .or_insert_with(B::State::default);
            B::on_event(&context, state, &record.event);
        } else {
            entities.remove(&record.entity_id);
        }
    }
}

impl<B> EntityManager<B>
where
    B: EventSourcedBehavior + Send + 'static,
{
    /// Establish a new entity manager with [DEFAULT_ACTIVE_STATE] instances
    /// active at a time.
    ///
    /// A task will also be spawned to source events and process
    /// commands using the stream and receiver channel passed in.
    pub fn new<RF, RS>(
        behavior: B,
        source: RS,
        flow: RF,
        receiver: Receiver<Message<B::Command>>,
    ) -> Self
    where
        B::Command: Send,
        B::Event: Send,
        B::State: Send + Sync,
        RS: RecordSource<B::Event> + Send + Sync + 'static,
        RF: RecordFlow<B::Event> + Send + Sync + 'static,
    {
        Self::with_capacity(behavior, source, flow, receiver, DEFAULT_ACTIVE_STATE)
    }

    /// Establish a new entity manager with capacity for a number of instances
    /// active at a time. This capacity is not a limit and memory can grow to
    /// accommodate more instances. However, dimensioning capacity in accordance
    /// with an application's working set needs is important. In particular,
    /// edge-based applications tend to retain all entities in memory.
    ///
    /// A task will also be spawned to source events and process
    /// commands using the stream and receiver channel passed in.
    pub fn with_capacity<RS, RF>(
        behavior: B,
        mut source: RS,
        mut flow: RF,
        mut receiver: Receiver<Message<B::Command>>,
        capacity: usize,
    ) -> Self
    where
        B::Command: Send,
        B::Event: Send,
        B::State: Send + Sync,
        RS: RecordSource<B::Event> + Send + Sync + 'static,
        RF: RecordFlow<B::Event> + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            // Source our events and populate our internal entities map.

            let mut entities: HashMap<EntityId, B::State> = HashMap::with_capacity(capacity);

            // Receive commands for the entities and process them.

            while let Some(message) = receiver.recv().await {
                // Source entity if we don't have it.

                if !entities.contains_key(&message.entity_id) {
                    if let Ok(records) = source.produce(&message.entity_id) {
                        tokio::pin!(records);
                        while let Some(record) = records.next().await {
                            Self::update_entity(&mut entities, record);
                        }
                    }
                }

                // Given an entity, send it the command, possibly producing an effect.
                // Effects may emit events that will update state on success.

                let context = Context {
                    entity_id: message.entity_id.clone(),
                };
                let mut effect = behavior.for_command(
                    &context,
                    entities.get(&message.entity_id),
                    message.command,
                );
                let _ = effect
                    .process(&mut flow, message.entity_id, Ok(()), &mut |record| {
                        Self::update_entity(&mut entities, record)
                    })
                    .await;
            }
        });

        Self {
            phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;
    use crate::{
        effect::{emit_deletion_event, emit_event, reply, then, unhandled, Effect, EffectExt},
        entity::Context,
    };
    use async_trait::async_trait;
    use test_log::test;
    use tokio::{
        sync::{mpsc, oneshot, Notify},
        time,
    };
    use tokio_stream::Stream;

    // Declare an entity behavior. We do this by declaring state, commands, events and then the
    // behavior itself. For our example, we are going to share a notifier object with our
    // behavior so that we can illustrate how things from outside can be passed in. This becomes
    // useful when needing to perform side-effects e.g. communicating with a sensor via a
    // connection over MODBUS, or sending something on a socket etc.

    #[derive(Default)]
    struct TempState {
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
        updated: Arc<Notify>,
    }

    impl EventSourcedBehavior for TempSensorBehavior {
        type State = TempState;

        type Command = TempCommand;

        type Event = TempEvent;

        fn for_command(
            &self,
            _context: &Context,
            state: Option<&Self::State>,
            command: Self::Command,
        ) -> Box<dyn Effect<Self::Event>> {
            match (state, command) {
                (None, TempCommand::Register) => emit_event(TempEvent::Registered).boxed(),

                (Some(_), TempCommand::Deregister) => {
                    emit_deletion_event(TempEvent::Deregistered).boxed()
                }

                (Some(state), TempCommand::GetTemperature { reply_to }) => {
                    reply(reply_to, state.temp).boxed()
                }

                (Some(_state), TempCommand::UpdateTemperature { temp }) => {
                    let updated = self.updated.clone();
                    emit_event(TempEvent::TemperatureUpdated { temp })
                        .and(then(|prev_result| async move {
                            if prev_result.is_ok() {
                                updated.notify_one();
                                println!("Updated!");
                            }
                            prev_result
                        }))
                        .boxed()
                }

                _ => unhandled(),
            }
        }

        fn on_event(_context: &Context, state: &mut Self::State, event: &Self::Event) {
            if let TempEvent::TemperatureUpdated { temp } = event {
                state.temp = *temp;
            }
        }
    }

    // Create a source of records. Sources are used to provide
    // the entity manager with records of events given an entity id. The
    // contract permits for records not belonging to an entity id to be
    // returned. This way, all records can be sourced at once, which is
    // typically what we do at the edge i.e. all entities are brought into
    // memory.
    //
    // For our tests, we produce our records from a vector provided during
    // construction. We only expect to be called once. If we are called
    // more than once then no records will be produced.
    struct VecRecordSource {
        records: Option<Vec<Record<TempEvent>>>,
    }

    impl RecordSource<TempEvent> for VecRecordSource {
        fn produce(
            &mut self,
            _entity_id: &EntityId,
        ) -> io::Result<Pin<Box<dyn Stream<Item = Record<TempEvent>> + Send>>> {
            if let Some(records) = self.records.take() {
                Ok(Box::pin(tokio_stream::iter(records)))
            } else {
                Ok(Box::pin(tokio_stream::empty()))
            }
        }
    }

    // Create a flow of records. Flows are used for the entity manager
    // to push records through given an effect that emits events. The flow
    // is generally used to persist an entity's events.
    //
    // For our tests, we record the records that have been pushed so we
    // can check them later.
    struct VecRecordFlow {
        records: Arc<Mutex<Vec<Record<TempEvent>>>>,
    }

    #[async_trait]
    impl RecordFlow<TempEvent> for VecRecordFlow {
        async fn process(&mut self, record: Record<TempEvent>) -> io::Result<Record<TempEvent>> {
            self.records.lock().unwrap().push(record.clone());
            Ok(record)
        }
    }

    // We now set up and run the entity manager, send a few commands, and consume a
    // few events.

    #[test(tokio::test)]
    async fn new_manager_with_one_update_and_a_message_reply() {
        // Set up the behavior and entity manager.

        let temp_sensor_updated = Arc::new(Notify::new());

        let temp_sensor_behavior = TempSensorBehavior {
            updated: temp_sensor_updated.clone(),
        };

        let temp_sensor_source = VecRecordSource {
            records: Some(vec![
                Record::new("id-1", TempEvent::Registered),
                Record::new("id-1", TempEvent::TemperatureUpdated { temp: 10 }),
            ]),
        };

        let temp_sensor_events = Arc::new(Mutex::new(Vec::with_capacity(4)));
        let temp_sensor_flow = VecRecordFlow {
            records: temp_sensor_events.clone(),
        };

        let (temp_sensor, temp_sensor_receiver) = mpsc::channel::<Message<TempCommand>>(10);

        EntityManager::new(
            temp_sensor_behavior,
            temp_sensor_source,
            temp_sensor_flow,
            temp_sensor_receiver,
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

        // Delete the entity

        assert!(temp_sensor
            .send(Message::new("id-1", TempCommand::Deregister,))
            .await
            .is_ok());

        // Create another entity

        assert!(temp_sensor
            .send(Message::new("id-2", TempCommand::Register,))
            .await
            .is_ok());

        // We now consume our entity manager as a source of events. If we attempt to
        // consume more than four events given this use-case, then the program would
        // run forever as we don't produce a fifth event.

        let mut wait_count = 0;

        while wait_count < 10 {
            let (temp_sensor_events_len, temp_sensor_events) = {
                let guarded_temp_sensor_events = temp_sensor_events.lock().unwrap();
                (
                    guarded_temp_sensor_events.len(),
                    guarded_temp_sensor_events.clone(),
                )
            };

            match temp_sensor_events_len.cmp(&4) {
                std::cmp::Ordering::Less => {
                    wait_count += 1;
                    time::sleep(Duration::from_millis(100)).await;
                }

                std::cmp::Ordering::Equal => {
                    let mut temp_sensor_events_iter = temp_sensor_events.iter();

                    assert_eq!(
                        temp_sensor_events_iter.next().unwrap(),
                        &Record::new("id-1", TempEvent::TemperatureUpdated { temp: 32 })
                    );
                    assert_eq!(
                        temp_sensor_events_iter.next().unwrap(),
                        &Record::new("id-1", TempEvent::TemperatureUpdated { temp: 64 })
                    );
                    assert_eq!(
                        temp_sensor_events_iter.next().unwrap(),
                        &Record {
                            entity_id: "id-1".to_string(),
                            event: TempEvent::Deregistered,
                            metadata: crate::RecordMetadata {
                                deletion_event: true
                            }
                        }
                    );
                    assert_eq!(
                        temp_sensor_events_iter.next().unwrap(),
                        &Record::new("id-2", TempEvent::Registered)
                    );
                    break;
                }

                std::cmp::Ordering::Greater => panic!("Too many events"),
            }
        }
    }
}
