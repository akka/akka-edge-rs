//! The [EntityManager] handles the lifecycle and routing of messages for
//! an entity type. One EntityManager per entity type.
//! The EntityManager will spawn the entities on demand, i.e. when first
//! message is sent to a specific entity. It will passivate least used
//! entites to have a bounded number of entities in memory.
//! The entities will recover their state from a stream of events.

use std::pin::Pin;
use std::task::{Context as StdContext, Poll};
use std::{collections::HashMap, marker::PhantomData};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use crate::entity::{Context, EntityOp};
use crate::entity::{EntityId, EventSourcedBehavior};

/// The default amount of state (instances of an entity) that we
/// retain at any one time.
pub const DEFAULT_ACTIVE_STATE: usize = 1;

/// A message encapsulates a command that is addressed to a specific entity.
#[derive(Debug, PartialEq)]
pub struct Message<C> {
    entity_id: EntityId,
    command: C,
}

impl<C> Message<C> {
    pub fn new<EI>(entity_id: EI, command: C) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            command,
        }
    }
}

/// A record is an event associated with a specific entity.
#[derive(Debug, PartialEq)]
pub struct Record<E> {
    entity_id: EntityId,
    event: E,
}

impl<E> Record<E> {
    pub fn new<EI>(entity_id: EI, event: E) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            event,
        }
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
///
/// Yielded events can be consumed by using the entity manager as a source
/// of a [Stream].
pub struct EntityManager<B>
where
    B: EventSourcedBehavior + Send,
{
    stream_receiver: mpsc::Receiver<Record<B::Event>>,
    phantom: PhantomData<B>,
}

impl<B> EntityManager<B>
where
    B: EventSourcedBehavior + Send + 'static,
{
    fn update_entity(
        entities: &mut HashMap<EntityId, B::State>,
        record: &Record<B::Event>,
    ) -> bool {
        // Apply an event to state if we have an entity for it
        let context = Context {
            entity_id: record.entity_id.clone(),
        };
        let state = entities.get_mut(&record.entity_id);
        match B::on_event(&context, state, &record.event) {
            EntityOp::Create(new_state) => {
                entities.insert(context.entity_id, new_state);
                true
            }
            EntityOp::Update => true,
            EntityOp::Delete => {
                entities.remove(&record.entity_id);
                true
            }
            EntityOp::None => false,
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
    pub fn new<S>(behavior: B, source: S, receiver: Receiver<Message<B::Command>>) -> Self
    where
        B::Command: Send,
        B::Event: Send,
        B::State: Send,
        S: Stream<Item = Record<B::Event>> + Send + 'static,
    {
        Self::with_capacity(behavior, source, receiver, DEFAULT_ACTIVE_STATE)
    }

    // Max number of event records we can enqueue for streaming out before
    // back-pressuring the sender.
    const MAX_STREAM_CAPACITY: usize = 10;

    /// Establish a new entity manager with capacity for a number of instances
    /// active at a time. This capacity is not a limit and memory can grow to
    /// accommodate more instances. However, dimensioning capacity in accordance
    /// with an application's working set needs is important. In particular,
    /// edge-based applications tend to retain all entities in memory.
    ///
    /// A task will also be spawned to source events and process
    /// commands using the stream and receiver channel passed in.
    pub fn with_capacity<S>(
        behavior: B,
        source: S,
        mut receiver: Receiver<Message<B::Command>>,
        capacity: usize,
    ) -> Self
    where
        B::Command: Send,
        B::Event: Send,
        B::State: Send,
        S: Stream<Item = Record<B::Event>> + Send + 'static,
    {
        // Create a channel that can be used to emit records using the
        // entity manager as a stream source.
        let (stream_sender, stream_receiver) = mpsc::channel(Self::MAX_STREAM_CAPACITY);

        tokio::spawn(async move {
            // Source our events and populate our internal entities map.

            let mut entities: HashMap<EntityId, B::State> = HashMap::with_capacity(capacity);

            tokio::pin!(source);
            while let Some(record) = source.next().await {
                Self::update_entity(&mut entities, &record);
            }

            // Receive commands for the entities and process them.

            while let Some(message) = receiver.recv().await {
                // Given an entity, send it the command, possibly producing an effect.

                let context = Context {
                    entity_id: message.entity_id.clone(),
                };
                let effect = behavior.for_command(
                    &context,
                    entities.get(&message.entity_id),
                    message.command,
                );

                if let Some(mut effect) = effect {
                    if let Ok(Some(event)) = effect.take(Ok(None)).await {
                        let record = Record::new(context.entity_id, event);
                        let entity_updated = Self::update_entity(&mut entities, &record);
                        if entity_updated {
                            // Emit our record to the outside world.
                            if stream_sender.send(record).await.is_err() {
                                // All bets are off if we can't emit events.
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self {
            stream_receiver,
            phantom: PhantomData,
        }
    }
}

impl<B> Stream for EntityManager<B>
where
    B: EventSourcedBehavior + Send + 'static,
{
    type Item = Record<B::Event>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut StdContext<'_>) -> Poll<Option<Self::Item>> {
        // Safety: we are making a promise to the compiler here that stream_receiver is
        // not going to be mutated elsewhere. This works as there is only one consumer of
        // this stream.
        let mut stream_receiver =
            unsafe { self.map_unchecked_mut(|this| &mut this.stream_receiver) };
        stream_receiver.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        effect::{emit_event, reply, then, Effect, EffectExt},
        entity::{Context, EntityOp},
    };
    use test_log::test;
    use tokio::sync::{mpsc, oneshot, Notify};

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

    #[derive(Debug, PartialEq)]
    enum TempEvent {
        Deregistered,
        Registered,
        TemperatureUpdated { temp: u32 },
    }

    struct TempSensor {
        updated: Arc<Notify>,
    }

    impl EventSourcedBehavior for TempSensor {
        type State = TempState;

        type Command = TempCommand;

        type Event = TempEvent;

        fn for_command(
            &self,
            _context: &Context,
            state: Option<&Self::State>,
            command: Self::Command,
        ) -> Option<Box<dyn Effect<Self::Event>>> {
            match (state, command) {
                (None, TempCommand::Register) => emit_event(TempEvent::Registered).build(),

                (Some(_), TempCommand::Deregister) => emit_event(TempEvent::Deregistered).build(),

                (Some(state), TempCommand::GetTemperature { reply_to }) => {
                    reply(reply_to, state.temp).build()
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
                        .build()
                }

                _ => None,
            }
        }

        fn on_event(
            _context: &Context,
            state: Option<&mut Self::State>,
            event: &Self::Event,
        ) -> EntityOp<Self::State> {
            match (state, event) {
                (None, TempEvent::Registered) => EntityOp::Create(TempState::default()),
                (Some(_), TempEvent::Deregistered) => EntityOp::Delete,
                (Some(state), TempEvent::TemperatureUpdated { temp }) => {
                    state.temp = *temp;
                    EntityOp::Update
                }
                _ => EntityOp::None,
            }
        }
    }

    // We now set up and run the entity manager, send a few commands, and consume a
    // few events.

    #[test(tokio::test)]
    async fn new_manager_with_one_update_and_a_message_reply() {
        // Set up the behavior and entity manager.

        let temp_sensor_updated = Arc::new(Notify::new());

        let temp_sensor_behavior = TempSensor {
            updated: temp_sensor_updated.clone(),
        };

        let temp_sensor_events = tokio_stream::iter(vec![
            Record::new("id-1", TempEvent::Registered),
            Record::new("id-1", TempEvent::TemperatureUpdated { temp: 10 }),
        ]);

        let (temp_sensor, temp_sensor_receiver) = mpsc::channel::<Message<TempCommand>>(10);

        let mut entity_manager = EntityManager::new(
            temp_sensor_behavior,
            temp_sensor_events,
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

        assert_eq!(
            entity_manager.next().await.unwrap(),
            Record::new("id-1", TempEvent::TemperatureUpdated { temp: 32 })
        );

        assert_eq!(
            entity_manager.next().await.unwrap(),
            Record::new("id-1", TempEvent::TemperatureUpdated { temp: 64 })
        );
        assert_eq!(
            entity_manager.next().await.unwrap(),
            Record::new("id-1", TempEvent::Deregistered)
        );
        assert_eq!(
            entity_manager.next().await.unwrap(),
            Record::new("id-2", TempEvent::Registered)
        );
    }
}
