// Handle temperature sensor entity concerns

use std::{io, num::NonZeroUsize, sync::Arc};

use akka_persistence_rs::{
    effect::{self, emit_event, reply, then, unhandled, Effect, EffectExt},
    entity::{Context, EventSourcedBehavior},
};
use akka_persistence_rs::{
    entity_manager::{self},
    EntityId, Message,
};
use akka_persistence_rs_commitlog::{
    CommitLogMarshaler, CommitLogTopicAdapter, EncryptedCommitLogMarshaler, EventEnvelope,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future;
use streambed::commit_log::{ConsumerRecord, Key, ProducerRecord, Topic};
use streambed_confidant::FileSecretStore;
use streambed_logged::{compaction::NthKeyBasedRetention, FileLog};
use tokio::sync::{broadcast, mpsc, oneshot};

pub use iot_service_model::temperature::{Event, SecretDataValue, State};

// Declare temperature sensor entity concerns

pub enum Command {
    Get { reply_to: oneshot::Sender<State> },
    Post { temperature: u32 },
    Register { secret: SecretDataValue },
}

#[derive(Clone)]
pub enum BroadcastEvent {
    Saved { entity_id: EntityId, event: Event },
    NotSaved { entity_id: EntityId },
}

pub struct Behavior {
    events: broadcast::Sender<BroadcastEvent>,
}

impl EventSourcedBehavior for Behavior {
    type State = State;

    type Command = Command;

    type Event = Event;

    fn for_command(
        context: &Context,
        state: &Self::State,
        command: Self::Command,
    ) -> Box<dyn Effect<Self>> {
        match command {
            Command::Get { reply_to } if !state.secret.is_empty() => {
                reply(reply_to, state.clone()).boxed()
            }

            Command::Post { temperature } if !state.secret.is_empty() => {
                let broadcast_entity_id = context.entity_id.clone();
                let broadcast_temperature = temperature;

                let broadcast_event =
                    move |behavior: &Self,
                          _new_state: Option<&State>,
                          prev_result: effect::Result| {
                        let result = if prev_result.is_ok() {
                            behavior
                                .events
                                .send(BroadcastEvent::Saved {
                                    entity_id: broadcast_entity_id,
                                    event: Event::TemperatureRead {
                                        temperature: broadcast_temperature,
                                    },
                                })
                                .map(|_| ())
                                .map_err(|_| {
                                    effect::Error::IoError(io::Error::new(
                                        io::ErrorKind::Other,
                                        "Problem emitting an event",
                                    ))
                                })
                        } else {
                            let _ = behavior.events.send(BroadcastEvent::NotSaved {
                                entity_id: broadcast_entity_id,
                            });
                            prev_result
                        };
                        future::ready(result)
                    };

                emit_event(Event::TemperatureRead { temperature })
                    .and(then(broadcast_event))
                    .boxed()
            }

            Command::Register { secret } => emit_event(Event::Registered { secret }).boxed(),

            _ => unhandled(),
        }
    }

    fn on_event(context: &Context, state: &mut Self::State, event: Self::Event) {
        state.on_event(context, event);
    }
}

// Declare how we marshal our data with FileLog. This is essentially
// down to encoding our event type and entity id to a key used for
// the purposes of log compaction. The marshaller will use CBOR for
// serialization of data persisted with the log. We do this as it has the
// benefits of JSON in terms of schema evolution, but is faster to serialize
// and represents itself as smaller on disk.
// Marshalers can be completely overidden to serialize events and encode
// keys in any way required.

pub struct EventEnvelopeMarshaler {
    pub events_key_secret_path: Arc<str>,
    pub secret_store: FileSecretStore,
}

// Our event keys will occupy the top 12 bits of the key, meaning
// that we can have 4K types of event. We use 32 of the bottom 52
// bits as the entity id. We choose 32 bits as this is a common size
// for identifiers transmitted from IoT sensors. These identifiers
// are also known as "device addresses" and represent an address
// which may, in turn, equate to a 64 bit address globally unique
// to a device. These globally unique addresses are not generally
// transmitted in order to conserve packet size.
const EVENT_TYPE_BIT_SHIFT: usize = 52;
const EVENT_ID_BIT_MASK: u64 = 0xFFFFFFFF;

#[async_trait]
impl CommitLogMarshaler<Event> for EventEnvelopeMarshaler {
    fn to_compaction_key(&self, entity_id: &EntityId, event: &Event) -> Option<Key> {
        let record_type = match event {
            Event::TemperatureRead { .. } => Some(0),
            Event::Registered { .. } => Some(1),
        };
        record_type.and_then(|record_type| {
            let entity_id = entity_id.parse::<u32>().ok()?;
            Some(record_type << EVENT_TYPE_BIT_SHIFT | entity_id as u64)
        })
    }

    fn to_entity_id(&self, record: &streambed::commit_log::ConsumerRecord) -> Option<EntityId> {
        let entity_id = (record.key & EVENT_ID_BIT_MASK) as u32;
        let mut buffer = itoa::Buffer::new();
        Some(EntityId::from(buffer.format(entity_id)))
    }

    async fn envelope(
        &self,
        entity_id: EntityId,
        record: ConsumerRecord,
    ) -> Option<EventEnvelope<Event>> {
        self.decrypted_envelope(entity_id, record).await
    }

    /// Produce a producer record from an event and its entity info.
    async fn producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: Event,
    ) -> Option<ProducerRecord> {
        self.encrypted_producer_record(topic, entity_id, seq_nr, timestamp, event)
            .await
    }
}

#[async_trait]
impl EncryptedCommitLogMarshaler<Event> for EventEnvelopeMarshaler {
    type SecretStore = FileSecretStore;

    fn secret_store(&self) -> &Self::SecretStore {
        &self.secret_store
    }

    fn secret_path(&self, _entity_id: &EntityId) -> Arc<str> {
        self.events_key_secret_path.clone()
    }
}

// Where events are published in the commit log.
pub const EVENTS_TOPIC: &str = "temperature";

// A namespace for our entity's events
pub const ENTITY_TYPE: &str = "Sensor";

const MAX_HISTORY_EVENTS: usize = 10;
// Size the following to the typical number of devices we expect to have in the system.
// Note though that it will impact memory, so there is a trade-off. Let's suppose this
// was some LoRaWAN system and that our gateway cannot handle more than 1,000 devices
// being connected. We can work out that 1,000 is therefore a reasonable limit. We can
// have less or more. The overhead is small, but should be calculated and measured for
// a production app.
const MAX_TOPIC_COMPACTION_KEYS: usize = 1_000;

// Manage the temperature sensor.
pub async fn task(
    mut commit_log: FileLog,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
    command_receiver: mpsc::Receiver<Message<Command>>,
    events: broadcast::Sender<BroadcastEvent>,
) {
    // We register a compaction strategy for our topic such that when we use up
    // 64KB of disk space (the default), we will run compaction so that unwanted
    // events are removed. In our scenario, unwanted events can be removed when
    // the exceed MAX_HISTORY_EVENTS as we do not have a requirement to ever
    // return more than that.
    let events_topic = Topic::from(EVENTS_TOPIC);

    commit_log
        .register_compaction(
            events_topic.clone(),
            NthKeyBasedRetention::new(MAX_TOPIC_COMPACTION_KEYS, MAX_HISTORY_EVENTS),
        )
        .await
        .unwrap();

    let file_log_topic_adapter = CommitLogTopicAdapter::new(
        commit_log,
        EventEnvelopeMarshaler {
            events_key_secret_path: Arc::from(events_key_secret_path),
            secret_store,
        },
        "iot-service",
        events_topic,
    );

    entity_manager::run(
        Behavior { events },
        file_log_topic_adapter,
        command_receiver,
        NonZeroUsize::new(10).unwrap(),
    )
    .await
}
