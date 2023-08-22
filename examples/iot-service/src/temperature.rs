//! Handle temperature sensor entity concerns
//!
use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc};

use akka_persistence_rs::{
    effect::{emit_event, reply, unhandled, EffectExt},
    entity::EventSourcedBehavior,
    entity_manager::{self},
    EntityId, Message,
};
use akka_persistence_rs_commitlog::{CommitLogEventEnvelopeMarshaler, CommitLogTopicAdapter};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use streambed::commit_log::Key;
use streambed_confidant::FileSecretStore;
use streambed_logged::{compaction::NthKeyBasedRetention, FileLog};
use tokio::sync::{mpsc, oneshot};

// Declare the entity

#[derive(Default)]
pub struct State {
    history: VecDeque<u32>,
    secret: SecretDataValue,
}

pub type SecretDataValue = SmolStr;

pub enum Command {
    Get { reply_to: oneshot::Sender<Vec<u32>> },
    Post { temperature: u32 },
    Register { secret: SecretDataValue },
}

#[derive(Clone, Deserialize, Serialize)]
pub enum Event {
    Registered { secret: SecretDataValue },
    TemperatureRead { temperature: u32 },
}

struct Behavior;

impl EventSourcedBehavior for Behavior {
    type State = State;

    type Command = Command;

    type Event = Event;

    fn for_command(
        _context: &akka_persistence_rs::entity::Context,
        state: &Self::State,
        command: Self::Command,
    ) -> Box<dyn akka_persistence_rs::effect::Effect<Self>> {
        match command {
            Command::Get { reply_to } if !state.history.is_empty() => {
                reply(reply_to, state.history.clone().into()).boxed()
            }

            Command::Post { temperature } if !state.secret.is_empty() => {
                emit_event(Event::TemperatureRead { temperature }).boxed()
            }

            Command::Register { secret } => emit_event(Event::Registered { secret }).boxed(),

            _ => unhandled(),
        }
    }

    fn on_event(
        _context: &akka_persistence_rs::entity::Context,
        state: &mut Self::State,
        event: Self::Event,
    ) {
        match event {
            Event::Registered { secret } => {
                state.secret = secret;
            }
            Event::TemperatureRead { temperature } => {
                if state.history.len() == MAX_HISTORY_EVENTS {
                    state.history.pop_front();
                }
                state.history.push_back(temperature);
            }
        }
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

struct EventEnvelopeMarshaler {
    events_key_secret_path: Arc<str>,
    secret_store: FileSecretStore,
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
impl CommitLogEventEnvelopeMarshaler<Event> for EventEnvelopeMarshaler {
    type SecretStore = FileSecretStore;

    fn to_compaction_key(entity_id: &EntityId, event: &Event) -> Option<Key> {
        let record_type = match event {
            Event::TemperatureRead { .. } => Some(0),
            Event::Registered { .. } => Some(1),
        };
        record_type.and_then(|record_type| {
            let entity_id = entity_id.parse::<u32>().ok()?;
            Some(record_type << EVENT_TYPE_BIT_SHIFT | entity_id as u64)
        })
    }

    fn to_entity_id(record: &streambed::commit_log::ConsumerRecord) -> Option<EntityId> {
        let entity_id = (record.key & EVENT_ID_BIT_MASK) as u32;
        let mut buffer = itoa::Buffer::new();
        Some(EntityId::from(buffer.format(entity_id)))
    }

    fn secret_store(&self) -> &Self::SecretStore {
        &self.secret_store
    }

    fn secret_path(&self, _entity_id: &EntityId) -> Arc<str> {
        self.events_key_secret_path.clone()
    }
}

const EVENTS_TOPIC: &str = "temperature";
const MAX_HISTORY_EVENTS: usize = 10;
// Size the following to the typical number of devices we expect to have in the system.
// Note though that it will impact memory, so there is a trade-off. Let's suppose this
// was some LoRaWAN system and that our gateway cannot handle more than 1,000 devices
// being connected. We can work out that 1,000 is therefore a reasonable limit. We can
// have less or more. The overhead is small, but should be calculated and measured for
// a production app.
const MAX_TOPIC_COMPACTION_KEYS: usize = 1_000;

/// Manage the temperature sensor.
pub async fn task(
    mut commit_log: FileLog,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
    command_receiver: mpsc::Receiver<Message<Command>>,
) {
    // We register a compaction strategy for our topic such that when we use up
    // 64KB of disk space (the default), we will run compaction so that unwanted
    // events are removed. In our scenario, unwanted events can be removed when
    // the exceed MAX_HISTORY_EVENTS as we do not have a requirement to ever
    // return more than that.
    commit_log
        .register_compaction(
            EVENTS_TOPIC.to_string(),
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
        EVENTS_TOPIC,
    );

    entity_manager::run(
        Behavior,
        file_log_topic_adapter,
        command_receiver,
        NonZeroUsize::new(10).unwrap(),
    )
    .await
}
