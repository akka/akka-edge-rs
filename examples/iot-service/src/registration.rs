//! Handle a local registration entity for testing without a remote gRPC endpoint.
//! Illustrates the use of a [akka_projection_rs::eventsource::LocalSourceProvider]
//!
use std::{num::NonZeroUsize, sync::Arc};

use akka_persistence_rs::{
    effect::{emit_event, EffectExt},
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
use streambed_logged::{compaction::KeyBasedRetention, FileLog};
use tokio::sync::mpsc;

// Declare the entity for the purposes of retaining registration keys for devices.

pub type SecretDataValue = SmolStr;

#[derive(Default)]
pub struct State {
    secret: SecretDataValue,
}

pub enum Command {
    Register { secret: SecretDataValue },
}

#[derive(Clone, Deserialize, Serialize)]
pub enum Event {
    Registered { secret: SecretDataValue },
}

struct Behavior;

impl EventSourcedBehavior for Behavior {
    type State = State;

    type Command = Command;

    type Event = Event;

    fn for_command(
        _context: &akka_persistence_rs::entity::Context,
        _state: &Self::State,
        command: Self::Command,
    ) -> Box<dyn akka_persistence_rs::effect::Effect<Self>> {
        let Command::Register { secret } = command;
        emit_event(Event::Registered { secret }).boxed()
    }

    fn on_event(
        _context: &akka_persistence_rs::entity::Context,
        state: &mut Self::State,
        event: Self::Event,
    ) {
        let Event::Registered { secret } = event;

        state.secret = secret.clone();
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
impl CommitLogEventEnvelopeMarshaler<Event> for EventEnvelopeMarshaler {
    type SecretStore = FileSecretStore;

    fn to_compaction_key(entity_id: &EntityId, event: &Event) -> Option<Key> {
        let entity_id = entity_id.parse::<u32>().ok()?;
        let Event::Registered { .. } = event;
        Some(0 << EVENT_TYPE_BIT_SHIFT | entity_id as u64)
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

pub const EVENTS_TOPIC: &str = "registrations";
// Size the following to the typical number of entities we expect to have in the system.
const MAX_TOPIC_COMPACTION_KEYS: usize = 1;

/// Manage the registration.
pub async fn task(
    mut commit_log: FileLog,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
    command_receiver: mpsc::Receiver<Message<Command>>,
) {
    commit_log
        .register_compaction(
            EVENTS_TOPIC.to_string(),
            KeyBasedRetention::new(MAX_TOPIC_COMPACTION_KEYS),
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
