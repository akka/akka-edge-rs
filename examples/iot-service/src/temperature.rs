use std::collections::VecDeque;

use akka_persistence_rs::{
    effect::{emit_event, reply, unhandled, EffectExt},
    entity::EventSourcedBehavior,
    entity_manager::EntityManager,
    EntityId, Message, Record,
};
use akka_persistence_rs_filelog::{FileLogRecordMarshaler, FileLogTopicAdapter};
use serde::{Deserialize, Serialize};
use streambed::commit_log::Key;
use streambed_logged::{compaction::NthKeyBasedRetention, FileLog};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinError,
};

const EVENTS_TOPIC: &str = "temperature";
const MAX_HISTORY_EVENTS: usize = 10;
// Size the following to the typical number of devices we expect to have in the system.
// Note though that it will impact memory, so there is a trade-off. Let's suppose this
// was some LoRaWAN system and that our gateway cannot handle more than 1,000 devices
// being connected. We can work out that 1,000 is therefore a reasonable limit. We can
// have less or more. The overhead is small, but should be calculated and measured for
// a production app.
const MAX_TOPIC_COMPACTION_KEYS: usize = 1_000;

// Declare the entity

#[derive(Default)]
pub struct State {
    history: VecDeque<u32>,
}

pub enum Command {
    Get { reply_to: oneshot::Sender<Vec<u32>> },
    Post { temperature: u32 },
}

#[derive(Clone, Deserialize, Serialize)]
pub enum Event {
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

            Command::Post { temperature } => {
                emit_event(Event::TemperatureRead { temperature }).boxed()
            }

            _ => unhandled(),
        }
    }

    fn on_event(
        _context: &akka_persistence_rs::entity::Context,
        state: &mut Self::State,
        event: &Self::Event,
    ) {
        let Event::TemperatureRead { temperature } = event;

        if state.history.len() == MAX_HISTORY_EVENTS {
            state.history.pop_front();
        }
        state.history.push_back(*temperature);
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

struct RecordMarshaler;

// Our event keys will occupy the top 12 bits of the key, meaning
// that we can have 4K types of record. We use 32 of the bottom 52
// bits as the entity id. We choose 32 bits as this is a common size
// for identifiers transmitted from IoT sensors. These identifiers
// are also known as "device addresses" and represent an address
// which may, in turn, equate to a 64 bit address globally unique
// to a device. These globally unique addresses are not generally
// transmitted in order to conserve packet size.
const EVENT_TYPE_BIT_SHIFT: usize = 52;
const EVENT_ID_BIT_MASK: u64 = 0xFFFFFFFF;

impl FileLogRecordMarshaler<Event> for RecordMarshaler {
    fn to_compaction_key(record: &Record<Event>) -> Option<Key> {
        let entity_id = record.entity_id.parse::<u32>().ok()?;
        let Event::TemperatureRead { .. } = record.event;
        Some(0 << EVENT_TYPE_BIT_SHIFT | entity_id as u64)
    }

    fn to_entity_id(record: &streambed::commit_log::ConsumerRecord) -> Option<EntityId> {
        let entity_id = (record.key & EVENT_ID_BIT_MASK) as u32;
        Some(entity_id.to_string())
    }
}

/// Manage the temperature sensor.
pub async fn task(
    mut cl: FileLog,
    command_receiver: mpsc::Receiver<Message<Command>>,
) -> Result<(), JoinError> {
    // We register a compaction strategy for our topic such that when we use up
    // 64KB of disk space (the default), we will run compaction so that unwanted
    // events are removed. In our scenario, unwanted events can be removed when
    // the exceed MAX_EVENTS_TO_REPLY as we do not have a requirement to ever
    // return more than that.
    cl.register_compaction(
        EVENTS_TOPIC.to_string(),
        NthKeyBasedRetention::new(MAX_TOPIC_COMPACTION_KEYS, MAX_HISTORY_EVENTS),
    )
    .await
    .unwrap();

    let file_log_topic_adapter =
        FileLogTopicAdapter::new(cl, RecordMarshaler, "iot-service", EVENTS_TOPIC);

    EntityManager::new(Behavior, file_log_topic_adapter, command_receiver)
        .join()
        .await
}
