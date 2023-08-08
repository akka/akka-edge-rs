use std::collections::VecDeque;

use akka_persistence_rs::{
    effect::{emit_event, reply, unhandled, EffectExt},
    entity::EventSourcedBehavior,
    entity_manager::EntityManager,
    Message, Record,
};
use akka_persistence_rs_filelog::{FileLogRecordMarshaler, FileLogTopicAdapter};
use serde::{Deserialize, Serialize};
use streambed::commit_log::{ConsumerRecord, Key, ProducerRecord, Topic};
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

// Declare how we marshal our data with FileLog.

// Our event keys will occupy the top 12 bits of the key, meaning
// that we can have 4K types of record. We use 32 of the bottom 52
// bits as an id.
const EVENT_TYPE_BIT_SHIFT: usize = 52;
const DEV_ADDR_BIT_MASK: u64 = 0xFFFFFFFF;

pub(crate) fn compaction_key(entity_id: &str, event: &Event) -> Key {
    let dev_addr = entity_id.parse::<u32>().unwrap();
    let Event::TemperatureRead { .. } = event;
    0 << EVENT_TYPE_BIT_SHIFT | dev_addr as u64
}

struct RecordMarshaler;

impl FileLogRecordMarshaler<Event> for RecordMarshaler {
    fn record(&self, record: ConsumerRecord) -> Option<Record<Event>> {
        let dev_addr = (record.key & DEV_ADDR_BIT_MASK) as u32;
        let entity_id = dev_addr.to_string();
        ciborium::de::from_reader::<Event, _>(&*record.value)
            .map(|event| Record::new(entity_id, event))
            .ok()
    }

    fn producer_record(
        &self,
        topic: Topic,
        record: Record<Event>,
    ) -> Option<(ProducerRecord, Record<Event>)> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&record.event, &mut buf).ok()?;
        Some((
            ProducerRecord {
                topic,
                headers: vec![],
                timestamp: None,
                key: compaction_key(&record.entity_id, &record.event),
                value: buf,
                partition: 0,
            },
            record,
        ))
    }
}

/// Manage the temperature sensor. We use CBOR as the serialization type for the data
/// we persist to the log. We do this as it has the benefits of JSON in terms
/// of schema evolution, but is faster to serialize and represents itself as
/// smaller on disk.
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
