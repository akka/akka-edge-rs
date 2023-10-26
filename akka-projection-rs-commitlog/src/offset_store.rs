//! An offset store for use with the Streambed commit log.

use akka_persistence_rs::{
    effect::{emit_event, reply, then, Effect, EffectExt},
    entity::{Context, EventSourcedBehavior},
    entity_manager, EntityId, EntityType, Message, Offset, PersistenceId,
};
use akka_persistence_rs_commitlog::{CommitLogMarshaler, CommitLogTopicAdapter, EventEnvelope};
use akka_projection_rs::offset_store::{self, LastOffset};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::cmp::Ordering;
use std::{io, num::NonZeroUsize};
use streambed::commit_log::{ConsumerRecord, Header, HeaderKey, Key, ProducerRecord, Topic};
use streambed_logged::{compaction::KeyBasedRetention, FileLog};
use tokio::sync::{mpsc, oneshot, watch};

mod internal {
    use super::*;

    #[derive(Clone, Default)]
    pub struct State {
        pub offsets: Vec<(PersistenceId, Offset)>,
    }

    pub enum Command {
        Get {
            persistence_id: PersistenceId,
            reply_to: oneshot::Sender<Option<Offset>>,
        },
        Save {
            persistence_id: PersistenceId,
            offset: Offset,
        },
    }

    #[derive(Clone, Deserialize, Serialize)]
    pub enum Event {
        Saved {
            persistence_id: PersistenceId,
            offset: Offset,
        },
    }

    pub struct Behavior {
        pub last_offset: watch::Sender<Option<LastOffset>>,
    }

    const HASH_COLLISION_WARNING_THRESHOLD: usize = 10;

    impl Behavior {
        // Updates the overall last offset for all of the entities being managed here.
        fn update_last_offset(
            &self,
            state: Option<&<Behavior as EventSourcedBehavior>::State>,
            persistence_id: PersistenceId,
        ) {
            self.last_offset.send_if_modified(|last_offset| {
            if let Some(state) = state {
                let state_offset = state.offsets.iter().find_map(|(pid, offset)| {
                    if pid == &persistence_id {
                        Some(offset.clone())
                    } else {
                        None
                    }
                });

                match (state_offset, last_offset) {
                    (Some(state_offset), Some((pids, last_offset))) => {
                        match state_offset.partial_cmp(last_offset) {
                            Some(Ordering::Equal) => {
                                if !pids.contains(&persistence_id) {
                                    let pids_len = pids.len();
                                    if pids_len < HASH_COLLISION_WARNING_THRESHOLD {
                                        pids.push(persistence_id);
                                        true
                                    } else {
                                        log::warn!("Exceeded hash collision threshold with {pids_len} entities sharing the same timestamp for the latest offset. Discarding {persistence_id} from here.");
                                        false
                                    }
                                } else {
                                    false
                                }
                            }
                            Some(Ordering::Greater) => {
                                *pids = vec![persistence_id];
                                *last_offset = state_offset;
                                true
                            }
                            Some(Ordering::Less) => false,
                            None => false,
                        }
                    }
                    (None, None) => false,
                    (None, Some(_)) => false,
                    (Some(state_offset), last_offset @ None) => {
                        *last_offset = Some((vec![persistence_id], state_offset));
                        true
                    }
                }
            } else {
                false
            }
        });
        }
    }

    #[async_trait]
    impl EventSourcedBehavior for Behavior {
        type State = State;
        type Command = Command;
        type Event = Event;

        fn for_command(
            _context: &Context,
            state: &Self::State,
            command: Self::Command,
        ) -> Box<dyn Effect<Self>> {
            match command {
                Command::Get {
                    persistence_id,
                    reply_to,
                } => {
                    let offset = state.offsets.iter().find_map(|(pid, offset)| {
                        if pid == &persistence_id {
                            Some(offset.clone())
                        } else {
                            None
                        }
                    });
                    reply(reply_to, offset).boxed()
                }
                Command::Save {
                    persistence_id,
                    offset,
                } => emit_event(Event::Saved {
                    persistence_id: persistence_id.clone(),
                    offset,
                })
                .and(then(|behavior: &Self, state, result| {
                    if result.is_ok() {
                        behavior.update_last_offset(state, persistence_id);
                    }
                    async { result }
                }))
                .boxed(),
            }
        }

        fn on_event(_context: &Context, state: &mut Self::State, event: Self::Event) {
            let Event::Saved {
                persistence_id,
                offset,
            } = event;
            match state
                .offsets
                .iter_mut()
                .find(|(pid, _)| pid == &persistence_id)
            {
                Some((_, o)) => *o = offset,
                None => {
                    let state_offsets_len = state.offsets.len();
                    if state_offsets_len < HASH_COLLISION_WARNING_THRESHOLD {
                        state.offsets.push((persistence_id, offset))
                    } else {
                        log::error!("Too many entities are hashing to the same value. Discarding offset history for {persistence_id}.");
                    }
                }
            }
        }

        async fn on_recovery_completed(&self, context: &Context, state: &Self::State) {
            if let Ok(persistence_id) = context.entity_id.to_string().parse::<PersistenceId>() {
                self.update_last_offset(Some(state), persistence_id);
            }
        }
    }

    pub struct OffsetStoreEventMarshaler<F> {
        pub entity_type: EntityType,
        pub to_compaction_key: F,
    }

    #[async_trait]
    impl<F> CommitLogMarshaler<Event> for OffsetStoreEventMarshaler<F>
    where
        F: Fn(&EntityId, &Event) -> Option<Key> + Send + Sync,
    {
        fn entity_type(&self) -> EntityType {
            self.entity_type.clone()
        }

        fn to_compaction_key(&self, entity_id: &EntityId, event: &Event) -> Option<Key> {
            (self.to_compaction_key)(entity_id, event)
        }

        fn to_entity_id(&self, record: &ConsumerRecord) -> Option<EntityId> {
            let Header { value, .. } = record
                .headers
                .iter()
                .find(|header| header.key == "entity-id")?;
            std::str::from_utf8(value).ok().map(EntityId::from)
        }

        async fn envelope(
            &self,
            entity_id: EntityId,
            record: ConsumerRecord,
        ) -> Option<EventEnvelope<Event>> {
            let event = ciborium::de::from_reader(record.value.as_slice()).ok()?;
            record.timestamp.map(|timestamp| EventEnvelope {
                persistence_id: PersistenceId::new(self.entity_type(), entity_id),
                seq_nr: 0, // We don't care about sequence numbers with the offset store as they won't be projected anywhere
                timestamp,
                event,
                offset: record.offset,
                tags: vec![],
            })
        }

        async fn producer_record(
            &self,
            topic: Topic,
            entity_id: EntityId,
            _seq_nr: u64,
            timestamp: DateTime<Utc>,
            event: &Event,
        ) -> Option<ProducerRecord> {
            let mut value = Vec::new();
            ciborium::ser::into_writer(event, &mut value).ok()?;

            let headers = vec![Header {
                key: HeaderKey::from("entity-id"),
                value: entity_id.as_bytes().into(),
            }];
            let key = self.to_compaction_key(&entity_id, event)?;
            Some(ProducerRecord {
                topic,
                headers,
                timestamp: Some(timestamp),
                key,
                value,
                partition: 0,
            })
        }
    }
}

/// Uniquely identifies an offset store.
pub type OffsetStoreId = SmolStr;

use internal::*;

/// Runs an offset store.
pub async fn run(
    mut commit_log: FileLog,
    keys_expected: usize,
    offset_store_id: OffsetStoreId,
    mut offset_store_receiver: mpsc::Receiver<offset_store::Command>,
) -> io::Result<()> {
    let events_entity_type = EntityType::from(offset_store_id.clone());
    let events_topic = Topic::from(offset_store_id.clone());

    let (offset_store_entities, offset_store_entities_receiver) = mpsc::channel(keys_expected);
    let (last_offset, last_offset_reeciver) = watch::channel(None);
    let offset_command_handler = async {
        while let Some(command) = offset_store_receiver.recv().await {
            match command {
                offset_store::Command::GetLastOffset { reply_to } => {
                    let _ = reply_to.send(last_offset_reeciver.borrow().clone());
                }
                offset_store::Command::GetOffset {
                    persistence_id,
                    reply_to,
                } => {
                    let _ = offset_store_entities
                        .send(Message::new(
                            EntityId::from(persistence_id.jdk_string_hash().to_string()),
                            Command::Get {
                                persistence_id,
                                reply_to,
                            },
                        ))
                        .await;
                }
                offset_store::Command::SaveOffset {
                    persistence_id,
                    offset,
                } => {
                    let _ = offset_store_entities
                        .send(Message::new(
                            EntityId::from(persistence_id.jdk_string_hash().to_string()),
                            Command::Save {
                                persistence_id,
                                offset,
                            },
                        ))
                        .await;
                }
            }
        }
    };

    commit_log
        .register_compaction(events_topic.clone(), KeyBasedRetention::new(keys_expected))
        .await
        .unwrap();

    let to_compaction_key = |_: &EntityId, event: &Event| -> Option<Key> {
        let Event::Saved { persistence_id, .. } = event;
        Some(persistence_id.jdk_string_hash() as u64)
    };

    let file_log_topic_adapter = CommitLogTopicAdapter::new(
        commit_log,
        OffsetStoreEventMarshaler {
            entity_type: events_entity_type,
            to_compaction_key,
        },
        &offset_store_id,
        events_topic,
    );

    let entity_manager_runner = entity_manager::run(
        Behavior { last_offset },
        file_log_topic_adapter,
        offset_store_entities_receiver,
        NonZeroUsize::new(keys_expected).unwrap(),
    );

    let (_, r2) = tokio::join!(offset_command_handler, entity_manager_runner);
    r2
}
