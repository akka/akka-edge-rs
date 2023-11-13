//! An offset store for use with the Streambed commit log.

use akka_persistence_rs::{
    effect::{persist_event, reply, Effect, EffectExt},
    entity::{Context, EventSourcedBehavior},
    entity_manager, EntityId, EntityType, Message, Offset, PersistenceId,
};
use akka_persistence_rs_commitlog::{CommitLogMarshaller, CommitLogTopicAdapter, EventEnvelope};
use akka_projection_rs::offset_store::{self, LastOffset};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::{cmp::Ordering, sync::Arc};
use std::{future::Future, io};
use streambed::commit_log::{ConsumerRecord, Header, HeaderKey, Key, ProducerRecord, Topic};
use streambed_logged::{compaction::KeyBasedRetention, FileLog};
use tokio::sync::{mpsc, oneshot, watch, Notify};

mod internal {
    use std::sync::Arc;

    use akka_persistence_rs_commitlog::{CannotConsume, CannotProduce};
    use tokio::sync::Notify;

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
        pub ready: Arc<Notify>,
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
                } => persist_event(Event::Saved {
                    persistence_id: persistence_id.clone(),
                    offset,
                })
                .and_then(|behavior: &Self, state, result| {
                    if result.is_ok() {
                        behavior.update_last_offset(state, persistence_id);
                    }
                    async { result }
                })
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

        async fn on_recovery_completed(&self, _context: &Context, state: &Self::State) {
            for (persistence_id, _) in state.offsets.iter() {
                self.update_last_offset(Some(state), persistence_id.clone());
            }
        }

        async fn on_initial_recovery_completed(&self) {
            self.ready.notify_one();
        }
    }

    pub struct OffsetStoreEventMarshaller<F> {
        pub entity_type: EntityType,
        pub to_compaction_key: F,
    }

    #[async_trait]
    impl<F> CommitLogMarshaller<Event> for OffsetStoreEventMarshaller<F>
    where
        F: Fn(&EntityId, &Event) -> Key + Send + Sync,
    {
        fn entity_type(&self) -> EntityType {
            self.entity_type.clone()
        }

        fn to_compaction_key(&self, entity_id: &EntityId, event: &Event) -> Key {
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
        ) -> Result<EventEnvelope<Event>, CannotConsume> {
            let event = ciborium::de::from_reader(record.value.as_slice()).map_err(|e| {
                CannotConsume::new(
                    entity_id.clone(),
                    format!("CBOR deserialization issue: {e}"),
                )
            })?;
            let envelope = record.timestamp.map(|timestamp| EventEnvelope {
                persistence_id: PersistenceId::new(self.entity_type(), entity_id.clone()),
                seq_nr: 0, // We don't care about sequence numbers with the offset store as they won't be projected anywhere
                timestamp,
                event,
                offset: record.offset,
                tags: vec![],
            });
            envelope.ok_or(CannotConsume::new(entity_id, "No timestamp"))
        }

        async fn producer_record(
            &self,
            topic: Topic,
            entity_id: EntityId,
            _seq_nr: u64,
            timestamp: DateTime<Utc>,
            event: &Event,
        ) -> Result<ProducerRecord, CannotProduce> {
            let mut value = Vec::new();
            ciborium::ser::into_writer(event, &mut value).map_err(|e| {
                CannotProduce::new(entity_id.clone(), format!("CBOR serialization issue: {e}"))
            })?;

            let headers = vec![Header {
                key: HeaderKey::from("entity-id"),
                value: entity_id.as_bytes().into(),
            }];
            let key = self.to_compaction_key(&entity_id, event);
            Ok(ProducerRecord {
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

/// Provides an asynchronous task and a command channel that can run and drive an offset store.
pub fn task(
    mut commit_log: FileLog,
    keys_expected: usize,
    offset_store_id: OffsetStoreId,
) -> (
    impl Future<Output = io::Result<()>>,
    mpsc::Sender<offset_store::Command>,
) {
    let (offset_store, mut offset_store_receiver) = mpsc::channel(1);

    let task = async move {
        let events_entity_type = EntityType::from(offset_store_id.clone());
        let events_topic = Topic::from(offset_store_id.clone());

        commit_log
            .register_compaction(events_topic.clone(), KeyBasedRetention::new(keys_expected))
            .await
            .unwrap();

        let to_compaction_key = |_: &EntityId, event: &Event| -> Key {
            let Event::Saved { persistence_id, .. } = event;
            persistence_id.jdk_string_hash() as u64
        };

        let file_log_topic_adapter = CommitLogTopicAdapter::new(
            commit_log,
            OffsetStoreEventMarshaller {
                entity_type: events_entity_type,
                to_compaction_key,
            },
            &offset_store_id,
            events_topic,
        );

        let (last_offset, last_offset_receiver) = watch::channel(None);
        let ready = Arc::new(Notify::new());

        let (entity_manager_runner, offset_store_entities) = entity_manager::task(
            Behavior {
                last_offset,
                ready: ready.clone(),
            },
            file_log_topic_adapter,
            keys_expected,
            keys_expected,
        );

        let offset_command_handler = async {
            ready.notified().await;
            ready.notify_one();
            while let Some(command) = offset_store_receiver.recv().await {
                match command {
                    offset_store::Command::GetLastOffset { reply_to } => {
                        let _ = reply_to.send(last_offset_receiver.borrow().clone());
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

        let (_, r2) = tokio::join!(offset_command_handler, entity_manager_runner);
        r2
    };
    (task, offset_store)
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use super::*;

    use akka_persistence_rs::TimestampOffset;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_get_last_offsets() {
        let logged_dir = env::temp_dir().join("test_get_last_offsets");
        let _ = fs::remove_dir_all(&logged_dir);
        let _ = fs::create_dir_all(&logged_dir);
        println!("Writing to {}", logged_dir.to_string_lossy());

        let commit_log = FileLog::new(logged_dir);

        // Shouldn't be any offsets right now

        let (offset_store_task, offset_store) =
            task(commit_log, 10, OffsetStoreId::from("some-offset-id"));
        tokio::spawn(offset_store_task);

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .unwrap();
        assert_eq!(reply_to_receiver.await, Ok(None));

        // Save an offset

        let persistence_id_0 = PersistenceId::new(
            EntityType::from("entity-type"),
            EntityId::from("entity-id0"),
        );

        let timestamp_0 = Utc::now();
        let seq_nr_0 = 2;

        offset_store
            .send(offset_store::Command::SaveOffset {
                persistence_id: persistence_id_0.clone(),
                offset: Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_0,
                    seq_nr: seq_nr_0,
                }),
            })
            .await
            .unwrap();

        // We should be able to read that offset back

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetOffset {
                persistence_id: persistence_id_0.clone(),
                reply_to,
            })
            .await
            .unwrap();
        assert_eq!(
            reply_to_receiver.await,
            Ok(Some(Offset::Timestamp(TimestampOffset {
                timestamp: timestamp_0,
                seq_nr: seq_nr_0
            })))
        );

        // The latest offset should now reflect the one we just added.

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .unwrap();
        assert_eq!(
            reply_to_receiver.await,
            Ok(Some((
                vec![persistence_id_0.clone()],
                Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_0,
                    seq_nr: seq_nr_0
                })
            )))
        );

        // Add another for the same persistence id that has an earlier timestamp. Shouldn't affect the
        // latest one.

        let timestamp_1 = timestamp_0 - chrono::Duration::minutes(1);
        let seq_nr_1 = 1;

        offset_store
            .send(offset_store::Command::SaveOffset {
                persistence_id: persistence_id_0.clone(),
                offset: Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_1,
                    seq_nr: seq_nr_1,
                }),
            })
            .await
            .unwrap();

        // The latest offset should be as it was before.

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .unwrap();
        assert_eq!(
            reply_to_receiver.await,
            Ok(Some((
                vec![persistence_id_0.clone()],
                Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_0,
                    seq_nr: seq_nr_0
                })
            )))
        );

        // Add another for the same persistence id that has an later timestamp.

        let timestamp_2 = timestamp_0 + chrono::Duration::minutes(1);
        let seq_nr_2 = 3;

        offset_store
            .send(offset_store::Command::SaveOffset {
                persistence_id: persistence_id_0.clone(),
                offset: Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_2,
                    seq_nr: seq_nr_2,
                }),
            })
            .await
            .unwrap();

        // We should be able to read that offset back

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetOffset {
                persistence_id: persistence_id_0.clone(),
                reply_to,
            })
            .await
            .unwrap();
        assert_eq!(
            reply_to_receiver.await,
            Ok(Some(Offset::Timestamp(TimestampOffset {
                timestamp: timestamp_2,
                seq_nr: seq_nr_2
            })))
        );

        // The latest offset should now be as per the the above.

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .unwrap();
        assert_eq!(
            reply_to_receiver.await,
            Ok(Some((
                vec![persistence_id_0.clone()],
                Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_2,
                    seq_nr: seq_nr_2
                })
            )))
        );

        // Add another for a new persistence id that has an earlier timestamp.

        let persistence_id_1 = PersistenceId::new(
            EntityType::from("entity-type"),
            EntityId::from("entity-id1"),
        );

        let timestamp_3 = timestamp_0 - chrono::Duration::minutes(1);
        let seq_nr_3 = 1;

        offset_store
            .send(offset_store::Command::SaveOffset {
                persistence_id: persistence_id_1,
                offset: Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_3,
                    seq_nr: seq_nr_3,
                }),
            })
            .await
            .unwrap();

        // The latest offset should not have changed.

        let (reply_to, reply_to_receiver) = oneshot::channel();
        offset_store
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .unwrap();
        assert_eq!(
            reply_to_receiver.await,
            Ok(Some((
                vec![persistence_id_0],
                Offset::Timestamp(TimestampOffset {
                    timestamp: timestamp_2,
                    seq_nr: seq_nr_2
                })
            )))
        );
    }
}
