#![doc = include_str!("../README.md")]

use akka_persistence_rs::{entity_manager::RecordAdapter, EntityId, Record};
use async_stream::stream;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{io, marker::PhantomData, pin::Pin};
use streambed::commit_log::{
    CommitLog, ConsumerRecord, Key, ProducerRecord, Subscription, Topic, TopicRef,
};
use streambed_logged::FileLog;
use tokio_stream::{Stream, StreamExt};

/// Provides the ability to transform the the memory representation of Akka Persistence records from
/// and to the records that a FileLog expects. Given the "cbor" feature, we use CBOR for serialization.
pub trait FileLogRecordMarshaler<E>
where
    E: DeserializeOwned + Serialize,
{
    /// Provide a key we can use for the purposes of log compaction.
    /// A key would generally comprise and event type value held in
    /// the high bits, and the entity id in the lower bits.
    fn to_compaction_key(record: &Record<E>) -> Option<Key>;

    /// Extract an entity id from a consumer record.
    fn to_entity_id(record: &ConsumerRecord) -> Option<EntityId>;

    #[cfg(feature = "cbor")]
    fn record(&self, record: ConsumerRecord) -> Option<Record<E>> {
        let entity_id = Self::to_entity_id(&record)?;
        ciborium::de::from_reader::<E, _>(&*record.value)
            .map(|event| Record::new(entity_id, event))
            .ok()
    }

    #[cfg(not(feature = "cbor"))]
    fn record(&self, record: ConsumerRecord) -> Option<Record<E>>;

    #[cfg(feature = "cbor")]
    fn producer_record(
        &self,
        topic: Topic,
        record: Record<E>,
    ) -> Option<(ProducerRecord, Record<E>)> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&record.event, &mut buf).ok()?;
        Some((
            ProducerRecord {
                topic,
                headers: vec![],
                timestamp: None,
                key: Self::to_compaction_key(&record)?,
                value: buf,
                partition: 0,
            },
            record,
        ))
    }

    #[cfg(not(feature = "cbor"))]
    fn producer_record(
        &self,
        topic: Topic,
        record: Record<E>,
    ) -> Option<(ProducerRecord, Record<E>)>;
}

/// Adapts a Streambed FileLog for use with Akka Persistence.
/// This adapter retains an instance of a FileLog and is
/// associated with a specific topic. A topic maps one-to-one
/// with a entity type i.e. many entity instances are held
/// within one topic.
///
/// As FileLog is intended for use at the edge, we assume
/// that all entities will be event sourced into memory.
///
/// Developers are required to provide implementations of [FileLogRecordMarshaler]
/// for bytes and records i.e. deserialization/decryption and
/// serialization/encryption respectively, along with FileLog's
/// use of keys for compaction including the storage of entities.
pub struct FileLogTopicAdapter<E, M>
where
    M: FileLogRecordMarshaler<E>,
    E: DeserializeOwned + Serialize,
{
    commit_log: FileLog,
    consumer_group_name: String,
    marshaller: M,
    topic: Topic,
    phantom: PhantomData<E>,
}

impl<E, M> FileLogTopicAdapter<E, M>
where
    M: FileLogRecordMarshaler<E>,
    E: DeserializeOwned + Serialize,
{
    pub fn new(
        commit_log: FileLog,
        marshaller: M,
        consumer_group_name: &str,
        topic: TopicRef,
    ) -> Self {
        Self {
            commit_log,
            consumer_group_name: consumer_group_name.into(),
            marshaller,
            topic: topic.into(),
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<E, M> RecordAdapter<E> for FileLogTopicAdapter<E, M>
where
    for<'async_trait> E: Send + 'async_trait,
    M: FileLogRecordMarshaler<E> + Send + Sync,
    E: DeserializeOwned + Serialize,
{
    async fn produce_initial(
        &mut self,
    ) -> io::Result<Pin<Box<dyn Stream<Item = Record<E>> + Send + 'async_trait>>> {
        let last_offset = self
            .commit_log
            .offsets(self.topic.clone(), 0)
            .await
            .map(|lo| lo.end_offset);

        if let Some(last_offset) = last_offset {
            let subscriptions = vec![Subscription {
                topic: self.topic.clone(),
            }];

            let mut records = self.commit_log.scoped_subscribe(
                &self.consumer_group_name,
                vec![],
                subscriptions,
                None,
            );

            let marshaller = &self.marshaller;

            Ok(Box::pin(stream!({
                while let Some(record) = records.next().await {
                    if record.offset <= last_offset {
                        let is_last_offset = record.offset == last_offset;
                        if let Some(record) = marshaller.record(record) {
                            yield record;
                            if !is_last_offset {
                                continue;
                            }
                        }
                    }
                    break;
                }
            })))
        } else {
            Ok(Box::pin(tokio_stream::empty()))
        }
    }

    async fn produce(
        &mut self,
        _entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = Record<E>> + Send + 'async_trait>>> {
        Ok(Box::pin(tokio_stream::empty()))
    }

    async fn process(&mut self, record: Record<E>) -> io::Result<Record<E>> {
        let (producer_record, record) = self
            .marshaller
            .producer_record(self.topic.clone(), record)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "A problem occurred converting a record when producing",
                )
            })?;
        self.commit_log
            .produce(producer_record)
            .await
            .map(|_| record)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "A problem occurred producing a record",
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, time::Duration};

    use super::*;
    use akka_persistence_rs::{
        entity::EventSourcedBehavior, entity_manager::EntityManager, RecordMetadata,
    };
    use serde::Deserialize;
    use streambed::commit_log::Header;
    use test_log::test;
    use tokio::{sync::mpsc, time};

    // Scaffolding

    #[derive(Deserialize, Serialize)]
    struct MyEvent {
        value: String,
    }

    struct MyBehavior;

    impl EventSourcedBehavior for MyBehavior {
        type State = ();

        type Command = ();

        type Event = MyEvent;

        fn for_command(
            _context: &akka_persistence_rs::entity::Context,
            _state: &Self::State,
            _command: Self::Command,
        ) -> Box<dyn akka_persistence_rs::effect::Effect<Self>> {
            todo!()
        }

        fn on_event(
            _context: &akka_persistence_rs::entity::Context,
            _state: &mut Self::State,
            _event: &Self::Event,
        ) {
            todo!()
        }
    }

    // Developers are expected to provide a marshaler of events.
    // The marshaler is responsible for more than just the serialization
    // of a record. Extracting/saving an entity id and determining other
    // metadata is also important. We would also expect to see any encryption
    // and decyption being performed by the marshaler.
    // The example here overrides the default methods of the marshaler and
    // effectively ignores the use of a key; just to prove that you really
    // can lay out a record any way that you would like to. Note that keys
    // are important though.
    struct MyEventMarshaler;

    impl FileLogRecordMarshaler<MyEvent> for MyEventMarshaler {
        fn to_compaction_key(_record: &Record<MyEvent>) -> Option<Key> {
            None
        }

        fn to_entity_id(_record: &ConsumerRecord) -> Option<EntityId> {
            None
        }

        fn record(&self, record: ConsumerRecord) -> Option<Record<MyEvent>> {
            let Header { value, .. } = record
                .headers
                .into_iter()
                .find(|header| header.key == "entity-id")?;
            let entity_id = String::from_utf8(value).ok()?;
            let value = String::from_utf8(record.value).ok()?;
            let event = MyEvent { value };
            Some(Record {
                entity_id,
                event,
                metadata: RecordMetadata {
                    deletion_event: false,
                },
            })
        }

        fn producer_record(
            &self,
            topic: Topic,
            record: Record<MyEvent>,
        ) -> Option<(ProducerRecord, Record<MyEvent>)> {
            let headers = vec![Header {
                key: "entity-id".to_string(),
                value: record.entity_id.clone().into_bytes(),
            }];
            Some((
                ProducerRecord {
                    topic,
                    headers,
                    timestamp: None,
                    key: 0,
                    value: record.event.value.clone().into_bytes(),
                    partition: 0,
                },
                record,
            ))
        }
    }

    #[test(tokio::test)]
    async fn can_source_and_flow() {
        // Set up the file log and adapter

        let logged_dir = env::temp_dir().join("can_source_and_flow");
        let _ = fs::remove_dir_all(&logged_dir);
        let _ = fs::create_dir_all(&logged_dir);
        println!("Writing to {}", logged_dir.to_string_lossy());

        let commit_log = FileLog::new(logged_dir);

        let marshaller = MyEventMarshaler;
        let mut adapter = FileLogTopicAdapter::new(
            commit_log.clone(),
            marshaller,
            "some-consumer",
            "some-topic",
        );

        // Scaffolding

        let entity_id = "some-entity".to_string();

        // Produce a stream given no prior persistence. Should return an empty stream.

        {
            let mut records = adapter.produce_initial().await.unwrap();
            assert!(records.next().await.is_none());
        }

        // Process some events and then produce a stream.

        let record = adapter
            .process(Record::new(
                entity_id.clone(),
                MyEvent {
                    value: "first-event".to_string(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(record.entity_id, entity_id);

        let record = adapter
            .process(Record::new(
                entity_id.clone(),
                MyEvent {
                    value: "second-event".to_string(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(record.entity_id, entity_id);

        // Wait until the number of records reported as being written is the number
        // that we have produced. We should then return those events that have been
        // produced.

        for _ in 0..10 {
            let last_offset = commit_log
                .offsets("some-topic".to_string(), 0)
                .await
                .map(|lo| lo.end_offset);
            if last_offset == Some(2) {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }

        {
            let mut records = adapter.produce_initial().await.unwrap();

            let record = records.next().await.unwrap();
            assert_eq!(record.entity_id, entity_id);
            assert_eq!(record.event.value, "first-event");

            let record = records.next().await.unwrap();
            assert_eq!(record.entity_id, entity_id);
            assert_eq!(record.event.value, "second-event");

            assert!(records.next().await.is_none());
        }
    }

    #[test(tokio::test)]
    async fn can_establish_an_entity_manager() {
        let commit_log = FileLog::new("/dev/null");

        let marshaller = MyEventMarshaler;

        let file_log_topic_adapter =
            FileLogTopicAdapter::new(commit_log, marshaller, "some-consumer", "some-topic");

        let my_behavior = MyBehavior;

        let (_, my_command_receiver) = mpsc::channel(10);

        EntityManager::new(my_behavior, file_log_topic_adapter, my_command_receiver);
    }
}
