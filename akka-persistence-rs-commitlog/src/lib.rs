#![doc = include_str!("../README.md")]

use akka_persistence_rs::{entity_manager::RecordAdapter, EntityId, Record};
use async_stream::stream;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{io, marker::PhantomData, pin::Pin};
use streambed::{
    commit_log::{CommitLog, ConsumerRecord, Key, ProducerRecord, Subscription, Topic, TopicRef},
    secret_store::SecretStore,
};
use tokio_stream::{Stream, StreamExt};

/// Provides the ability to transform the the memory representation of Akka Persistence records from
/// and to the records that a CommitLog expects. Given the "cbor" feature, we use CBOR for serialization.
/// Encryption/decryption to commit log records is also applied. Therefore a secret store is expected.
#[async_trait]
pub trait CommitLogRecordMarshaler<E>
where
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type SecretStore: SecretStore;

    /// Provide a key we can use for the purposes of log compaction.
    /// A key would generally comprise and event type value held in
    /// the high bits, and the entity id in the lower bits.
    fn to_compaction_key(record: &Record<E>) -> Option<Key>;

    /// Extract an entity id from a consumer record.
    fn to_entity_id(record: &ConsumerRecord) -> Option<EntityId>;

    /// Return a reference to a secret store for encryption/decryption.
    fn secret_store(&self) -> &Self::SecretStore;

    /// Return a path to use for looking up secrets with respect to
    /// an entity being encrypted/decrypted.
    fn secret_path(&self, entity_id: &EntityId) -> String;

    #[cfg(feature = "cbor")]
    async fn record(&self, mut record: ConsumerRecord) -> Option<Record<E>> {
        let entity_id = Self::to_entity_id(&record)?;
        streambed::decrypt_buf(
            self.secret_store(),
            &self.secret_path(&entity_id),
            &mut record.value,
            |value| ciborium::de::from_reader(value),
        )
        .await
        .map(|event| Record::new(entity_id, event))
    }

    #[cfg(not(feature = "cbor"))]
    async fn record(&self, record: ConsumerRecord) -> Option<Record<E>>;

    #[cfg(feature = "cbor")]
    async fn producer_record(
        &self,
        topic: Topic,
        record: Record<E>,
    ) -> Option<(ProducerRecord, Record<E>)> {
        let key = Self::to_compaction_key(&record)?;
        let buf = streambed::encrypt_struct(
            self.secret_store(),
            &self.secret_path(&record.entity_id),
            |event| {
                let mut buf = Vec::new();
                ciborium::ser::into_writer(event, &mut buf).map(|_| buf)
            },
            rand::thread_rng,
            &record.event,
        )
        .await?;
        Some((
            ProducerRecord {
                topic,
                headers: vec![],
                timestamp: None,
                key,
                value: buf,
                partition: 0,
            },
            record,
        ))
    }

    #[cfg(not(feature = "cbor"))]
    async fn producer_record(
        &self,
        topic: Topic,
        record: Record<E>,
    ) -> Option<(ProducerRecord, Record<E>)>;
}

/// Adapts a Streambed CommitLog for use with Akka Persistence.
/// This adapter retains an instance of a CommitLog and is
/// associated with a specific topic. A topic maps one-to-one
/// with a entity type i.e. many entity instances are held
/// within one topic.
///
/// As CommitLog is intended for use at the edge, we assume
/// that all entities will be event sourced into memory.
///
/// Developers are required to provide implementations of [CommitLogRecordMarshaler]
/// for bytes and records i.e. deserialization/decryption and
/// serialization/encryption respectively, along with CommitLog's
/// use of keys for compaction including the storage of entities.
pub struct CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogRecordMarshaler<E>,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    commit_log: CL,
    consumer_group_name: String,
    marshaller: M,
    topic: Topic,
    phantom: PhantomData<E>,
}

impl<CL, E, M> CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogRecordMarshaler<E>,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    pub fn new(commit_log: CL, marshaller: M, consumer_group_name: &str, topic: TopicRef) -> Self {
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
impl<CL, E, M> RecordAdapter<E> for CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogRecordMarshaler<E> + Send + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
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
                        if let Some(record) = marshaller.record(record).await {
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
        entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = Record<E>> + Send + 'async_trait>>> {
        let records = self.produce_initial().await;
        if let Ok(mut records) = records {
            Ok(Box::pin(stream!({
                while let Some(record) = records.next().await {
                    if &record.entity_id == entity_id {
                        yield record;
                    }
                }
            })))
        } else {
            Ok(Box::pin(tokio_stream::empty()))
        }
    }

    async fn process(&mut self, record: Record<E>) -> io::Result<Record<E>> {
        let (producer_record, record) = self
            .marshaller
            .producer_record(self.topic.clone(), record)
            .await
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
    use std::{env, fs, num::NonZeroUsize, time::Duration};

    use super::*;
    use akka_persistence_rs::{entity::EventSourcedBehavior, entity_manager, RecordMetadata};
    use serde::Deserialize;
    use streambed::{
        commit_log::Header,
        secret_store::{AppRoleAuthReply, Error, GetSecretReply, SecretData, UserPassAuthReply},
    };
    use streambed_logged::FileLog;
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
    // effectively ignores the use of a secret key; just to prove that you really
    // can lay out a record any way that you would like to. Note that secret keys
    // are important though.

    #[derive(Clone)]
    struct NoopSecretStore;

    #[async_trait]
    impl SecretStore for NoopSecretStore {
        async fn approle_auth(
            &self,
            _role_id: &str,
            _secret_id: &str,
        ) -> Result<AppRoleAuthReply, Error> {
            panic!("should not be called")
        }

        async fn create_secret(
            &self,
            _secret_path: &str,
            _secret_data: SecretData,
        ) -> Result<(), Error> {
            panic!("should not be called")
        }

        async fn get_secret(&self, _secret_path: &str) -> Result<Option<GetSecretReply>, Error> {
            panic!("should not be called")
        }

        async fn token_auth(&self, _token: &str) -> Result<(), Error> {
            panic!("should not be called")
        }

        async fn userpass_auth(
            &self,
            _username: &str,
            _password: &str,
        ) -> Result<UserPassAuthReply, Error> {
            panic!("should not be called")
        }

        async fn userpass_create_update_user(
            &self,
            _current_username: &str,
            _username: &str,
            _password: &str,
        ) -> Result<(), Error> {
            panic!("should not be called")
        }
    }

    struct MyEventMarshaler;

    #[async_trait]
    impl CommitLogRecordMarshaler<MyEvent> for MyEventMarshaler {
        type SecretStore = NoopSecretStore;

        fn to_compaction_key(_record: &Record<MyEvent>) -> Option<Key> {
            panic!("should not be called")
        }

        fn to_entity_id(_record: &ConsumerRecord) -> Option<EntityId> {
            panic!("should not be called")
        }

        fn secret_store(&self) -> &Self::SecretStore {
            panic!("should not be called")
        }

        fn secret_path(&self, _entity_id: &EntityId) -> String {
            panic!("should not be called")
        }

        async fn record(&self, record: ConsumerRecord) -> Option<Record<MyEvent>> {
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

        async fn producer_record(
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
        let mut adapter = CommitLogTopicAdapter::new(
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

        // Produce to a different entity id, so that we can test out the filtering next.

        adapter
            .process(Record::new(
                "some-other-entity-id",
                MyEvent {
                    value: "third-event".to_string(),
                },
            ))
            .await
            .unwrap();

        // Wait until the number of records reported as being written is the number
        // that we have produced. We should then return those events that have been
        // produced.

        for _ in 0..10 {
            let last_offset = commit_log
                .offsets("some-topic".to_string(), 0)
                .await
                .map(|lo| lo.end_offset);
            if last_offset == Some(3) {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }

        {
            let mut records = adapter.produce(&entity_id).await.unwrap();

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
            CommitLogTopicAdapter::new(commit_log, marshaller, "some-consumer", "some-topic");

        let my_behavior = MyBehavior;

        let (_, my_command_receiver) = mpsc::channel(10);

        entity_manager::run(
            my_behavior,
            file_log_topic_adapter,
            my_command_receiver,
            NonZeroUsize::new(1).unwrap(),
        )
        .await;
    }
}
