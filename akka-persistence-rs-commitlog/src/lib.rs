#![doc = include_str!("../README.md")]

use akka_persistence_rs::{
    entity_manager::{EventEnvelope as EntityManagerEventEnvelope, Handler, SourceProvider},
    EntityId, Offset, TimestampOffset, WithEntityId, WithOffset, WithSeqNr, WithTimestampOffset,
};
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::{io, marker::PhantomData, pin::Pin, sync::Arc};
use streambed::{
    commit_log::{
        CommitLog, ConsumerRecord, Key, Offset as CommitLogOffset, ProducerRecord, Subscription,
        Topic,
    },
    secret_store::SecretStore,
};
use tokio_stream::{Stream, StreamExt};

/// An envelope wraps a commit log event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub entity_id: EntityId,
    pub seq_nr: u64,
    pub timestamp: DateTime<Utc>,
    pub event: E,
    pub offset: CommitLogOffset,
}

impl<E> EventEnvelope<E> {
    pub fn new<EI>(
        entity_id: EI,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: E,
        offset: CommitLogOffset,
    ) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            seq_nr,
            timestamp,
            event,
            offset,
        }
    }
}

impl<E> WithEntityId for EventEnvelope<E> {
    fn entity_id(&self) -> EntityId {
        self.entity_id.clone()
    }
}

impl<E> WithOffset for EventEnvelope<E> {
    fn offset(&self) -> Offset {
        Offset::Sequence(self.offset)
    }
}

impl<E> WithSeqNr for EventEnvelope<E> {
    fn seq_nr(&self) -> u64 {
        self.seq_nr
    }
}

impl<E> WithTimestampOffset for EventEnvelope<E> {
    fn timestamp_offset(&self) -> TimestampOffset {
        TimestampOffset {
            timestamp: self.timestamp,
            // FIXME: Is this correct?
            seen: vec![],
        }
    }
}

/// Provides the ability to transform the the memory representation of Akka Persistence events from
/// and to the records that a CommitLog expects. Given the "cbor" feature, we use CBOR for serialization.
/// Encryption/decryption to commit log records is also applied. Therefore a secret store is expected.
#[async_trait]
pub trait CommitLogEventEnvelopeMarshaler<E>
where
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type SecretStore: SecretStore;

    /// Provide a key we can use for the purposes of log compaction.
    /// A key would generally comprise and event type value held in
    /// the high bits, and the entity id in the lower bits.
    fn to_compaction_key(entity_id: &EntityId, event: &E) -> Option<Key>;

    /// Extract an entity id from a consumer envelope.
    fn to_entity_id(record: &ConsumerRecord) -> Option<EntityId>;

    /// Return a reference to a secret store for encryption/decryption.
    fn secret_store(&self) -> &Self::SecretStore;

    /// Return a path to use for looking up secrets with respect to
    /// an entity being encrypted/decrypted.
    fn secret_path(&self, entity_id: &EntityId) -> Arc<str>;

    #[cfg(feature = "cbor")]
    async fn envelope(
        &self,
        entity_id: EntityId,
        mut record: ConsumerRecord,
    ) -> Option<EventEnvelope<E>> {
        use streambed::commit_log::{Header, HeaderKey};

        streambed::decrypt_buf(
            self.secret_store(),
            &self.secret_path(&entity_id),
            &mut record.value,
            |value| ciborium::de::from_reader(value),
        )
        .await
        .and_then(|event| {
            let seq_nr = record.headers.iter().find_map(|Header { key, value }| {
                if key == &HeaderKey::from("seq_nr") {
                    if value.len() >= 8 {
                        if let Ok(value) = value[0..8].try_into() {
                            Some(u64::from_be_bytes(value))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            });
            seq_nr.and_then(|seq_nr| {
                record.timestamp.map(|timestamp| {
                    EventEnvelope::new(entity_id, seq_nr, timestamp, event, record.offset)
                })
            })
        })
    }

    #[cfg(not(feature = "cbor"))]
    async fn envelope(
        &self,
        entity_id: EntityId,
        record: ConsumerRecord,
    ) -> Option<EventEnvelope<E>>;

    #[cfg(feature = "cbor")]
    async fn producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: E,
    ) -> Option<ProducerRecord> {
        use streambed::commit_log::{Header, HeaderKey};

        let key = Self::to_compaction_key(&entity_id, &event)?;
        let buf = streambed::encrypt_struct(
            self.secret_store(),
            &self.secret_path(&entity_id),
            |event| {
                let mut buf = Vec::new();
                ciborium::ser::into_writer(event, &mut buf).map(|_| buf)
            },
            rand::thread_rng,
            &event,
        )
        .await?;
        Some(ProducerRecord {
            topic,
            headers: vec![Header {
                key: HeaderKey::from("seq_nr"),
                value: u64::to_be_bytes(seq_nr).to_vec(),
            }],
            timestamp: Some(timestamp),
            key,
            value: buf,
            partition: 0,
        })
    }

    #[cfg(not(feature = "cbor"))]
    async fn producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: E,
    ) -> Option<ProducerRecord>;
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
/// Developers are required to provide implementations of [CommitLogEventEnvelopeMarshaler]
/// for bytes and events i.e. deserialization/decryption and
/// serialization/encryption respectively, along with CommitLog's
/// use of keys for compaction including the storage of entities.
pub struct CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E>,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    commit_log: CL,
    consumer_group_name: String,
    marshaler: M,
    topic: Topic,
    phantom: PhantomData<E>,
}

impl<CL, E, M> CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E>,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    pub fn new(commit_log: CL, marshaler: M, consumer_group_name: &str, topic: Topic) -> Self {
        Self {
            commit_log,
            consumer_group_name: consumer_group_name.into(),
            marshaler,
            topic,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<CL, E, M> SourceProvider<E> for CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E> + Send + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    async fn source_initial(
        &mut self,
    ) -> io::Result<Pin<Box<dyn Stream<Item = EntityManagerEventEnvelope<E>> + Send + 'async_trait>>>
    {
        let consumer_records = produce_to_last_offset(
            &self.commit_log,
            &self.consumer_group_name,
            self.topic.clone(),
        )
        .await;

        let marshaler = &self.marshaler;

        if let Ok(mut consumer_records) = consumer_records {
            Ok(Box::pin(stream!({
                while let Some(consumer_record) = consumer_records.next().await {
                    if let Some(record_entity_id) = M::to_entity_id(&consumer_record) {
                        if let Some(envelope) =
                            marshaler.envelope(record_entity_id, consumer_record).await
                        {
                            yield EntityManagerEventEnvelope::new(
                                envelope.entity_id,
                                envelope.seq_nr,
                                envelope.timestamp,
                                envelope.event,
                            );
                        }
                    }
                }
            })))
        } else {
            Ok(Box::pin(tokio_stream::empty()))
        }
    }

    async fn source(
        &mut self,
        entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = EntityManagerEventEnvelope<E>> + Send + 'async_trait>>>
    {
        let consumer_records = produce_to_last_offset(
            &self.commit_log,
            &self.consumer_group_name,
            self.topic.clone(),
        )
        .await;

        let marshaler = &self.marshaler;

        if let Ok(mut consumer_records) = consumer_records {
            Ok(Box::pin(stream!({
                while let Some(consumer_record) = consumer_records.next().await {
                    if let Some(record_entity_id) = M::to_entity_id(&consumer_record) {
                        if &record_entity_id == entity_id {
                            if let Some(envelope) =
                                marshaler.envelope(record_entity_id, consumer_record).await
                            {
                                yield EntityManagerEventEnvelope::new(
                                    envelope.entity_id,
                                    envelope.seq_nr,
                                    envelope.timestamp,
                                    envelope.event,
                                );
                            }
                        }
                    }
                }
            })))
        } else {
            Ok(Box::pin(tokio_stream::empty()))
        }
    }
}

async fn produce_to_last_offset<'async_trait>(
    commit_log: &'async_trait impl CommitLog,
    consumer_group_name: &str,
    topic: Topic,
) -> io::Result<Pin<Box<dyn Stream<Item = ConsumerRecord> + Send + 'async_trait>>> {
    let last_offset = commit_log
        .offsets(topic.clone(), 0)
        .await
        .map(|lo| lo.end_offset);

    if let Some(last_offset) = last_offset {
        let subscriptions = vec![Subscription { topic }];

        let mut records =
            commit_log.scoped_subscribe(consumer_group_name, vec![], subscriptions, None);

        Ok(Box::pin(stream!({
            while let Some(record) = records.next().await {
                if record.offset <= last_offset {
                    let is_last_offset = record.offset == last_offset;
                    yield record;
                    if !is_last_offset {
                        continue;
                    }
                }
                break;
            }
        })))
    } else {
        Ok(Box::pin(tokio_stream::empty()))
    }
}

#[async_trait]
impl<CL, E, M> Handler<E> for CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E> + Send + Sync,
    for<'async_trait> E: Clone + DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    async fn process(
        &mut self,
        envelope: EntityManagerEventEnvelope<E>,
    ) -> io::Result<EntityManagerEventEnvelope<E>> {
        let producer_record = self
            .marshaler
            .producer_record(
                self.topic.clone(),
                envelope.entity_id.clone(),
                envelope.seq_nr,
                envelope.timestamp,
                envelope.event.clone(),
            )
            .await
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "A problem occurred converting a envelope when producing",
                )
            })?;
        self.commit_log
            .produce(producer_record)
            .await
            .map(|_| envelope)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "A problem occurred producing a envelope",
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, num::NonZeroUsize, time::Duration};

    use super::*;
    use akka_persistence_rs::{entity::EventSourcedBehavior, entity_manager};
    use serde::Deserialize;
    use streambed::{
        commit_log::{Header, HeaderKey},
        secret_store::{AppRoleAuthReply, Error, GetSecretReply, SecretData, UserPassAuthReply},
    };
    use streambed_logged::FileLog;
    use test_log::test;
    use tokio::{sync::mpsc, time};

    // Scaffolding

    #[derive(Clone, Deserialize, Serialize)]
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
            _event: Self::Event,
        ) {
            todo!()
        }
    }

    // Developers are expected to provide a marshaler of events.
    // The marshaler is responsible for more than just the serialization
    // of an envelope. Extracting/saving an entity id and determining other
    // metadata is also important. We would also expect to see any encryption
    // and decyption being performed by the marshaler.
    // The example here overrides the default methods of the marshaler and
    // effectively ignores the use of a secret key; just to prove that you really
    // can lay out an envelope any way that you would like to. Note that secret keys
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
    impl CommitLogEventEnvelopeMarshaler<MyEvent> for MyEventMarshaler {
        type SecretStore = NoopSecretStore;

        fn to_compaction_key(_entity_id: &EntityId, _event: &MyEvent) -> Option<Key> {
            panic!("should not be called")
        }

        fn to_entity_id(record: &ConsumerRecord) -> Option<EntityId> {
            let Header { value, .. } = record
                .headers
                .iter()
                .find(|header| header.key == "entity-id")?;
            std::str::from_utf8(value).ok().map(EntityId::from)
        }

        fn secret_store(&self) -> &Self::SecretStore {
            panic!("should not be called")
        }

        fn secret_path(&self, _entity_id: &EntityId) -> Arc<str> {
            panic!("should not be called")
        }

        async fn envelope(
            &self,
            entity_id: EntityId,
            record: ConsumerRecord,
        ) -> Option<EventEnvelope<MyEvent>> {
            let value = String::from_utf8(record.value).ok()?;
            let event = MyEvent { value };
            record.timestamp.map(|timestamp| EventEnvelope {
                entity_id,
                seq_nr: 1,
                timestamp,
                event,
                offset: 0,
            })
        }

        async fn producer_record(
            &self,
            topic: Topic,
            entity_id: EntityId,
            _seq_nr: u64,
            timestamp: DateTime<Utc>,
            event: MyEvent,
        ) -> Option<ProducerRecord> {
            let headers = vec![Header {
                key: HeaderKey::from("entity-id"),
                value: entity_id.as_bytes().into(),
            }];
            Some(ProducerRecord {
                topic,
                headers,
                timestamp: Some(timestamp),
                key: 0,
                value: event.value.clone().into_bytes(),
                partition: 0,
            })
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

        let marshaler = MyEventMarshaler;
        let mut adapter = CommitLogTopicAdapter::new(
            commit_log.clone(),
            marshaler,
            "some-consumer",
            Topic::from("some-topic"),
        );

        // Scaffolding

        let entity_id = EntityId::from("some-entity");
        let timestamp = Utc::now();

        // Produce a stream given no prior persistence. Should return an empty stream.

        {
            let mut envelopes = adapter.source_initial().await.unwrap();
            assert!(envelopes.next().await.is_none());
        }

        // Process some events and then produce a stream.

        let envelope = adapter
            .process(EntityManagerEventEnvelope::new(
                entity_id.clone(),
                1,
                timestamp,
                MyEvent {
                    value: "first-event".to_string(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(envelope.entity_id, entity_id);

        let envelope = adapter
            .process(EntityManagerEventEnvelope::new(
                entity_id.clone(),
                2,
                timestamp,
                MyEvent {
                    value: "second-event".to_string(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(envelope.entity_id, entity_id);

        // Produce to a different entity id, so that we can test out the filtering next.

        adapter
            .process(EntityManagerEventEnvelope::new(
                "some-other-entity-id",
                1,
                timestamp,
                MyEvent {
                    value: "third-event".to_string(),
                },
            ))
            .await
            .unwrap();

        // Wait until the number of events reported as being written is the number
        // that we have produced. We should then return those events that have been
        // produced.

        for _ in 0..10 {
            let last_offset = commit_log
                .offsets(Topic::from("some-topic"), 0)
                .await
                .map(|lo| lo.end_offset);
            if last_offset == Some(3) {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }

        {
            let mut envelopes = adapter.source(&entity_id).await.unwrap();

            let envelope = envelopes.next().await.unwrap();
            assert_eq!(envelope.entity_id, entity_id);
            assert_eq!(envelope.seq_nr, 1);
            assert_eq!(envelope.event.value, "first-event");

            let envelope = envelopes.next().await.unwrap();
            assert_eq!(envelope.entity_id, entity_id);
            assert_eq!(envelope.event.value, "second-event");

            assert!(envelopes.next().await.is_none());
        }
    }

    #[test(tokio::test)]
    async fn can_establish_an_entity_manager() {
        let commit_log = FileLog::new("/dev/null");

        let marshaler = MyEventMarshaler;

        let file_log_topic_adapter = CommitLogTopicAdapter::new(
            commit_log,
            marshaler,
            "some-consumer",
            Topic::from("some-topic"),
        );

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
