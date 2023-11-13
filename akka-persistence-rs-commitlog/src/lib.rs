#![doc = include_str!("../README.md")]

use akka_persistence_rs::{
    entity_manager::{EventEnvelope as EntityManagerEventEnvelope, Handler, SourceProvider},
    EntityId, EntityType, Offset, PersistenceId, Source, Tag, WithOffset, WithPersistenceId,
    WithSeqNr, WithSource, WithTags, WithTimestamp,
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
/// Tags are not presently considered useful at the edge. A remote consumer would be interested
/// in all events from the edge in most cases, and the edge itself decides what to publish
/// (producer defined filter).
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub persistence_id: PersistenceId,
    pub seq_nr: u64,
    pub timestamp: DateTime<Utc>,
    pub event: E,
    pub offset: CommitLogOffset,
    pub tags: Vec<Tag>,
}

impl<E> WithPersistenceId for EventEnvelope<E> {
    fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
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

impl<E> WithSource for EventEnvelope<E> {
    fn source(&self) -> akka_persistence_rs::Source {
        Source::Regular
    }
}

impl<E> WithTags for EventEnvelope<E> {
    fn tags(&self) -> &[Tag] {
        &self.tags
    }
}

impl<E> WithTimestamp for EventEnvelope<E> {
    fn timestamp(&self) -> &DateTime<Utc> {
        &self.timestamp
    }
}

/// This describes an error when there has been some run-time issue in attempting to consume records.
#[derive(Debug)]
pub struct CannotConsume {
    _entity_id: EntityId,
    _cause: String,
}

impl CannotConsume {
    pub fn new<S>(entity_id: EntityId, cause: S) -> Self
    where
        S: ToString,
    {
        Self {
            _entity_id: entity_id,
            _cause: cause.to_string(),
        }
    }
}

/// This describes an error when there has been some run-time issue in attempting to produce records.
#[derive(Debug)]
pub struct CannotProduce {
    _entity_id: EntityId,
    _cause: String,
}

impl CannotProduce {
    pub fn new<S>(entity_id: EntityId, cause: S) -> Self
    where
        S: ToString,
    {
        Self {
            _entity_id: entity_id,
            _cause: cause.to_string(),
        }
    }
}

/// Provides the ability to transform the the memory representation of Akka Persistence events from
/// and to the records that a CommitLog expects.
#[async_trait]
pub trait CommitLogMarshaller<E>
where
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    /// Declares the entity type to the marshaller.
    fn entity_type(&self) -> EntityType;

    /// Provide a key we can use for the purposes of log compaction.
    /// A key would generally comprise and event type value held in
    /// the high bits, and the entity id in the lower bits.
    fn to_compaction_key(&self, entity_id: &EntityId, event: &E) -> Key;

    /// Extract an entity id from a consumer envelope.
    fn to_entity_id(&self, record: &ConsumerRecord) -> Option<EntityId>;

    /// Produce an event envelope from a consumer record.
    /// Note that this may not always be possible due to record formats having
    /// changed, in which case we want the consumer to continue and skip it.
    /// Changes in a record's layout should not prevent the system from working.
    async fn envelope(
        &self,
        entity_id: EntityId,
        record: ConsumerRecord,
    ) -> Result<EventEnvelope<E>, CannotConsume>;

    /// Produce a producer record from an event and its entity info.
    async fn producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: &E,
    ) -> Result<ProducerRecord, CannotProduce>;
}

/// Provides the ability to transform the the memory representation of Akka Persistence events from
/// and to the records that a CommitLog expects. Given the "cbor" feature, we use CBOR for serialization.
/// Encryption/decryption to commit log records is also applied. Therefore a secret store is expected.
#[async_trait]
pub trait EncryptedCommitLogMarshaller<E>: CommitLogMarshaller<E>
where
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type SecretStore: SecretStore;

    /// Return a reference to a secret store for encryption/decryption.
    fn secret_store(&self) -> &Self::SecretStore;

    /// Return a path to use for looking up secrets with respect to
    /// an entity being encrypted/decrypted.
    fn secret_path(&self, entity_id: &EntityId) -> Arc<str>;

    #[cfg(feature = "cbor")]
    async fn decrypted_envelope(
        &self,
        entity_id: EntityId,
        mut record: ConsumerRecord,
    ) -> Result<EventEnvelope<E>, CannotConsume> {
        use streambed::commit_log::{Header, HeaderKey};

        let secret_path = self.secret_path(&entity_id);
        let event = streambed::decrypt_buf(
            self.secret_store(),
            &secret_path,
            &mut record.value,
            |value| ciborium::de::from_reader(value),
        )
        .await
        .ok_or(CannotConsume::new(
            entity_id.clone(),
            format!("Cannot decrypt with key: {secret_path}"),
        ))?;

        let seq_nr = record
            .headers
            .iter()
            .find_map(|Header { key, value }| {
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
            })
            .ok_or(CannotConsume::new(
                entity_id.clone(),
                "Cannot find seq_nr header",
            ))?;

        let envelope = record.timestamp.map(|timestamp| EventEnvelope {
            persistence_id: PersistenceId::new(self.entity_type(), entity_id.clone()),
            seq_nr,
            timestamp,
            event,
            offset: record.offset,
            tags: vec![],
        });

        envelope.ok_or(CannotConsume::new(entity_id, "No timestamp"))
    }

    #[cfg(not(feature = "cbor"))]
    async fn decrypted_envelope(
        &self,
        entity_id: EntityId,
        mut record: ConsumerRecord,
    ) -> Option<EventEnvelope<E>>;

    #[cfg(feature = "cbor")]
    async fn encrypted_producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: &E,
    ) -> Result<ProducerRecord, CannotProduce> {
        use streambed::commit_log::{Header, HeaderKey};

        // Being unable to derive a compaction key is a non-recoverable error.
        let key = self.to_compaction_key(&entity_id, event);
        let secret_path = self.secret_path(&entity_id);
        let buf = streambed::encrypt_struct(
            self.secret_store(),
            &secret_path,
            |event| {
                let mut buf = Vec::new();
                ciborium::ser::into_writer(event, &mut buf).map(|_| buf)
            },
            rand::thread_rng,
            &event,
        )
        .await
        .ok_or(CannotProduce::new(
            entity_id,
            format!("Problem encrypting and serializing with secret path: {secret_path}"),
        ))?;

        Ok(ProducerRecord {
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
    async fn encrypted_producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: &E,
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
/// Developers are required to provide implementations of [SourceProvider]
/// for bytes and events i.e. deserialization/decryption and
/// serialization/encryption respectively, along with CommitLog's
/// use of keys for compaction including the storage of entities.
pub struct CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogMarshaller<E>,
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
    M: CommitLogMarshaller<E>,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    pub fn new(commit_log: CL, marshaller: M, consumer_group_name: &str, topic: Topic) -> Self {
        Self {
            commit_log,
            consumer_group_name: consumer_group_name.into(),
            marshaller,
            topic,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<CL, E, M> SourceProvider<E> for CommitLogTopicAdapter<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogMarshaller<E> + Send + Sync,
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

        let marshaller = &self.marshaller;

        if let Ok(mut consumer_records) = consumer_records {
            Ok(Box::pin(stream!({
                while let Some(consumer_record) = consumer_records.next().await {
                    if let Some(record_entity_id) = marshaller.to_entity_id(&consumer_record) {
                        match marshaller.envelope(record_entity_id, consumer_record).await {
                            Ok(envelope) => {
                                yield EntityManagerEventEnvelope::new(
                                    envelope.persistence_id.entity_id,
                                    envelope.seq_nr,
                                    envelope.timestamp,
                                    envelope.event,
                                );
                            }
                            Err(e) => {
                                panic!("When initially consuming: {e:?}. Having to abort as this is unrecoverable.");
                            }
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

        let marshaller = &self.marshaller;

        if let Ok(mut consumer_records) = consumer_records {
            Ok(Box::pin(stream!({
                while let Some(consumer_record) = consumer_records.next().await {
                    if let Some(record_entity_id) = marshaller.to_entity_id(&consumer_record) {
                        if &record_entity_id == entity_id {
                            match marshaller.envelope(record_entity_id, consumer_record).await {
                                Ok(envelope) => {
                                    yield EntityManagerEventEnvelope::new(
                                        envelope.persistence_id.entity_id,
                                        envelope.seq_nr,
                                        envelope.timestamp,
                                        envelope.event,
                                    );
                                }
                                Err(e) => {
                                    panic!("When consuming: {e:?}. Having to abort as this is unrecoverable.");
                                }
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
    M: CommitLogMarshaller<E> + Send + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    async fn process(
        &mut self,
        envelope: EntityManagerEventEnvelope<E>,
    ) -> io::Result<EntityManagerEventEnvelope<E>> {
        let producer_record = self
            .marshaller
            .producer_record(
                self.topic.clone(),
                envelope.entity_id.clone(),
                envelope.seq_nr,
                envelope.timestamp,
                &envelope.event,
            )
            .await
            .map_err(|_| {
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

#[cfg(feature = "cbor")]
pub mod cbor {
    use super::*;

    pub struct Marshaller<E, F, SS> {
        pub entity_type: EntityType,
        pub events_key_secret_path: Arc<str>,
        pub to_record_type: F,
        pub secret_store: SS,
        phantom: PhantomData<E>,
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
    impl<E, F, SS> CommitLogMarshaller<E> for Marshaller<E, F, SS>
    where
        for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
        F: Fn(&E) -> u32 + Sync,
        SS: SecretStore,
    {
        fn entity_type(&self) -> EntityType {
            self.entity_type.clone()
        }

        fn to_compaction_key(&self, entity_id: &EntityId, event: &E) -> Key {
            let record_type = (self.to_record_type)(event);
            // It is an unrecoverable error if the entity id is non-numeric.
            let entity_id = entity_id.parse::<u32>().unwrap();
            (record_type as u64) << EVENT_TYPE_BIT_SHIFT | entity_id as u64
        }

        fn to_entity_id(&self, record: &ConsumerRecord) -> Option<EntityId> {
            let entity_id = (record.key & EVENT_ID_BIT_MASK) as u32;
            let mut buffer = itoa::Buffer::new();
            Some(EntityId::from(buffer.format(entity_id)))
        }

        async fn envelope(
            &self,
            entity_id: EntityId,
            record: ConsumerRecord,
        ) -> Result<EventEnvelope<E>, CannotConsume> {
            self.decrypted_envelope(entity_id, record).await
        }

        async fn producer_record(
            &self,
            topic: Topic,
            entity_id: EntityId,
            seq_nr: u64,
            timestamp: DateTime<Utc>,
            event: &E,
        ) -> Result<ProducerRecord, CannotProduce> {
            self.encrypted_producer_record(topic, entity_id, seq_nr, timestamp, event)
                .await
        }
    }

    #[async_trait]
    impl<E, F, SS> EncryptedCommitLogMarshaller<E> for Marshaller<E, F, SS>
    where
        for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
        F: Fn(&E) -> u32 + Sync,
        SS: SecretStore,
    {
        type SecretStore = SS;

        fn secret_store(&self) -> &Self::SecretStore {
            &self.secret_store
        }

        fn secret_path(&self, _entity_id: &EntityId) -> Arc<str> {
            self.events_key_secret_path.clone()
        }
    }

    /// Provides a marshaller that conveniently uses CBOR for serialization and
    /// a supplied secret store for encryption. Entity identifiers are also
    /// required to be numeric. These characteristics are reasonable when using
    /// the Streambed commit log at the edge.
    pub fn marshaller<E, F, S, SS>(
        entity_type: EntityType,
        events_key_secret_path: S,
        secret_store: SS,
        to_record_type: F,
    ) -> Marshaller<E, F, SS>
    where
        for<'a> E: DeserializeOwned + Serialize + Send + Sync + 'a,
        F: Fn(&E) -> u32 + Sync,
        SS: SecretStore,
        S: ToString,
    {
        Marshaller {
            entity_type,
            events_key_secret_path: Arc::from(events_key_secret_path.to_string()),
            to_record_type,
            secret_store,
            phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, time::Duration};

    use super::*;
    use akka_persistence_rs::{entity::EventSourcedBehavior, entity_manager};
    use serde::Deserialize;
    use streambed::commit_log::{Header, HeaderKey};
    use streambed_logged::FileLog;
    use test_log::test;
    use tokio::time;

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

    // Developers are expected to provide a marshaller of events.
    // The marshaller is responsible for more than just the serialization
    // of an envelope. Extracting/saving an entity id and determining other
    // metadata is also important. We would also expect to see any encryption
    // and decyption being performed by the marshaller.
    // The example here overrides the default methods of the marshaller and
    // effectively ignores the use of a secret key; just to prove that you really
    // can lay out an envelope any way that you would like to. Note that secret keys
    // are important though.

    struct MyEventMarshaller;

    #[async_trait]
    impl CommitLogMarshaller<MyEvent> for MyEventMarshaller {
        fn entity_type(&self) -> EntityType {
            EntityType::from("some-entity-type")
        }

        fn to_compaction_key(&self, _entity_id: &EntityId, _event: &MyEvent) -> Key {
            panic!("should not be called")
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
        ) -> Result<EventEnvelope<MyEvent>, CannotConsume> {
            let value = String::from_utf8(record.value)
                .ok()
                .ok_or(CannotConsume::new(entity_id.clone(), "bad entity id"))?;
            let event = MyEvent { value };
            let envelope = record.timestamp.map(|timestamp| EventEnvelope {
                persistence_id: PersistenceId::new(self.entity_type(), entity_id.clone()),
                seq_nr: 1,
                timestamp,
                event,
                offset: 0,
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
            event: &MyEvent,
        ) -> Result<ProducerRecord, CannotProduce> {
            let headers = vec![Header {
                key: HeaderKey::from("entity-id"),
                value: entity_id.as_bytes().into(),
            }];
            Ok(ProducerRecord {
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

        let marshaller = MyEventMarshaller;
        let mut adapter = CommitLogTopicAdapter::new(
            commit_log.clone(),
            marshaller,
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

        let marshaller = MyEventMarshaller;

        let file_log_topic_adapter = CommitLogTopicAdapter::new(
            commit_log,
            marshaller,
            "some-consumer",
            Topic::from("some-topic"),
        );

        let (entity_manager, _) = entity_manager::task(MyBehavior, file_log_topic_adapter, 10, 1);
        assert!(entity_manager.await.is_ok());
    }
}
