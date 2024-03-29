#![doc = include_str!("../README.md")]

pub mod offset_store;

use std::{future::Future, marker::PhantomData, ops::Range, pin::Pin};

use akka_persistence_rs::{Offset, PersistenceId};
use akka_persistence_rs_commitlog::{CommitLogMarshaller, EventEnvelope};
use akka_projection_rs::{offset_store::LastOffset, SourceProvider};
use async_stream::stream;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use streambed::commit_log::{CommitLog, ConsumerOffset, Subscription, Topic};
use tokio_stream::{Stream, StreamExt};

/// Source events for given slices from a Streambed commit log.
pub struct CommitLogSourceProvider<CL, E, M> {
    commit_log: CL,
    consumer_group_name: String,
    marshaller: M,
    slice_range: Range<u32>,
    topic: Topic,
    phantom: PhantomData<E>,
}

impl<CL, E, M> CommitLogSourceProvider<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogMarshaller<E> + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    pub fn new(commit_log: CL, marshaller: M, consumer_group_name: &str, topic: Topic) -> Self {
        // When it comes to having a projection sourced from a local
        // commit log, there's little benefit if having many of them.
        // We therefore manage all slices from just one projection.
        let slice_range = akka_persistence_rs::slice_ranges(1);

        Self::with_slice_range(
            commit_log,
            marshaller,
            consumer_group_name,
            topic,
            slice_range.first().cloned().unwrap(),
        )
    }

    pub fn with_slice_range(
        commit_log: CL,
        marshaller: M,
        consumer_group_name: &str,
        topic: Topic,
        slice_range: Range<u32>,
    ) -> Self {
        Self {
            commit_log,
            consumer_group_name: consumer_group_name.into(),
            marshaller,
            slice_range,
            topic,
            phantom: PhantomData,
        }
    }

    async fn events_by_slices(
        &self,
        offset: Option<LastOffset>,
    ) -> Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send + '_>> {
        let offsets = if let Some((_, Offset::Sequence(offset))) = offset {
            vec![ConsumerOffset {
                partition: 0,
                offset,
                topic: self.topic.clone(),
            }]
        } else {
            vec![]
        };

        let subscriptions = vec![Subscription {
            topic: self.topic.clone(),
        }];

        let mut records = self.commit_log.scoped_subscribe(
            self.consumer_group_name.as_str(),
            offsets,
            subscriptions,
            None,
        );

        let marshaller = &self.marshaller;

        Box::pin(stream!({
            while let Some(consumer_record) = records.next().await {
                if let Some(record_entity_id) = marshaller.to_entity_id(&consumer_record) {
                    let persistence_id =
                        PersistenceId::new(marshaller.entity_type(), record_entity_id);
                    if self.slice_range.contains(&persistence_id.slice()) {
                        match marshaller
                            .envelope(persistence_id.entity_id, consumer_record)
                            .await
                        {
                            Ok(envelope) => yield envelope,
                            Err(e) => log::info!("Problem consuming record: {e:?}"),
                        }
                    }
                }
            }
        }))
    }
}

#[async_trait]
impl<CL, E, M> SourceProvider for CommitLogSourceProvider<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogMarshaller<E> + Send + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type Envelope = EventEnvelope<E>;

    async fn source<F, FR>(
        &self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<LastOffset>> + Send,
    {
        let offset = offset().await;
        self.events_by_slices(offset).await
    }

    async fn load_envelope(
        &self,
        _persistence_id: PersistenceId,
        _sequence_nr: u64,
    ) -> Option<Self::Envelope> {
        None // Backtracking for commit logs etc isn't supported so we don't need to query for a specific event
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use super::*;
    use akka_persistence_rs::{EntityId, EntityType};
    use akka_persistence_rs_commitlog::{CannotConsume, CannotProduce};
    use chrono::{DateTime, Utc};
    use serde::Deserialize;
    use streambed::commit_log::{ConsumerRecord, Header, Key, ProducerRecord};
    use streambed_logged::FileLog;
    use test_log::test;
    use tokio_stream::StreamExt;

    // Scaffolding

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    struct MyEvent {
        value: String,
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
            EntityType::from("some-topic")
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
                .map_err(|_| CannotConsume::new(entity_id.clone(), "Non numeric entity id"))?;
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
                key: Topic::from("entity-id"),
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
    async fn can_source() {
        // Set up the file log and adapter

        let logged_dir = env::temp_dir().join("can_source");
        let _ = fs::remove_dir_all(&logged_dir);
        let _ = fs::create_dir_all(&logged_dir);
        println!("Writing to {}", logged_dir.to_string_lossy());

        let commit_log = FileLog::new(logged_dir);

        // Scaffolding

        let entity_type = EntityType::from("some-topic");
        let entity_id = EntityId::from("some-entity");
        let persistence_id = PersistenceId::new(entity_type.clone(), entity_id.clone());
        let topic = Topic::from("some-topic");
        let event_value = "some value".to_string();

        let headers = vec![Header {
            key: Topic::from("entity-id"),
            value: entity_id.as_bytes().into(),
        }];
        let record = ProducerRecord {
            topic: topic.clone(),
            headers,
            timestamp: Some(Utc::now()),
            key: 0,
            value: event_value.clone().into_bytes(),
            partition: 0,
        };
        assert!(commit_log.produce(record).await.is_ok());

        // Source that event just produced.

        let source_provider = CommitLogSourceProvider::new(
            commit_log.clone(),
            MyEventMarshaller,
            "some-consumer",
            topic,
        );

        let mut envelopes = source_provider.source(|| async { None }).await;
        let envelope = envelopes.next().await.unwrap();

        assert_eq!(envelope.persistence_id, persistence_id);
        assert_eq!(envelope.event, MyEvent { value: event_value },);
    }
}
