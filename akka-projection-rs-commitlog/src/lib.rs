#![doc = include_str!("../README.md")]

use std::{future::Future, marker::PhantomData, ops::Range, pin::Pin};

use akka_persistence_rs::{EntityType, Offset, PersistenceId};
use akka_persistence_rs_commitlog::{CommitLogEventEnvelopeMarshaler, EventEnvelope};
use akka_projection_rs::SourceProvider;
use async_stream::stream;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use streambed::commit_log::{CommitLog, ConsumerOffset, Subscription, Topic};
use tokio_stream::{Stream, StreamExt};

/// Source events for given slices from a Streambed commit log.
pub struct CommitLogSourceProvider<CL, E, M> {
    commit_log: CL,
    consumer_group_name: String,
    entity_type: EntityType,
    marshaler: M,
    slice_range: Range<u32>,
    topic: Topic,
    phantom: PhantomData<E>,
}

impl<CL, E, M> CommitLogSourceProvider<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E> + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    pub fn new(
        commit_log: CL,
        marshaler: M,
        slice_range: Range<u32>,
        consumer_group_name: &str,
        topic: Topic,
        entity_type: EntityType,
    ) -> Self {
        Self {
            commit_log,
            consumer_group_name: consumer_group_name.into(),
            marshaler,
            slice_range,
            topic,
            entity_type,
            phantom: PhantomData,
        }
    }

    async fn events_by_slices(
        &self,
        offset: Option<Offset>,
    ) -> Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send + '_>> {
        let offsets = if let Some(Offset::Sequence(offset)) = offset {
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

        let marshaler = &self.marshaler;

        Box::pin(stream!({
            while let Some(consumer_record) = records.next().await {
                if let Some(record_entity_id) = M::to_entity_id(&consumer_record) {
                    let persistence_id =
                        PersistenceId::new(self.entity_type.clone(), record_entity_id);
                    if self.slice_range.contains(&persistence_id.slice()) {
                        if let Some(envelope) = marshaler
                            .envelope(persistence_id.entity_id, consumer_record)
                            .await
                        {
                            yield envelope;
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
    M: CommitLogEventEnvelopeMarshaler<E> + Send + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type Envelope = EventEnvelope<E>;

    async fn source<F, FR>(
        &self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: FnOnce() -> FR + Send,
        FR: Future<Output = Option<Offset>> + Send,
    {
        let offset = offset().await;
        self.events_by_slices(offset).await
    }
}
