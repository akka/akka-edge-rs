#![doc = include_str!("../README.md")]

use std::{marker::PhantomData, pin::Pin};

use akka_persistence_rs::EntityType;
use akka_persistence_rs_commitlog::{CommitLogEventEnvelopeMarshaler, EventEnvelope};
use akka_projection_rs::{Offset, SourceProvider};
use async_stream::stream;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use streambed::commit_log::{CommitLog, ConsumerOffset, Subscription, Topic};
use tokio_stream::{Stream, StreamExt};

/// Source events for a projection from a Streambed commit log.
pub struct CommitLogSourceProvider<CL, E, M> {
    commit_log: CL,
    consumer_group_name: String,
    marshaler: M,
    topic: Topic,
    phantom: PhantomData<E>,
}

impl<CL, E, M> CommitLogSourceProvider<CL, E, M> {
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
impl<CL, E, M> SourceProvider for CommitLogSourceProvider<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E> + Sync,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type Envelope = EventEnvelope<E>;

    async fn events_by_slices(
        &self,
        // The entity type means nothing in this context as we have a topic.
        _entity_type: EntityType,
        // Slices are not used with the commit log
        _min_slice: u32,
        _max_slice: u32,
        offset: Option<Offset>,
    ) -> Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send + 'async_trait>> {
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
                    if let Some(envelope) =
                        marshaler.envelope(record_entity_id, consumer_record).await
                    {
                        yield envelope;
                    }
                }
            }
        }))
    }
}
