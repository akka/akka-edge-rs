#![doc = include_str!("../README.md")]

use std::{marker::PhantomData, ops::Range, pin::Pin};

use akka_persistence_rs::EntityType;
use akka_persistence_rs_commitlog::{CommitLogEventEnvelopeMarshaler, EventEnvelope};
use akka_projection_rs::{Offset, SourceProvider};
use serde::{de::DeserializeOwned, Serialize};
use streambed::commit_log::{CommitLog, Topic, TopicRef};
use tokio_stream::Stream;

/// Source events for a projection from a Streambed commit log.
pub struct CommitLogSourceProvider<CL, E, M> {
    _commit_log: CL,
    _consumer_group_name: String,
    _marshaler: M,
    _slice_range: Range<u32>,
    _topic: Topic,
    phantom: PhantomData<E>,
}

impl<CL, E, M> CommitLogSourceProvider<CL, E, M> {
    pub fn new(
        commit_log: CL,
        marshaler: M,
        consumer_group_name: &str,
        topic: TopicRef,
        slice_range: Range<u32>,
    ) -> Self {
        Self {
            _commit_log: commit_log,
            _consumer_group_name: consumer_group_name.into(),
            _marshaler: marshaler,
            _slice_range: slice_range,
            _topic: topic.into(),
            phantom: PhantomData,
        }
    }
}

impl<CL, E, M> SourceProvider for CommitLogSourceProvider<CL, E, M>
where
    CL: CommitLog,
    M: CommitLogEventEnvelopeMarshaler<E>,
    for<'async_trait> E: DeserializeOwned + Serialize + Send + Sync + 'async_trait,
{
    type Envelope = EventEnvelope<E>;

    type Offset = Offset;

    fn events_by_slices<Event>(
        _entity_type: EntityType,
        _min_slice: u32,
        _max_slice: u32,
        _offset: Offset,
    ) -> Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send>> {
        todo!()
    }
}
