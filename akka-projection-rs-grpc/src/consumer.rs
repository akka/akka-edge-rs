use std::{marker::PhantomData, pin::Pin};

use akka_persistence_rs::{EntityType, Offset};
use akka_projection_rs::SourceProvider;
use async_trait::async_trait;
use tokio_stream::Stream;

use crate::EventEnvelope;

pub struct GrpcSourceProvider<E> {
    phantom: PhantomData<E>,
}

#[async_trait]
impl<E> SourceProvider for GrpcSourceProvider<E>
where
    E: Sync,
{
    type Envelope = EventEnvelope<E>;

    async fn events_by_slices(
        &self,
        _entity_type: EntityType,
        _min_slice: u32,
        _max_slice: u32,
        _offset: Option<Offset>,
    ) -> Pin<Box<dyn Stream<Item = EventEnvelope<E>> + Send + 'async_trait>> {
        todo!()
    }
}
