use std::{future::Future, marker::PhantomData, pin::Pin};

use akka_persistence_rs::Offset;
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

    async fn source<F, FR>(
        &self,
        _offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: FnOnce() -> FR + Send,
        FR: Future<Output = Option<Offset>> + Send,
    {
        todo!()
    }
}
