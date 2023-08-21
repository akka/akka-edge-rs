use std::{marker::PhantomData, pin::Pin};

use akka_persistence_rs::{entity_manager::EventEnvelope, EntityType};
use akka_projection_rs::{Offset, SourceProvider};
use tokio_stream::Stream;

pub struct GrpcSourceProvider<E> {
    phantom: PhantomData<E>,
}

impl<E> SourceProvider for GrpcSourceProvider<E> {
    /// The envelope processed by the provider.
    type Envelope = EventEnvelope<E>;

    /// The type that describes offsets into a journal
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
