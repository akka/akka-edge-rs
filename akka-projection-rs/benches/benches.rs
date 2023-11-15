use std::{future::Future, pin::Pin, sync::Arc};

use akka_persistence_rs::{
    EntityId, EntityType, Offset, PersistenceId, Source, WithOffset, WithPersistenceId, WithSeqNr,
    WithSource,
};
use akka_projection_rs::{
    consumer, offset_store::LastOffset, Handler, HandlerError, SourceProvider,
};
use async_stream::stream;
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::sync::{mpsc, Notify};
use tokio_stream::Stream;

const NUM_EVENTS: usize = 10_000;

struct TestEnvelope {
    persistence_id: PersistenceId,
    offset: u64,
}

impl WithPersistenceId for TestEnvelope {
    fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }
}

impl WithOffset for TestEnvelope {
    fn offset(&self) -> Offset {
        Offset::Sequence(self.offset)
    }
}

impl WithSource for TestEnvelope {
    fn source(&self) -> akka_persistence_rs::Source {
        Source::Regular
    }
}

impl WithSeqNr for TestEnvelope {
    fn seq_nr(&self) -> u64 {
        0
    }
}

struct TestSourceProvider;

#[async_trait]
impl SourceProvider for TestSourceProvider {
    type Envelope = TestEnvelope;

    async fn source<F, FR>(
        &self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<LastOffset>> + Send,
    {
        let persistence_id =
            PersistenceId::new(EntityType::from("entity-type"), EntityId::from("entity-id"));
        let _ = offset().await;
        Box::pin(stream!(loop {
            for offset in 0..NUM_EVENTS as u64 {
                yield TestEnvelope {
                    persistence_id: persistence_id.clone(),
                    offset,
                };
            }
        }))
    }

    async fn load_envelope(
        &self,
        _persistence_id: PersistenceId,
        _sequence_nr: u64,
    ) -> Option<Self::Envelope> {
        None
    }
}

struct TestHandler {
    events_processed: Arc<Notify>,
}

#[async_trait]
impl Handler for TestHandler {
    type Envelope = TestEnvelope;

    async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        const LAST_OFFSET: u64 = NUM_EVENTS as u64 - 1;
        if envelope.offset == LAST_OFFSET {
            self.events_processed.notify_one();
            return Err(HandlerError);
        }
        Ok(())
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("project events", move |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let events_processed = Arc::new(Notify::new());
        let (offset_store, mut offset_store_receiver) = mpsc::channel(1);
        let offset_store_task =
            async move { while let Some(_) = offset_store_receiver.recv().await {} };
        let (projection_task, _kill_switch) = consumer::task(
            offset_store,
            TestSourceProvider,
            TestHandler {
                events_processed: events_processed.clone(),
            },
        );

        let _ = rt.spawn(async move { tokio::join!(offset_store_task, projection_task) });

        b.to_async(&rt).iter(|| {
            let task_events_processed = events_processed.clone();
            async move {
                tokio::spawn(async move {
                    task_events_processed.notified().await;
                })
                .await
            }
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
