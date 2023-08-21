use std::{io, num::NonZeroUsize, pin::Pin, sync::Arc};

use akka_persistence_rs::{
    effect::{emit_event, Effect, EffectExt},
    entity::{Context, EventSourcedBehavior},
    entity_manager::{self, EventEnvelope, Handler, SourceProvider},
    EntityId, Message,
};
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::sync::{mpsc, Notify};
use tokio_stream::Stream;

const NUM_EVENTS: usize = 10_000;

#[derive(Default)]
struct State;

struct Command;

struct Event;

struct Behavior;

impl EventSourcedBehavior for Behavior {
    type State = State;
    type Command = Command;
    type Event = Event;

    fn for_command(
        _context: &Context,
        _state: &Self::State,
        _command: Self::Command,
    ) -> Box<dyn Effect<Self>> {
        emit_event(Event).boxed()
    }

    fn on_event(_context: &Context, _state: &mut Self::State, _event: Self::Event) {}
}

struct Adapter {
    event_count: usize,
    events_processed: Arc<Notify>,
}

#[async_trait]
impl SourceProvider<Event> for Adapter {
    async fn source_initial(
        &mut self,
    ) -> io::Result<Pin<Box<dyn Stream<Item = EventEnvelope<Event>> + Send + 'async_trait>>> {
        Ok(Box::pin(tokio_stream::empty()))
    }

    async fn source(
        &mut self,
        _entity_id: &EntityId,
    ) -> io::Result<Pin<Box<dyn Stream<Item = EventEnvelope<Event>> + Send + 'async_trait>>> {
        Ok(Box::pin(tokio_stream::empty()))
    }
}

#[async_trait]
impl Handler<Event> for Adapter {
    async fn process(
        &mut self,
        envelope: EventEnvelope<Event>,
    ) -> io::Result<EventEnvelope<Event>> {
        self.event_count += 1;
        if self.event_count == NUM_EVENTS {
            self.events_processed.notify_one();
            self.event_count = 0;
        }
        Ok(envelope)
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("produce events", move |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let events_processed = Arc::new(Notify::new());
        let (sender, receiver) = mpsc::channel(10);
        let _ = rt.spawn(entity_manager::run(
            Behavior,
            Adapter {
                event_count: 0,
                events_processed: events_processed.clone(),
            },
            receiver,
            NonZeroUsize::new(1).unwrap(),
        ));

        b.to_async(&rt).iter(|| {
            let task_events_processed = events_processed.clone();
            let task_sender = sender.clone();
            async move {
                tokio::spawn(async move {
                    for _ in 0..NUM_EVENTS {
                        let _ = task_sender
                            .send(Message::new("id-1".to_string(), Command))
                            .await;
                    }
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
