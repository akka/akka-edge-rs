use akka_persistence_rs::EntityId;
use akka_persistence_rs::EntityType;
use akka_persistence_rs::PersistenceId;
use akka_persistence_rs::Tag;
use akka_persistence_rs::TimestampOffset;
use akka_persistence_rs::WithPersistenceId;
use akka_persistence_rs::WithSeqNr;
use akka_persistence_rs::WithTags;
use akka_persistence_rs::WithTimestampOffset;
use akka_projection_rs::consumer_filter::Filter;
use akka_projection_rs::consumer_filter::FilterCriteria;
use akka_projection_rs::HandlerError;
use akka_projection_rs::PendingHandler;
use async_stream::stream;
use async_trait::async_trait;
use futures::future;
use log::debug;
use log::warn;
use prost::Name;
use prost_types::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;

use crate::delayer::Delayer;
use crate::proto;
use crate::to_filter_criteria;
use crate::EventEnvelope;
use crate::StreamId;

/// The result of a transformation function for the purposes of
/// passing data on to a gRPC producer.
pub struct Transformation<E> {
    pub entity_id: EntityId,
    pub seq_nr: u64,
    pub offset: TimestampOffset,
    pub event: Option<E>,
}

/// Processes events transformed from some unknown event envelope (EI)
/// to then pass on to a gRPC event producer.
pub struct GrpcEventProcessor<E, EI, F>
where
    F: Fn(&EI) -> Option<E>,
{
    flow: GrpcEventFlow<E>,
    consumer_filters_receiver: watch::Receiver<Vec<FilterCriteria>>,
    filter: Filter,
    transformer: F,
    phantom: PhantomData<EI>,
}

#[async_trait]
impl<EI, E, F> PendingHandler for GrpcEventProcessor<E, EI, F>
where
    EI: WithPersistenceId + WithSeqNr + WithTags + WithTimestampOffset + Send,
    E: Send,
    F: Fn(&EI) -> Option<E> + Send,
{
    type Envelope = EI;

    const MAX_PENDING: usize = 10;

    async fn process_pending(
        &mut self,
        envelope: Self::Envelope,
    ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError> {
        if self.consumer_filters_receiver.has_changed().unwrap_or(true) {
            self.filter
                .update(self.consumer_filters_receiver.borrow().clone());
        };

        let event = if self.filter.matches(&envelope) {
            (self.transformer)(&envelope)
        } else {
            // Gaps are ok given a filter situation.
            return Ok(Box::pin(future::ready(Ok(()))));
        };

        let transformation = Transformation {
            entity_id: envelope.persistence_id().entity_id,
            seq_nr: envelope.seq_nr(),
            offset: envelope.timestamp_offset(),
            event,
        };
        let result = self.flow.process(transformation).await;
        if let Ok(receiver_reply) = result {
            Ok(Box::pin(async {
                receiver_reply.await.map_err(|_| HandlerError)
            }))
        } else {
            Err(HandlerError)
        }
    }
}

/// Transform and forward gRPC events given a user-supplied transformation function.
pub struct GrpcEventFlow<E> {
    entity_type: EntityType,
    grpc_producer: mpsc::Sender<(EventEnvelope<E>, oneshot::Sender<()>)>,
}

impl<E> GrpcEventFlow<E> {
    pub fn new(
        entity_type: EntityType,
        grpc_producer: mpsc::Sender<(EventEnvelope<E>, oneshot::Sender<()>)>,
    ) -> Self {
        Self {
            entity_type,
            grpc_producer,
        }
    }

    /// Produces a handler for this flow. The handler will receive events,
    /// apply filters and, if the filters match, transform and forward events
    /// on to a gRPC producer.
    pub fn handler<EI, F>(
        self,
        consumer_filters_receiver: watch::Receiver<Vec<FilterCriteria>>,
        topic_tag_prefix: Tag,
        transformer: F,
    ) -> GrpcEventProcessor<E, EI, F>
    where
        F: Fn(&EI) -> Option<E>,
    {
        GrpcEventProcessor {
            flow: self,
            consumer_filters_receiver,
            filter: Filter::new(topic_tag_prefix),
            transformer,
            phantom: PhantomData,
        }
    }

    async fn process(
        &mut self,
        transformation: Transformation<E>,
    ) -> Result<oneshot::Receiver<()>, HandlerError> {
        let (reply, reply_receiver) = oneshot::channel();
        let persistence_id = PersistenceId::new(self.entity_type.clone(), transformation.entity_id);
        self.grpc_producer
            .send((
                EventEnvelope {
                    persistence_id: persistence_id.clone(),
                    seq_nr: transformation.seq_nr,
                    event: transformation.event,
                    offset: transformation.offset,
                },
                reply,
            ))
            .await
            .map_err(|_| HandlerError)?;

        Ok(reply_receiver)
    }
}

type Context<E> = (EventEnvelope<E>, oneshot::Sender<()>);

/// Reliably stream event envelopes to a consumer. Event envelope transmission
/// requests are sent over a channel and have a reply that is completed on the
/// remote consumer's acknowledgement of receipt.
pub async fn run<E, EC, ECR>(
    event_consumer_channel: EC,
    origin_id: StreamId,
    stream_id: StreamId,
    consumer_filters: watch::Sender<Vec<FilterCriteria>>,
    entity_type: EntityType,
    mut envelopes: mpsc::Receiver<(EventEnvelope<E>, oneshot::Sender<()>)>,
    mut kill_switch: oneshot::Receiver<()>,
) where
    E: Clone + Name + 'static,
    EC: Fn() -> ECR + Send + Sync,
    ECR: Future<Output = Result<Channel, tonic::transport::Error>> + Send,
{
    let mut delayer: Option<Delayer> = None;

    let mut in_flight: HashMap<PersistenceId, VecDeque<Context<E>>> = HashMap::new();

    'outer: loop {
        if let Err(oneshot::error::TryRecvError::Closed) = kill_switch.try_recv() {
            break;
        }

        let mut connection = if let Ok(connection) = (event_consumer_channel)()
            .await
            .map(proto::event_consumer_service_client::EventConsumerServiceClient::new)
        {
            delayer = None;
            Some(connection)
        } else {
            if let Some(d) = &mut delayer {
                d.delay().await;
            } else {
                let mut d = Delayer::default();
                d.delay().await;
                delayer = Some(d);
            }
            None
        };

        if let Some(connection) = &mut connection {
            let origin_id = origin_id.to_string();
            let stream_id = stream_id.to_string();

            let (event_in, mut event_in_receiver) = mpsc::unbounded_channel::<EventEnvelope<E>>();

            let request = Request::new(stream! {
                yield proto::ConsumeEventIn {
                    message: Some(proto::consume_event_in::Message::Init(
                        proto::ConsumerEventInit {
                            origin_id,
                            stream_id,
                        },
                    )),
                };

                let ordinary_events_source = smol_str::SmolStr::from("");

                while let Some(envelope) = event_in_receiver.recv().await {
                    let timestamp = prost_types::Timestamp {
                        seconds: envelope.offset.timestamp.timestamp(),
                        nanos: envelope.offset.timestamp.timestamp_nanos_opt().unwrap_or_default() as i32
                    };

                    if let Some(event) = envelope.event {
                        if let Ok(any) = Any::from_msg(&event) {
                            yield proto::ConsumeEventIn {
                                message: Some(proto::consume_event_in::Message::Event(
                                    proto::Event {
                                        persistence_id: envelope.persistence_id.to_string(),
                                        seq_nr: envelope.seq_nr as i64,
                                        slice: envelope.persistence_id.slice() as i32,
                                        offset: Some(proto::Offset { timestamp: Some(timestamp), seen: vec![] }),
                                        payload: Some(any),
                                        source: ordinary_events_source.to_string(),
                                        metadata: None,
                                        tags: vec![]
                                    },
                                )),
                            };
                        }
                    } else {
                        yield proto::ConsumeEventIn {
                            message: Some(proto::consume_event_in::Message::FilteredEvent(
                                proto::FilteredEvent {
                                    persistence_id: envelope.persistence_id.to_string(),
                                    seq_nr: envelope.seq_nr as i64,
                                    slice: envelope.persistence_id.slice() as i32,
                                    offset: Some(proto::Offset { timestamp: Some(timestamp), seen: vec![] }),
                                    source: ordinary_events_source.to_string(),
                                },
                            )),
                        };
                    }
                }
            });
            let result = connection.consume_event(request).await;

            if let Ok(response) = result {
                let mut stream_outs = response.into_inner();

                loop {
                    tokio::select! {
                        Some(Ok(proto::ConsumeEventOut {
                            message: Some(message),
                        })) = stream_outs.next() => match message {
                            proto::consume_event_out::Message::Start(proto::ConsumerEventStart { filter }) => {
                                debug!("Starting the protocol");
                                let _ = consumer_filters.send(
                                    filter
                                        .into_iter()
                                        .flat_map(|f| to_filter_criteria(entity_type.clone(), f))
                                        .collect(),
                                );
                                break;
                            }
                            _ => {
                                warn!("Received a message before starting the protocol - ignoring event");
                            }
                        },

                        _ = &mut kill_switch => break 'outer,
                    }
                }

                for (_, contexts) in in_flight.iter() {
                    for (envelope, _) in contexts {
                        if event_in.send(envelope.clone()).is_err() {
                            continue 'outer;
                        }
                    }
                }

                loop {
                    tokio::select! {
                        request = envelopes.recv() => {
                            if let Some((envelope, reply_to)) = request {
                                let contexts = in_flight
                                    .entry(envelope.persistence_id.clone())
                                    .or_default();
                                contexts.push_back((envelope.clone(), reply_to));

                                if event_in.send(envelope).is_err()  {
                                    continue 'outer;
                                }

                            } else {
                                continue 'outer;
                            }
                        }

                        Some(Ok(proto::ConsumeEventOut {
                            message: Some(message),
                        })) = stream_outs.next() => match message {
                            proto::consume_event_out::Message::Start(proto::ConsumerEventStart { .. }) => {
                                warn!("Received a protocol start when already started - ignoring");
                            }
                            proto::consume_event_out::Message::Ack(proto::ConsumerEventAck { persistence_id, seq_nr }) => {
                                if let Ok(persistence_id) = persistence_id.parse() {
                                    if let Some(contexts) = in_flight.get_mut(&persistence_id) {
                                        let seq_nr = seq_nr as u64;
                                        while let Some((envelope, reply_to)) = contexts.pop_front() {
                                            if seq_nr == envelope.seq_nr && reply_to.send(()).is_ok() {
                                                break;
                                            }
                                        }
                                        if contexts.is_empty() {
                                            in_flight.remove(&persistence_id);
                                        }
                                    }
                                } else {
                                    warn!("Received an event but could not parse the persistence id - ignoring event");
                                }
                            }
                        },

                        _ = &mut kill_switch => break 'outer,

                        else => break
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::OriginId;
    use chrono::Utc;
    use std::{net::ToSocketAddrs, pin::Pin, sync::Arc};
    use test_log::test;
    use tokio::sync::Notify;
    use tokio_stream::Stream;
    use tonic::{transport::Server, Status, Streaming};

    struct TestEventConsumerService {}

    #[async_trait]
    impl proto::event_consumer_service_server::EventConsumerService for TestEventConsumerService {
        type ConsumeEventStream =
            Pin<Box<dyn Stream<Item = Result<proto::ConsumeEventOut, Status>> + Send>>;

        async fn consume_event(
            &self,
            request: Request<Streaming<proto::ConsumeEventIn>>,
        ) -> std::result::Result<tonic::Response<Self::ConsumeEventStream>, tonic::Status> {
            let mut consume_events_in = request.into_inner();
            if let Some(Ok(proto::ConsumeEventIn {
                message:
                    Some(proto::consume_event_in::Message::Init(proto::ConsumerEventInit {
                        origin_id,
                        stream_id,
                    })),
            })) = consume_events_in.next().await
            {
                if origin_id == "some-origin-id" && stream_id == "some-stream-id" {
                    let consume_events_out = Box::pin(stream! {
                        yield Ok(proto::ConsumeEventOut {
                            message: Some(proto::consume_event_out::Message::Start(
                                proto::ConsumerEventStart {
                                    filter: vec![proto::FilterCriteria {
                                        message: Some(proto::filter_criteria::Message::ExcludeEntityIds(proto::ExcludeEntityIds { entity_ids: vec![] })),
                                    }]
                                },
                            )),
                        });

                        if let Some(Ok(proto::ConsumeEventIn {
                            message:
                                Some(proto::consume_event_in::Message::Event(proto::Event {
                                    persistence_id,
                                    seq_nr,
                                    ..
                                })),
                        })) = consume_events_in.next().await
                        {
                            yield Ok(proto::ConsumeEventOut {
                                message: Some(proto::consume_event_out::Message::Ack(
                                    proto::ConsumerEventAck {
                                        persistence_id,
                                        seq_nr
                                    },
                                )),
                            })
                        }
                    });
                    Ok(tonic::Response::new(consume_events_out))
                } else {
                    Err(tonic::Status::failed_precondition(
                        "Expecting a certain origin and stream id",
                    ))
                }
            } else {
                Err(tonic::Status::failed_precondition("Expecting init"))
            }
        }
    }

    #[test(tokio::test)]
    async fn can_run() {
        let server_kill_switch = Arc::new(Notify::new());

        let task_kill_switch = server_kill_switch.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    proto::event_consumer_service_server::EventConsumerServiceServer::new(
                        TestEventConsumerService {},
                    ),
                )
                .serve_with_shutdown(
                    "127.0.0.1:50052".to_socket_addrs().unwrap().next().unwrap(),
                    task_kill_switch.notified(),
                )
                .await
                .unwrap();
        });

        let (consumer_filters, mut consumer_filters_receiver) = watch::channel(vec![]);
        let (sender, receiver) = mpsc::channel(10);
        let (_task_kill_switch, task_kill_switch_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let channel = Channel::from_static("http://127.0.0.1:50052");
            let _ = run(
                || channel.connect(),
                OriginId::from("some-origin-id"),
                StreamId::from("some-stream-id"),
                consumer_filters,
                EntityType::from("some-entity-type"),
                receiver,
                task_kill_switch_receiver,
            )
            .await;
        });

        let (reply, reply_receiver) = oneshot::channel();
        assert!(sender
            .send((
                EventEnvelope {
                    persistence_id: "entity-type|entity-id".parse().unwrap(),
                    seq_nr: 1,
                    event: Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 0
                    }),
                    offset: TimestampOffset {
                        timestamp: Utc::now(),
                        seen: vec![]
                    }
                },
                reply,
            ))
            .await
            .is_ok());

        assert!(consumer_filters_receiver.changed().await.is_ok());
        assert!(!consumer_filters_receiver.borrow().is_empty());

        assert!(reply_receiver.await.is_ok());

        server_kill_switch.notified();
    }
}
