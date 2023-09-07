use akka_persistence_rs::EntityId;
use akka_persistence_rs::EntityType;
use akka_persistence_rs::PersistenceId;
use akka_persistence_rs::TimestampOffset;
use akka_persistence_rs::WithEntityId;
use akka_persistence_rs::WithSeqNr;
use akka_persistence_rs::WithTimestampOffset;
use akka_projection_rs::HandlerError;
use akka_projection_rs::PendingHandler;
use async_stream::stream;
use async_trait::async_trait;
use futures::future;
use prost::Name;
use prost_types::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tonic::{transport::Uri, Request};

use crate::delayer::Delayer;
use crate::proto;
use crate::EventEnvelope;
use crate::StreamId;

/// The result of a transformation function for the purposes of
/// passing data on to a gRPC producer.
pub struct Transformation<E> {
    pub entity_id: EntityId,
    pub seq_nr: u64,
    pub offset: TimestampOffset,
    pub event: E,
}

/// Processes events transformed from some unknown event envelope (EI)
/// to then pass on to a gRPC event producer.
pub struct GrpcEventProcessor<E, EI, F>
where
    F: Fn(&EI) -> Option<E>,
{
    producer: GrpcEventProducer<E>,
    transformer: F,
    phantom: PhantomData<EI>,
}

#[async_trait]
impl<EI, E, F> PendingHandler for GrpcEventProcessor<E, EI, F>
where
    EI: WithEntityId + WithSeqNr + WithTimestampOffset + Send,
    E: Send,
    F: Fn(&EI) -> Option<E> + Send,
{
    type Envelope = EI;

    const MAX_PENDING: usize = 10;

    async fn process_pending(
        &mut self,
        envelope: Self::Envelope,
    ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError> {
        let Some(event) = (self.transformer)(&envelope) else {
            return Ok(Box::pin(future::ready(Ok(()))));
        };
        let transformation = Transformation {
            entity_id: envelope.entity_id(),
            seq_nr: envelope.seq_nr(),
            offset: envelope.timestamp_offset(),
            event,
        };
        let result = self.producer.process(transformation).await;
        if let Ok(receiver_reply) = result {
            Ok(Box::pin(async {
                receiver_reply.await.map_err(|_| HandlerError)
            }))
        } else {
            Err(HandlerError)
        }
    }
}

/// Produce gRPC events given a user-supplied transformation function.
pub struct GrpcEventProducer<E> {
    entity_type: EntityType,
    grpc_producer: mpsc::Sender<(EventEnvelope<E>, oneshot::Sender<()>)>,
}

impl<E> GrpcEventProducer<E> {
    pub fn new(
        entity_type: EntityType,
        grpc_producer: mpsc::Sender<(EventEnvelope<E>, oneshot::Sender<()>)>,
    ) -> Self {
        Self {
            entity_type,
            grpc_producer,
        }
    }

    pub fn handler<EI, F>(self, transformer: F) -> GrpcEventProcessor<E, EI, F>
    where
        F: Fn(&EI) -> Option<E>,
    {
        GrpcEventProcessor {
            producer: self,
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

/// Reliably stream event envelopes to a consumer. Event envelope transmission
/// requests are sent over a channel and have a reply that is completed on the
/// remote consumer's acknowledgement of receipt.
pub async fn run<E>(
    event_consumer_addr: Uri,
    origin_id: StreamId,
    stream_id: StreamId,
    mut envelopes: mpsc::Receiver<(EventEnvelope<E>, oneshot::Sender<()>)>,
    mut kill_switch: oneshot::Receiver<()>,
) where
    E: Clone + Name + 'static,
{
    let mut delayer: Option<Delayer> = None;
    let mut connection = None;

    let mut unused_request: Option<(EventEnvelope<E>, oneshot::Sender<()>)> = None;

    'outer: loop {
        if let Err(oneshot::error::TryRecvError::Closed) = kill_switch.try_recv() {
            break;
        }

        if connection.is_none() {
            connection = if let Ok(connection) =
                proto::event_consumer_service_client::EventConsumerServiceClient::connect(
                    event_consumer_addr.clone(),
                )
                .await
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
        }

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
                        nanos: envelope.offset.timestamp.timestamp_nanos() as i32
                    };

                    if let Ok(any) = Any::from_msg(&envelope.event) {
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
                }
            });
            let result = connection.consume_event(request).await;

            if let Ok(response) = result {
                let mut stream_outs = response.into_inner();

                let mut in_flight = HashMap::new();

                fn push_in_flight<'a, E>(
                    in_flight: &'a mut HashMap<PersistenceId, VecDeque<(u64, oneshot::Sender<()>)>>,
                    envelope: &EventEnvelope<E>,
                    reply_to: oneshot::Sender<()>,
                ) -> &'a mut VecDeque<(u64, oneshot::Sender<()>)> {
                    let contexts = in_flight
                        .entry(envelope.persistence_id.clone())
                        .or_default();
                    contexts.push_back((envelope.seq_nr, reply_to));
                    contexts
                }

                if let Some((envelope, reply_to)) = unused_request.take() {
                    let (inner_reply_to, inner_reply) = oneshot::channel::<()>();

                    let contexts = push_in_flight(&mut in_flight, &envelope, inner_reply_to);

                    if event_in.send(envelope.clone()).is_ok() {
                        if inner_reply.await.is_ok() {
                            if reply_to.send(()).is_err() {
                                break 'outer;
                            }
                        } else {
                            unused_request = Some((envelope, reply_to));
                            contexts.pop_back();
                            continue;
                        }
                    } else {
                        unused_request = Some((envelope, reply_to));
                        contexts.pop_back();
                        continue;
                    }
                }

                loop {
                    tokio::select! {
                        request = envelopes.recv() => {
                            if let Some((envelope, reply_to)) = request {
                                let (inner_reply_to, inner_reply) = oneshot::channel();

                                let contexts = push_in_flight(&mut in_flight, &envelope, inner_reply_to);

                                if event_in.send(envelope.clone()).is_ok() && inner_reply.await.is_ok() {
                                    if reply_to.send(()).is_ok() {
                                        continue;
                                    } else {
                                        break 'outer;
                                    }
                                }

                                unused_request = Some((envelope, reply_to));
                                contexts.pop_back();

                                break;

                            } else {
                                break 'outer;
                            }
                        }

                        Some(Ok(proto::ConsumeEventOut {
                            message: Some(proto::consume_event_out::Message::Ack(proto::ConsumerEventAck { persistence_id, seq_nr })),
                        })) = stream_outs.next() => {
                            if let Ok(persistence_id) = persistence_id.parse() {
                                let contexts = in_flight
                                    .entry(persistence_id)
                                    .or_default();
                                while let Some((expected_seq_nr, reply_to)) = contexts.pop_front() {
                                    if seq_nr as u64 == expected_seq_nr  && reply_to.send(()).is_ok() {
                                        break;
                                    }
                                }
                            }
                        }

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
            _request: Request<Streaming<proto::ConsumeEventIn>>,
        ) -> std::result::Result<tonic::Response<Self::ConsumeEventStream>, tonic::Status> {
            todo!()
        }
    }

    #[ignore]
    #[test(tokio::test)]
    async fn can_flow() {
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
                    "127.0.0.1:50051".to_socket_addrs().unwrap().next().unwrap(),
                    task_kill_switch.notified(),
                )
                .await
                .unwrap();
        });

        let mut tried = 0;

        let (sender, receiver) = mpsc::channel(10);
        let (_task_kill_switch, task_kill_switch_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let _ = run(
                "http://127.0.0.1:50051".parse().unwrap(),
                OriginId::from("some-origin-id"),
                StreamId::from("some-stream-id"),
                receiver,
                task_kill_switch_receiver,
            )
            .await;
        });

        loop {
            let (reply, reply_receiver) = oneshot::channel();
            assert!(sender
                .send((
                    EventEnvelope {
                        // FIXME Flesh out these fields
                        persistence_id: "".parse().unwrap(),
                        seq_nr: 1,
                        event: prost_types::Duration {
                            seconds: 0,
                            nanos: 0
                        },
                        offset: TimestampOffset {
                            timestamp: Utc::now(),
                            seen: vec![]
                        }
                    },
                    reply,
                ))
                .await
                .is_ok());

            let ack = reply_receiver.await;

            tried += 1;

            if ack.is_err() && tried < 100 {
                continue;
            }

            assert!(ack.is_ok());

            break;
        }

        server_kill_switch.notified();
    }
}
