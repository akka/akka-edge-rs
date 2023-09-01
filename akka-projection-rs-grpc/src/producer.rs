use akka_persistence_rs::EntityId;
use akka_persistence_rs::EntityType;
use akka_persistence_rs::PersistenceId;
use akka_projection_rs::Handler;
use akka_projection_rs::HandlerError;
use akka_projection_rs::SinkProvider;
use async_stream::stream;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tonic::{transport::Uri, Request};

use crate::delayer::Delayer;
use crate::proto;
use crate::EventEnvelope;
use crate::StreamId;

/// A handler to produce gRPC events from event envelopes expressed as an entity id
/// and event.
pub struct GrpcEventProducer<E> {
    entity_type: EntityType,
    grpc_producer: mpsc::Sender<(EventEnvelope<E>, oneshot::Sender<()>)>,
    offset: u64,
}

impl<E> GrpcEventProducer<E> {
    pub fn new(
        entity_type: EntityType,
        grpc_producer: mpsc::Sender<(EventEnvelope<E>, oneshot::Sender<()>)>,
    ) -> Self {
        Self {
            entity_type,
            grpc_producer,
            offset: 0,
        }
    }
}

#[async_trait]
impl<E> Handler for GrpcEventProducer<E>
where
    E: Send,
{
    type Envelope = (EntityId, Option<DateTime<Utc>>, E);

    async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        let (entity_id, timestamp, event) = envelope;

        let (reply, reply_receiver) = oneshot::channel();
        let persistence_id = PersistenceId::new(self.entity_type.clone(), entity_id);
        self.grpc_producer
            .send((
                EventEnvelope {
                    persistence_id: persistence_id.clone(),
                    event,
                    timestamp: timestamp.unwrap_or_else(Utc::now),
                    seen: vec![(persistence_id, self.offset)],
                },
                reply,
            ))
            .await
            .map_err(|_| HandlerError)?;
        reply_receiver.await.map_err(|_| HandlerError)?;

        self.offset = self.offset.wrapping_add(1);

        Ok(())
    }
}

/// Reliably stream event envelopes to a consumer. Event envelopes transmission
/// requests are sent over a channel and have a reply that is completed on the
/// remote consumer's acknowledgement of receipt.
pub struct GrpcSinkProvider<E> {
    delayer: Option<Delayer>,
    event_consumer_addr: Uri,
    origin_id: StreamId,
    stream_id: StreamId,
    phantom: PhantomData<E>,
}

impl<E> GrpcSinkProvider<E> {
    pub fn new(event_consumer_addr: Uri, origin_id: StreamId, stream_id: StreamId) -> Self {
        Self {
            delayer: None,
            event_consumer_addr,
            origin_id,
            stream_id,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<E> SinkProvider for GrpcSinkProvider<E>
where
    E: Message + Send + Sync + 'static,
{
    type Envelope = EventEnvelope<E>;

    async fn sink(&mut self, mut envelopes: mpsc::Receiver<(Self::Envelope, oneshot::Sender<()>)>) {
        let connection = if let Ok(connection) =
            proto::event_consumer_service_client::EventConsumerServiceClient::connect(
                self.event_consumer_addr.clone(),
            )
            .await
        {
            self.delayer = None;
            Some(connection)
        } else {
            if let Some(delayer) = &mut self.delayer {
                delayer.delay().await;
            } else {
                let mut delayer = Delayer::default();
                delayer.delay().await;
                self.delayer = Some(delayer);
            }
            None
        };

        if let Some(mut connection) = connection {
            let origin_id = self.origin_id.to_string();
            let stream_id = self.stream_id.to_string();

            let request = Request::new(stream! {
                yield proto::ConsumeEventIn {
                    message: Some(proto::consume_event_in::Message::Init(
                        proto::ConsumerEventInit {
                            origin_id,
                            stream_id,
                        },
                    )),
                };

                while let Some(_envelope) = envelopes.recv().await {
                    yield proto::ConsumeEventIn {
                        message: Some(proto::consume_event_in::Message::Event(
                            // FIXME Place the correct field values
                            proto::Event { persistence_id: String::from(""), seq_nr: 0, slice: 0, offset: None, payload: None, source: String::from(""), metadata: None, tags: vec![] },
                        )),
                    };
                }

            });
            let result = connection.consume_event(request).await;
            if let Ok(response) = result {
                let mut stream_outs = response.into_inner();
                if let Some(Ok(proto::ConsumeEventOut {
                    message: Some(proto::consume_event_out::Message::Start { .. }),
                })) = stream_outs.next().await
                {
                    // FIXME: if the ack info lines up with what we're expecting then reply to the associated oneshot.
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

        let mut flow_provider = GrpcSinkProvider::<u32>::new(
            "http://127.0.0.1:50051".parse().unwrap(),
            OriginId::from("some-origin-id"),
            StreamId::from("some-stream-id"),
        );

        let mut tried = 0;

        let (sender, receiver) = mpsc::channel(10);
        let _ = flow_provider.sink(receiver).await;

        loop {
            let (reply, reply_receiver) = oneshot::channel();
            assert!(sender
                .send((
                    EventEnvelope {
                        // FIXME Flesh out these fields
                        persistence_id: "".parse().unwrap(),
                        event: 0,
                        seen: vec![],
                        timestamp: Utc::now(),
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
