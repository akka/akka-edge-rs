use akka_persistence_rs::EntityId;
use akka_persistence_rs::Message as EntityMessage;
use akka_persistence_rs::Offset;
use akka_persistence_rs::PersistenceId;
use akka_persistence_rs::TimestampOffset;
use akka_projection_rs::offset_store;
use akka_projection_rs::SourceProvider;
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use prost::Message;
use prost_types::Timestamp;
use std::{future::Future, marker::PhantomData, ops::Range, pin::Pin};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::{transport::Uri, Request};

use crate::delayer::Delayer;
use crate::proto;
use crate::EventEnvelope;
use crate::StreamId;

pub struct GrpcSourceProvider<E> {
    delayer: Option<Delayer>,
    event_producer_addr: Uri,
    offset_store: mpsc::Sender<EntityMessage<offset_store::Command>>,
    slice_range: Range<u32>,
    stream_id: StreamId,
    phantom: PhantomData<E>,
}

impl<E> GrpcSourceProvider<E> {
    pub fn new(
        event_producer_addr: Uri,
        stream_id: StreamId,
        offset_store: mpsc::Sender<EntityMessage<offset_store::Command>>,
    ) -> Self {
        let slice_range = akka_persistence_rs::slice_ranges(1);

        Self::with_slice_range(
            event_producer_addr,
            stream_id,
            offset_store,
            slice_range.get(0).cloned().unwrap(),
        )
    }

    pub fn with_slice_range(
        event_producer_addr: Uri,
        stream_id: StreamId,
        offset_store: mpsc::Sender<EntityMessage<offset_store::Command>>,
        slice_range: Range<u32>,
    ) -> Self {
        Self {
            delayer: None,
            event_producer_addr,
            offset_store,
            slice_range,
            stream_id,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<E> SourceProvider for GrpcSourceProvider<E>
where
    E: Default + Message + Send + Sync,
{
    type Envelope = EventEnvelope<E>;

    async fn source<F, FR>(
        &mut self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<Offset>> + Send,
    {
        let connection = if let Ok(connection) =
            proto::event_producer_service_client::EventProducerServiceClient::connect(
                self.event_producer_addr.clone(),
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
            let offset = offset().await.and_then(|offset| {
                if let Offset::Timestamp(TimestampOffset { timestamp, seen }) = offset {
                    Some(proto::Offset {
                        timestamp: Some(Timestamp {
                            seconds: timestamp.timestamp(),
                            nanos: timestamp.nanosecond() as i32,
                        }),
                        seen: seen
                            .into_iter()
                            .map(|(persistence_id, offset)| proto::PersistenceIdSeqNr {
                                persistence_id: persistence_id.to_string(),
                                seq_nr: offset as i64,
                            })
                            .collect(),
                    })
                } else {
                    None
                }
            });

            let request = Request::new(
                tokio_stream::iter(vec![proto::StreamIn {
                    message: Some(proto::stream_in::Message::Init(proto::InitReq {
                        stream_id: self.stream_id.to_string(),
                        slice_min: self.slice_range.start as i32,
                        slice_max: self.slice_range.end as i32 - 1,
                        offset,
                        filter: vec![],
                    })),
                }])
                .chain(tokio_stream::pending()),
            );

            let result = connection.events_by_slices(request).await;
            if let Ok(response) = result {
                let stream_offset_store = self.offset_store.clone();
                let mut stream_connection = connection.clone();
                let stream_stream_id = self.stream_id.to_string();
                let mut stream_outs = response.into_inner();
                Box::pin(stream! {
                    while let Some(stream_out) = stream_outs.next().await {
                        if let Ok(proto::StreamOut{ message: Some(proto::stream_out::Message::Event(streamed_event)) }) = stream_out {
                            // If we can't parse the persistence id then we abort.

                            let Ok(persistence_id) = streamed_event.persistence_id.parse::<PersistenceId>() else { break };

                            // Process the sequence number. If it isn't what we expect then we go round again.

                            let seq_nr = streamed_event.seq_nr as u64;

                            let (reply_to, reply_to_receiver) = oneshot::channel();
                            if stream_offset_store
                                .send(EntityMessage::new(
                                    EntityId::from(streamed_event.persistence_id.clone()),
                                    offset_store::Command::Get { reply_to },
                                ))
                                .await
                                .is_err()
                            {
                                break;
                            }

                            let next_seq_nr = if let Ok(offset_store::State { last_seq_nr }) = reply_to_receiver.await {
                                last_seq_nr.wrapping_add(1)
                            } else {
                                break
                            };

                            if seq_nr > next_seq_nr && streamed_event.source == "BT" {
                                // This shouldn't happen, if so then abort.
                                break;
                            } else if seq_nr != next_seq_nr {
                                // Duplicate or gap
                                continue;
                            }

                            // If the sequence number is what we expect and the producer is backtracking, then
                            // request its payload. If we can't get its payload then we abort as it is an error.

                            let streamed_event = if streamed_event.source == "BT" {
                                if let Ok(response) = stream_connection
                                    .load_event(proto::LoadEventRequest {
                                        stream_id: stream_stream_id.clone(),
                                        persistence_id: persistence_id.to_string(),
                                        seq_nr: seq_nr as i64,
                                    })
                                    .await
                                {
                                    if let Some(proto::load_event_response::Message::Event(event)) =
                                        response.into_inner().message
                                    {
                                        Some(event)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                Some(streamed_event)
                            };

                            let Some(streamed_event) = streamed_event else { break; };

                            // Parse the event and abort if we can't.

                            let event = if let Some(payload) = streamed_event.payload {
                                if !payload.type_url.starts_with("type.googleapis.com/") {
                                    break
                                }
                                let Ok(event) = E::decode(Bytes::from(payload.value)) else { break };
                                Some(event)
                            } else {
                                None
                            };

                            let Some(offset) = streamed_event.offset else { break };
                            let Some(timestamp) = offset.timestamp else { break };
                            let Some(timestamp) = NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32) else { break };
                            let timestamp = Utc.from_utc_datetime(&timestamp);
                            let seen = offset.seen.iter().flat_map(|pis| pis.persistence_id.parse().ok().map(|pid|(pid, pis.seq_nr as u64))).collect();
                            let offset = TimestampOffset { timestamp, seen };

                            // All is well, so emit the event.

                            yield EventEnvelope {persistence_id, seq_nr, event, offset};
                        }
                    }
                })
            } else {
                Box::pin(tokio_stream::empty())
            }
        } else {
            Box::pin(tokio_stream::empty())
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::proto::load_event_response;

    use super::*;
    use akka_persistence_rs::{EntityId, EntityType, PersistenceId};
    use async_stream::stream;
    use chrono::{DateTime, Utc};
    use prost_types::Any;
    use std::{net::ToSocketAddrs, sync::Arc};
    use test_log::test;
    use tokio::sync::Notify;
    use tokio_stream::StreamExt;
    use tonic::{transport::Server, Response, Status, Streaming};

    struct TestEventProducerService {
        event_time: DateTime<Utc>,
        event_seen_by: Vec<(PersistenceId, u64)>,
    }

    #[async_trait]
    impl proto::event_producer_service_server::EventProducerService for TestEventProducerService {
        type EventsBySlicesStream =
            Pin<Box<dyn Stream<Item = Result<proto::StreamOut, Status>> + Send>>;

        async fn events_by_slices(
            &self,
            _request: Request<Streaming<proto::StreamIn>>,
        ) -> Result<Response<Self::EventsBySlicesStream>, Status> {
            let stream_event_time = self.event_time;
            let stream_event_seen_by = self.event_seen_by.clone();
            Ok(Response::new(Box::pin(stream!({
                let mut value = Vec::with_capacity(6);
                0xffffffffu32.encode(&mut value).unwrap();

                yield Ok(proto::StreamOut {
                    message: Some(proto::stream_out::Message::Event(proto::Event {
                        persistence_id: "entity-type|entity-id".to_string(),
                        seq_nr: 2,
                        slice: 0,
                        offset: Some(proto::Offset {
                            timestamp: Some(Timestamp {
                                seconds: stream_event_time.timestamp(),
                                nanos: stream_event_time.nanosecond() as i32,
                            }),
                            seen: stream_event_seen_by
                                .iter()
                                .map(|(persistence_id, seq_nr)| proto::PersistenceIdSeqNr {
                                    persistence_id: persistence_id.to_string(),
                                    seq_nr: *seq_nr as i64,
                                })
                                .collect(),
                        }),
                        payload: Some(Any {
                            type_url: String::from(
                                "type.googleapis.com/google.protobuf.UInt32Value",
                            ),
                            value: value.clone(),
                        }),
                        source: "".to_string(),
                        metadata: None,
                        tags: vec![],
                    })),
                });

                yield Ok(proto::StreamOut {
                    message: Some(proto::stream_out::Message::Event(proto::Event {
                        persistence_id: "entity-type|entity-id".to_string(),
                        seq_nr: 1,
                        slice: 0,
                        offset: None,
                        payload: None,
                        source: "BT".to_string(),
                        metadata: None,
                        tags: vec![],
                    })),
                });
            }))))
        }

        async fn event_timestamp(
            &self,
            _request: Request<proto::EventTimestampRequest>,
        ) -> Result<Response<proto::EventTimestampResponse>, Status> {
            todo!()
        }

        async fn load_event(
            &self,
            _request: Request<proto::LoadEventRequest>,
        ) -> Result<Response<proto::LoadEventResponse>, Status> {
            let mut value = Vec::with_capacity(6);
            0xffffffffu32.encode(&mut value).unwrap();

            Ok(Response::new(proto::LoadEventResponse {
                message: Some(load_event_response::Message::Event(proto::Event {
                    persistence_id: "entity-type|entity-id".to_string(),
                    seq_nr: 1,
                    slice: 0,
                    offset: Some(proto::Offset {
                        timestamp: Some(Timestamp {
                            seconds: self.event_time.timestamp(),
                            nanos: self.event_time.nanosecond() as i32,
                        }),
                        seen: self
                            .event_seen_by
                            .iter()
                            .map(|(persistence_id, seq_nr)| proto::PersistenceIdSeqNr {
                                persistence_id: persistence_id.to_string(),
                                seq_nr: *seq_nr as i64,
                            })
                            .collect(),
                    }),
                    payload: Some(Any {
                        type_url: String::from("type.googleapis.com/google.protobuf.UInt32Value"),
                        value,
                    }),
                    source: "".to_string(),
                    metadata: None,
                    tags: vec![],
                })),
            }))
        }
    }

    #[test(tokio::test)]
    async fn can_source() {
        let entity_type = EntityType::from("entity-type");
        let entity_id = EntityId::from("entity-id");
        let persistence_id = PersistenceId::new(entity_type, entity_id.clone());
        let event_time = Utc::now();
        let event_seen_by = vec![(persistence_id.clone(), 1)];

        let server_kill_switch = Arc::new(Notify::new());

        let task_event_time = event_time;
        let task_event_seen_by = event_seen_by.clone();
        let task_kill_switch = server_kill_switch.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    proto::event_producer_service_server::EventProducerServiceServer::new(
                        TestEventProducerService {
                            event_time: task_event_time,
                            event_seen_by: task_event_seen_by,
                        },
                    ),
                )
                .serve_with_shutdown(
                    "127.0.0.1:50051".to_socket_addrs().unwrap().next().unwrap(),
                    task_kill_switch.notified(),
                )
                .await
                .unwrap();
        });

        let (offset_store, mut offset_store_receiver) = mpsc::channel(10);
        let task_persistence_id = persistence_id.clone();
        tokio::spawn(async move {
            while let Some(EntityMessage { entity_id, command }) =
                offset_store_receiver.recv().await
            {
                if entity_id == task_persistence_id.to_string() {
                    if let offset_store::Command::Get { reply_to } = command {
                        let _ = reply_to.send(offset_store::State { last_seq_nr: 0 });
                    }
                }
            }
        });

        let mut source_provider = GrpcSourceProvider::<u32>::new(
            "http://127.0.0.1:50051".parse().unwrap(),
            StreamId::from("some-string-id"),
            offset_store,
        );

        let mut tried = 0;

        loop {
            let task_event_seen_by = event_seen_by.clone();
            let mut source = source_provider
                .source(|| async {
                    Some(Offset::Timestamp(TimestampOffset {
                        timestamp: event_time,
                        seen: task_event_seen_by.clone(),
                    }))
                })
                .await;

            let envelope = source.next().await;

            tried += 1;

            if envelope.is_none() && tried < 100 {
                continue;
            }

            assert_eq!(
                envelope,
                Some(EventEnvelope {
                    persistence_id,
                    seq_nr: 1,
                    event: Some(0xffffffff),
                    offset: TimestampOffset {
                        timestamp: event_time,
                        seen: event_seen_by,
                    }
                })
            );

            break;
        }

        server_kill_switch.notified();
    }
}
