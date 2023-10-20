use akka_persistence_rs::Message as EntityMessage;
use akka_persistence_rs::Offset;
use akka_persistence_rs::PersistenceId;
use akka_persistence_rs::TimestampOffset;
use akka_projection_rs::consumer_filter::FilterCriteria;
use akka_projection_rs::offset_store;
use akka_projection_rs::SourceProvider;
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use log::warn;
use prost::Message;
use prost_types::Timestamp;
use std::{future::Future, marker::PhantomData, ops::Range, pin::Pin};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;

use crate::delayer::Delayer;
use crate::proto;
use crate::EventEnvelope;
use crate::StreamId;

pub struct GrpcSourceProvider<E, EP> {
    consumer_filters: Option<watch::Receiver<Vec<FilterCriteria>>>,
    delayer: Option<Delayer>,
    event_producer_channel: EP,
    offset_store: mpsc::Sender<EntityMessage<offset_store::Command>>,
    slice_range: Range<u32>,
    stream_id: StreamId,
    phantom: PhantomData<E>,
}

impl<E, EP, EPR> GrpcSourceProvider<E, EP>
where
    EP: Fn() -> EPR,
    EPR: Future<Output = Result<Channel, tonic::transport::Error>>,
{
    pub fn new(
        event_producer_channel: EP,
        stream_id: StreamId,
        offset_store: mpsc::Sender<EntityMessage<offset_store::Command>>,
    ) -> Self {
        let slice_range = akka_persistence_rs::slice_ranges(1);

        Self::with_slice_range(
            event_producer_channel,
            stream_id,
            offset_store,
            slice_range.get(0).cloned().unwrap(),
        )
    }

    pub fn with_slice_range(
        event_producer_channel: EP,
        stream_id: StreamId,
        offset_store: mpsc::Sender<EntityMessage<offset_store::Command>>,
        slice_range: Range<u32>,
    ) -> Self {
        Self {
            consumer_filters: None,
            delayer: None,
            event_producer_channel,
            offset_store,
            slice_range,
            stream_id,
            phantom: PhantomData,
        }
    }

    pub fn with_consumer_filters(
        mut self,
        consumer_filters: watch::Receiver<Vec<FilterCriteria>>,
    ) -> Self {
        self.consumer_filters = Some(consumer_filters);
        self
    }
}

#[async_trait]
impl<E, EP, EPR> SourceProvider for GrpcSourceProvider<E, EP>
where
    E: Default + Message + Send + Sync,
    EP: Fn() -> EPR + Send + Sync,
    EPR: Future<Output = Result<Channel, tonic::transport::Error>> + Send,
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
        if let Some(delayer) = &mut self.delayer {
            delayer.delay().await;
        } else {
            let mut delayer = Delayer::default();
            delayer.delay().await;
            self.delayer = Some(delayer);
        }

        let connection = if let Ok(connection) = (self.event_producer_channel)()
            .await
            .map(proto::event_producer_service_client::EventProducerServiceClient::new)
        {
            self.delayer = Some(Delayer::default());
            Some(connection)
        } else {
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

            let stream_consumer_filters = self.consumer_filters.as_ref().cloned();

            let consumer_filters = stream! {
                if let Some(mut consumer_filters) = stream_consumer_filters {
                    while consumer_filters.changed().await.is_ok() {
                        let criteria: Vec<proto::FilterCriteria> = consumer_filters
                            .borrow()
                            .clone()
                            .into_iter()
                            .map(|c| c.into())
                            .collect();
                        yield proto::StreamIn {
                            message: Some(proto::stream_in::Message::Filter(proto::FilterReq {
                                criteria,
                            })),
                        };
                    }
                } else {
                    futures::future::pending::<()>().await;
                }
            };

            let request = Request::new(
                tokio_stream::iter(vec![proto::StreamIn {
                    message: Some(proto::stream_in::Message::Init(proto::InitReq {
                        stream_id: self.stream_id.to_string(),
                        slice_min: self.slice_range.start as i32,
                        slice_max: self.slice_range.end as i32 - 1,
                        offset,
                        filter: self
                            .consumer_filters
                            .as_ref()
                            .map_or(vec![], |consumer_filters| {
                                consumer_filters
                                    .borrow()
                                    .clone()
                                    .into_iter()
                                    .map(|c| c.into())
                                    .collect()
                            }),
                    })),
                }])
                .chain(consumer_filters),
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

                            let Ok(persistence_id) = streamed_event.persistence_id.parse::<PersistenceId>() else {
                                warn!("Cannot parse persistence id: {}. Aborting stream.", streamed_event.persistence_id);
                                break
                            };

                            let entity_id = persistence_id.entity_id.clone();

                            // Process the sequence number. If it isn't what we expect then we go round again.

                            let seq_nr = streamed_event.seq_nr as u64;

                            let (reply_to, reply_to_receiver) = oneshot::channel();
                            if stream_offset_store
                                .send(EntityMessage::new(
                                    entity_id.clone(),
                                    offset_store::Command::Get { reply_to },
                                ))
                                .await
                                .is_err()
                            {
                                warn!("Cannot send to the offset store: {}. Aborting stream.", streamed_event.persistence_id);
                                break;
                            }

                            let next_seq_nr = if let Ok(offset_store::State { last_seq_nr }) = reply_to_receiver.await {
                                last_seq_nr.wrapping_add(1)
                            } else {
                                warn!("Cannot receive from the offset store: {}. Aborting stream.", streamed_event.persistence_id);
                                break
                            };

                            if seq_nr > next_seq_nr && streamed_event.source == "BT" {
                                // This shouldn't happen, if so then abort.
                                warn!("Back track received for a future event: {}. Aborting stream.", streamed_event.persistence_id);
                                break;
                            } else if seq_nr != next_seq_nr {
                                // Duplicate or gap
                                continue;
                            }

                            // If the sequence number is what we expect and the producer is backtracking, then
                            // request its payload. If we can't get its payload then we abort as it is an error.

                            let resolved_streamed_event = if streamed_event.source == "BT" {
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
                                        warn!("Cannot receive an backtrack event: {}. Aborting stream.", streamed_event.persistence_id);
                                        None
                                    }
                                } else {
                                    warn!("Cannot obtain an backtrack event: {}. Aborting stream.", streamed_event.persistence_id);
                                    None
                                }
                            } else {
                                Some(streamed_event)
                            };

                            let Some(streamed_event) = resolved_streamed_event else { break; };

                            // Parse the event and abort if we can't.

                            let event = if let Some(payload) = streamed_event.payload {
                                if !payload.type_url.starts_with("type.googleapis.com/") {
                                    warn!("Payload type was not expected: {}: {}. Aborting stream.", streamed_event.persistence_id, payload.type_url);
                                    break
                                }
                                let Ok(event) = E::decode(Bytes::from(payload.value)) else { break };
                                Some(event)
                            } else {
                                None
                            };

                            let Some(offset) = streamed_event.offset else {
                                warn!("Payload offset was not present: {}. Aborting stream.", streamed_event.persistence_id);
                                break;
                            };
                            let Some(timestamp) = offset.timestamp else {
                                warn!("Payload timestamp was not present: {}. Aborting stream.", streamed_event.persistence_id);
                                break;
                            };
                            let Some(timestamp) = NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32) else {
                                warn!("Payload timestamp was not able to be converted: {}: {}. Aborting stream.", streamed_event.persistence_id, timestamp);
                                break;
                            };
                            let timestamp = Utc.from_utc_datetime(&timestamp);
                            let seen = offset.seen.iter().flat_map(|pis| pis.persistence_id.parse().ok().map(|pid|(pid, pis.seq_nr as u64))).collect();
                            let offset = TimestampOffset { timestamp, seen };

                            // Save the offset

                            if stream_offset_store
                                .send(EntityMessage::new(
                                    entity_id,
                                    offset_store::Command::Save { seq_nr },
                                ))
                                .await
                                .is_err()
                            {
                                warn!("Cannot save to the offset store: {}. Aborting stream.", streamed_event.persistence_id);
                                break;
                            }

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
    use akka_projection_rs::consumer_filter::{self, EntityIdOffset};
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
            request: Request<Streaming<proto::StreamIn>>,
        ) -> Result<Response<Self::EventsBySlicesStream>, Status> {
            let mut inner = request.into_inner();

            let Some(Ok(proto::StreamIn {
                message: Some(proto::stream_in::Message::Init(proto::InitReq { filter, .. })),
            })) = inner.next().await
            else {
                return Err(Status::aborted("Expected the initial request"));
            };

            if filter.is_empty() {
                return Err(Status::aborted(
                    "Expected the initial request to have a filter",
                ));
            }

            let Some(Ok(proto::StreamIn {
                message: Some(proto::stream_in::Message::Filter(proto::FilterReq { criteria, .. })),
            })) = inner.next().await
            else {
                return Err(Status::aborted("Expected the criteria to be updated"));
            };

            if criteria.is_empty() {
                return Err(Status::aborted(
                    "Expected the filter request to have a filter",
                ));
            }

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
        let offset_saved = Arc::new(Notify::new());

        let task_event_time = event_time;
        let task_event_seen_by = event_seen_by.clone();
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
                    server_kill_switch.notified(),
                )
                .await
                .unwrap();
        });

        let (offset_store, mut offset_store_receiver) = mpsc::channel(10);
        let task_entity_id = entity_id.clone();
        let task_offset_saved = offset_saved.clone();
        tokio::spawn(async move {
            while let Some(EntityMessage { entity_id, command }) =
                offset_store_receiver.recv().await
            {
                if entity_id == task_entity_id.to_string() {
                    match command {
                        offset_store::Command::Get { reply_to } => {
                            let _ = reply_to.send(offset_store::State { last_seq_nr: 0 });
                        }
                        offset_store::Command::Save { seq_nr } => {
                            assert_eq!(seq_nr, 1);
                            task_offset_saved.notify_one();
                        }
                    }
                }
            }
        });

        let (consumer_filters, consumer_filters_receiver) =
            watch::channel(vec![FilterCriteria::IncludeEntityIds {
                entity_id_offsets: vec![EntityIdOffset {
                    entity_id: persistence_id.entity_id.clone(),
                    seq_nr: 0,
                }],
            }]);

        let channel = Channel::from_static("http://127.0.0.1:50051");
        let mut source_provider = GrpcSourceProvider::<u32, _>::new(
            || channel.connect(),
            StreamId::from("some-string-id"),
            offset_store,
        )
        .with_consumer_filters(consumer_filters_receiver);

        assert!(consumer_filters
            .send(vec![consumer_filter::exclude_all()])
            .is_ok());

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
    }
}
