use akka_persistence_rs::Offset;
use akka_persistence_rs::PersistenceId;
use akka_persistence_rs::TimestampOffset;
use akka_projection_rs::consumer_filter::FilterCriteria;
use akka_projection_rs::offset_store::LastOffset;
use akka_projection_rs::SourceProvider;
use async_stream::stream;
use async_trait::async_trait;
use chrono::Timelike;
use log::warn;
use prost::Message;
use prost_types::Timestamp;
use std::{future::Future, marker::PhantomData, ops::Range, pin::Pin};
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
    event_producer_channel: EP,
    slice_range: Range<u32>,
    stream_id: StreamId,
    phantom: PhantomData<E>,
}

impl<E, EP, EPR> GrpcSourceProvider<E, EP>
where
    EP: Fn() -> EPR,
    EPR: Future<Output = Result<Channel, tonic::transport::Error>>,
{
    pub fn new(event_producer_channel: EP, stream_id: StreamId) -> Self {
        let slice_range = akka_persistence_rs::slice_ranges(1);

        Self::with_slice_range(
            event_producer_channel,
            stream_id,
            slice_range.get(0).cloned().unwrap(),
        )
    }

    pub fn with_slice_range(
        event_producer_channel: EP,
        stream_id: StreamId,
        slice_range: Range<u32>,
    ) -> Self {
        Self {
            consumer_filters: None,
            event_producer_channel,
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
        &self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<LastOffset>> + Send,
    {
        let mut delayer: Option<Delayer> = None;

        Box::pin(stream! {
            'outer: loop {
                if let Some(delayer) = &mut delayer {
                    delayer.delay().await;
                } else {
                    let mut d = Delayer::default();
                    d.delay().await;
                    delayer = Some(d);
                }

                let connection = if let Ok(connection) = (self.event_producer_channel)()
                    .await
                    .map(proto::event_producer_service_client::EventProducerServiceClient::new)
                {
                    delayer = Some(Delayer::default());
                    Some(connection)
                } else {
                    continue 'outer;
                };

                if let Some(mut connection) = connection {
                    let offset = offset().await.and_then(|(seen, offset)| {
                        if let (seen, Offset::Timestamp(TimestampOffset { timestamp, seq_nr })) =
                            (seen, offset)
                        {
                            Some(proto::Offset {
                                timestamp: Some(Timestamp {
                                    seconds: timestamp.timestamp(),
                                    nanos: timestamp.nanosecond() as i32,
                                }),
                                seen: seen
                                    .into_iter()
                                    .map(|persistence_id| proto::PersistenceIdSeqNr {
                                        persistence_id: persistence_id.to_string(),
                                        seq_nr: seq_nr as i64,
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
                                filter: self.consumer_filters.as_ref().map_or(
                                    vec![],
                                    |consumer_filters| {
                                        consumer_filters
                                            .borrow()
                                            .clone()
                                            .into_iter()
                                            .map(|c| c.into())
                                            .collect()
                                    },
                                ),
                            })),
                        }])
                        .chain(consumer_filters),
                    );

                    let result = connection.events_by_slices(request).await;
                    if let Ok(response) = result {
                        let mut stream_outs = response.into_inner();
                        while let Some(stream_out) = stream_outs.next().await {
                            if let Ok(proto::StreamOut{ message: Some(proto::stream_out::Message::Event(streamed_event)) }) = stream_out {
                                // Marshal and abort if we can't.

                                let Ok(envelope) = streamed_event.try_into() else {
                                    warn!("Cannot marshal envelope. Aborting stream.");
                                    break
                                };

                                // All is well, so emit the event.

                                yield envelope;
                            }
                        }
                    }
                }
            }
        })
    }

    async fn load_envelope(
        &self,
        persistence_id: PersistenceId,
        seq_nr: u64,
    ) -> Option<Self::Envelope> {
        if let Ok(mut connection) = (self.event_producer_channel)()
            .await
            .map(proto::event_producer_service_client::EventProducerServiceClient::new)
        {
            if let Ok(response) = connection
                .load_event(proto::LoadEventRequest {
                    stream_id: self.stream_id.to_string(),
                    persistence_id: persistence_id.to_string(),
                    seq_nr: seq_nr as i64,
                })
                .await
            {
                if let Some(proto::load_event_response::Message::Event(event)) =
                    response.into_inner().message
                {
                    if let Ok(envelope) = event.try_into() {
                        Some(envelope)
                    } else {
                        warn!(
                            "Cannot marshal envelope for: {}. Aborting stream.",
                            persistence_id
                        );
                        None
                    }
                } else {
                    warn!("Cannot load an event due to parsing: {}.", persistence_id);
                    None
                }
            } else {
                warn!(
                    "Cannot load an event due to request failure: {}.",
                    persistence_id
                );
                None
            }
        } else {
            warn!(
                "Cannot load an event due to connection failure: {}.",
                persistence_id
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::proto::load_event_response;

    use super::*;
    use akka_persistence_rs::{EntityId, EntityType, PersistenceId, Source};
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
                        seq_nr: 1,
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

        let (consumer_filters, consumer_filters_receiver) =
            watch::channel(vec![FilterCriteria::IncludeEntityIds {
                entity_id_offsets: vec![EntityIdOffset {
                    entity_id: persistence_id.entity_id.clone(),
                    seq_nr: 0,
                }],
            }]);

        let channel = Channel::from_static("http://127.0.0.1:50051");
        let source_provider = GrpcSourceProvider::<u32, _>::new(
            || channel.connect(),
            StreamId::from("some-string-id"),
        )
        .with_consumer_filters(consumer_filters_receiver);

        assert!(consumer_filters
            .send(vec![consumer_filter::exclude_all()])
            .is_ok());

        let mut tried = 0;

        loop {
            let task_persistence_id = persistence_id.clone();
            let mut source = source_provider
                .source(|| {
                    let task_persistence_id = task_persistence_id.clone();
                    async {
                        Some((
                            vec![task_persistence_id],
                            Offset::Timestamp(TimestampOffset {
                                timestamp: event_time,
                                seq_nr: 1,
                            }),
                        ))
                    }
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
                    timestamp: event_time,
                    seq_nr: 1,
                    source: Source::Regular,
                    event: Some(0xffffffff),
                })
            );

            break;
        }
    }
}
