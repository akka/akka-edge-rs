//! Handle temperature projection concerns
//!

use crate::proto;
use crate::temperature::{self, EventEnvelopeMarshaler};
use akka_persistence_rs::EntityType;
use akka_persistence_rs_commitlog::EventEnvelope as CommitLogEventEnvelope;
use akka_projection_rs::SinkProvider;
use akka_projection_rs_commitlog::CommitLogSourceProvider;
use akka_projection_rs_grpc::producer::{GrpcEventProducer, GrpcSinkProvider, Transformation};
use akka_projection_rs_grpc::{OriginId, StreamId};
use akka_projection_rs_storage::Command;
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::sync::mpsc;
use tonic::transport::Uri;

/// Apply sensor observations to a remote consumer.
pub async fn task(
    commit_log: FileLog,
    event_consumer_addr: Uri,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
    offsets_key_secret_path: String,
    receiver: mpsc::Receiver<Command>,
    state_storage_path: PathBuf,
) {
    // Establish a sink of envelopes that will be forwarded
    // on to a consumer via gRPC event producer.

    let (grpc_producer, grpc_producer_receiver) = mpsc::channel(10);

    let grpc_producer =
        GrpcEventProducer::new(EntityType::from(temperature::EVENTS_TOPIC), grpc_producer);

    tokio::spawn(async {
        let mut sink_provider = GrpcSinkProvider::new(
            event_consumer_addr,
            OriginId::from("edge-iot-service"),
            StreamId::from("temperature-events"),
        );

        sink_provider.sink(grpc_producer_receiver).await
    });

    // Establish our source of events as a commit log

    let source_provider = CommitLogSourceProvider::new(
        commit_log,
        EventEnvelopeMarshaler {
            events_key_secret_path: Arc::from(events_key_secret_path),
            secret_store: secret_store.clone(),
        },
        "iot-service-projection",
        Topic::from(temperature::EVENTS_TOPIC),
        EntityType::from(temperature::EVENTS_TOPIC),
    );

    // Optionally transform events from the commit log to values the
    // gRPC producer understands.

    let transformer = |envelope: CommitLogEventEnvelope<temperature::Event>| {
        let temperature::Event::TemperatureRead { temperature } = envelope.event else {
            return None;
        };

        let event = proto::TemperatureRead {
            temperature: temperature as i32,
        };

        Some(Transformation {
            entity_id: envelope.entity_id,
            timestamp: envelope.timestamp,
            event,
        })
    };

    // Finally, start up a projection that will use Streambed storage
    // to remember the offset consumed from the commit log. This then
    // permits us to restart from a specific point in the source given
    // restarts.
    // A handler is formed from the gRPC producer. This handler will
    // call upon the transformer function to, in turn, produce the
    // gRPC events to a remote consumer. The handler is a "flowing" one
    // where an upper limit of the number of envelopes in-flight is set.

    akka_projection_rs_storage::run(
        &secret_store,
        &offsets_key_secret_path,
        &state_storage_path,
        receiver,
        source_provider,
        grpc_producer.handler(transformer),
        Duration::from_millis(100),
    )
    .await
}
