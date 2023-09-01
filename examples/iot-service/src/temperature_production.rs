//! Handle temperature projection concerns
//!

use crate::proto;
use crate::temperature::{self, EventEnvelopeMarshaler};
use akka_persistence_rs::EntityType;
use akka_persistence_rs_commitlog::EventEnvelope as CommitLogEventEnvelope;
use akka_projection_rs::SinkProvider;
use akka_projection_rs::{Handler, HandlerError};
use akka_projection_rs_commitlog::CommitLogSourceProvider;
use akka_projection_rs_grpc::producer::{GrpcEventProducer, GrpcSinkProvider};
use akka_projection_rs_grpc::{OriginId, StreamId};
use akka_projection_rs_storage::Command;
use async_trait::async_trait;
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::sync::mpsc;
use tonic::transport::Uri;

/// A handler for forwarding on temperature envelopes from a projection source to
/// a remote gRPC consumer.
pub struct TemperatureHandler {
    grpc_producer: GrpcEventProducer<proto::TemperatureRead>,
}

#[async_trait]
impl Handler for TemperatureHandler {
    type Envelope = CommitLogEventEnvelope<temperature::Event>;

    async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        let temperature::Event::TemperatureRead { temperature } = envelope.event else {
            return Ok(());
        };

        let event = proto::TemperatureRead {
            sensor_id: envelope.entity_id.to_string(),
            temperature: temperature as i32,
        };

        self.grpc_producer
            .process((envelope.entity_id, envelope.timestamp, event))
            .await
    }
}

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
    // on to a consumer via gRPC producer handler.

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

    // Declare a handler to forward projection events on to the remote consumer.

    let handler = TemperatureHandler { grpc_producer };

    // Finally, start up a projection that will use Streambed storage
    // to remember the offset consumed. This then permits us to restart
    // from a specific point in the source given restarts.

    akka_projection_rs_storage::run(
        &secret_store,
        &offsets_key_secret_path,
        &state_storage_path,
        receiver,
        source_provider,
        handler,
        Duration::from_millis(100),
    )
    .await
}
