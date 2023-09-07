//! Handle registration projection concerns
//!

#[cfg(feature = "local")]
use akka_persistence_rs::EntityType;
use akka_persistence_rs::Message;
#[cfg(feature = "local")]
use akka_persistence_rs_commitlog::EventEnvelope;
use akka_projection_rs::{Handler, HandlerError};
#[cfg(feature = "local")]
use akka_projection_rs_commitlog::CommitLogSourceProvider;
#[cfg(feature = "grpc")]
use akka_projection_rs_grpc::{consumer::GrpcSourceProvider, EventEnvelope, StreamId};
use async_trait::async_trait;
#[cfg(feature = "local")]
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
#[cfg(feature = "local")]
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
#[cfg(feature = "local")]
use streambed_logged::FileLog;
use tokio::sync::{mpsc, oneshot};
#[cfg(feature = "grpc")]
use tonic::transport::Uri;

#[cfg(feature = "grpc")]
use crate::registration;
#[cfg(feature = "local")]
use crate::registration::{self, EventEnvelopeMarshaler};
use crate::temperature;

/// A handler for forwarding on registration envelopes from a projection source to
/// our temperature sensor entity.
pub struct RegistrationHandler {
    temperature_sender: mpsc::Sender<Message<temperature::Command>>,
}

#[async_trait]
impl Handler for RegistrationHandler {
    #[cfg(feature = "local")]
    type Envelope = EventEnvelope<registration::Event>;

    #[cfg(feature = "grpc")]
    type Envelope = EventEnvelope<registration::Registered>;

    async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        #[cfg(feature = "local")]
        let (entity_id, secret) = {
            let registration::Event::Registered { secret } = envelope.event;
            (envelope.entity_id, secret)
        };

        #[cfg(feature = "grpc")]
        let (entity_id, secret) = {
            let secret = {
                let Some(registration::Registered {
                    secret: Some(secret),
                    ..
                }) = envelope.event
                else {
                    return Err(HandlerError);
                };
                secret.value.into()
            };
            (envelope.persistence_id.entity_id, secret)
        };

        self.temperature_sender
            .send(Message::new(
                entity_id,
                temperature::Command::Register { secret },
            ))
            .await
            .map(|_| ())
            .map_err(|_| HandlerError)
    }
}

/// Apply sensor registrations to the temperature sensor entity.
pub async fn task(
    #[cfg(feature = "local")] commit_log: FileLog,
    #[cfg(feature = "grpc")] event_producer_addr: Uri,
    secret_store: FileSecretStore,
    #[cfg(feature = "local")] events_key_secret_path: String,
    offsets_key_secret_path: String,
    kill_switch: oneshot::Receiver<()>,
    state_storage_path: PathBuf,
    temperature_sender: mpsc::Sender<Message<temperature::Command>>,
) {
    // Establish our source of events either as a commit log or a gRPC
    // connection, depending on our feature configuration.

    #[cfg(feature = "local")]
    let source_provider = CommitLogSourceProvider::new(
        commit_log,
        EventEnvelopeMarshaler {
            events_key_secret_path: Arc::from(events_key_secret_path),
            secret_store: secret_store.clone(),
        },
        "iot-service-projection",
        Topic::from(registration::EVENTS_TOPIC),
        EntityType::from(registration::EVENTS_TOPIC),
    );

    #[cfg(feature = "grpc")]
    let source_provider =
        GrpcSourceProvider::new(event_producer_addr, StreamId::from("registration-events"));

    // Declare a handler to forward projection events on to the temperature entity.

    let handler = RegistrationHandler { temperature_sender };

    // Finally, start up a projection that will use Streambed storage
    // to remember the offset consumed. This then permits us to restart
    // from a specific point in the source given restarts.

    akka_projection_rs_storage::run(
        &secret_store,
        &offsets_key_secret_path,
        &state_storage_path,
        kill_switch,
        source_provider,
        handler,
        Duration::from_millis(100),
    )
    .await
}
