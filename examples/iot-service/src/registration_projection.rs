//! Handle registration projection concerns
//!

use akka_persistence_rs::Message;
use akka_projection_rs::HandlerError;
use akka_projection_rs_grpc::{consumer::GrpcSourceProvider, EventEnvelope, StreamId};
use std::{path::PathBuf, time::Duration};
use streambed_confidant::FileSecretStore;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Uri;

use crate::registration;
use crate::temperature;

/// Apply sensor registrations to the temperature sensor entity.
pub async fn task(
    event_producer_addr: Uri,
    secret_store: FileSecretStore,
    offsets_key_secret_path: String,
    kill_switch: oneshot::Receiver<()>,
    state_storage_path: PathBuf,
    temperature_sender: mpsc::Sender<Message<temperature::Command>>,
) {
    // Establish our source of events either as a commit log or a gRPC
    // connection, depending on our feature configuration.

    let source_provider =
        GrpcSourceProvider::new(event_producer_addr, StreamId::from("registration-events"));

    // Declare a handler to forward projection events on to the temperature entity.

    let handler = |envelope: EventEnvelope<registration::Registered>| async {
        let (entity_id, secret) = {
            let secret = {
                let Some(registration::Registered {
                    secret: Some(secret),
                    ..
                }) = envelope.event
                else {
                    return Ok(());
                };
                secret.value.into()
            };
            (envelope.persistence_id.entity_id, secret)
        };

        temperature_sender
            .send(Message::new(
                entity_id,
                temperature::Command::Register { secret },
            ))
            .await
            .map(|_| ())
            .map_err(|_| HandlerError)
    };

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
