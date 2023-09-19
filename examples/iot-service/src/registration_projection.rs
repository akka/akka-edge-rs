// Handle registration projection concerns

use akka_persistence_rs::Message;
use akka_projection_rs::HandlerError;
use akka_projection_rs_grpc::{consumer::GrpcSourceProvider, EventEnvelope, StreamId};
use std::{path::PathBuf, time::Duration};
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Uri;

use crate::registration;
use crate::temperature;

// Declares the expected number of distinct device registrations.

const EXPECTED_DISTINCT_REGISTRATIONS: usize = 1000;

// Apply sensor registrations to the temperature sensor entity.

pub async fn task(
    commit_log: FileLog,
    event_producer_addr: Uri,
    secret_store: FileSecretStore,
    offsets_key_secret_path: String,
    kill_switch: oneshot::Receiver<()>,
    state_storage_path: PathBuf,
    temperature_command: mpsc::Sender<Message<temperature::Command>>,
) {
    // Establish our source of as a gRPC connection, and also setup
    // an offset store to track the ordering of remote events. In so doing,
    // we must also describe how an entity can be represented reliably as
    // distinct keys to this store. In the case here, because our entity
    // ids are numeric, we attempt a numeric conversion.

    let stream_id = StreamId::from("registration-events");

    let (offset_store, offset_store_receiver) = mpsc::channel(1);
    let offset_store_id = stream_id.clone();
    tokio::spawn(async move {
        akka_projection_rs_commitlog::offset_store::run(
            commit_log,
            EXPECTED_DISTINCT_REGISTRATIONS,
            offset_store_id,
            offset_store_receiver,
            |entity_id, _| entity_id.parse().ok(),
        )
        .await
    });

    let source_provider = GrpcSourceProvider::new(event_producer_addr, stream_id, offset_store);

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

        temperature_command
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
