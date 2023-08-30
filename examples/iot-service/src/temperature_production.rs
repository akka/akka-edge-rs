//! Handle temperature projection concerns
//!

use akka_persistence_rs::EntityType;
use akka_persistence_rs_commitlog::EventEnvelope;
use akka_projection_rs::{Handler, HandlerError};
use akka_projection_rs_commitlog::CommitLogSourceProvider;
use akka_projection_rs_storage::Command;
use async_trait::async_trait;
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::sync::mpsc;
use tonic::transport::Uri;

use crate::temperature::{self, EventEnvelopeMarshaler};

/// A handler for forwarding on temperature envelopes from a projection source to
/// our temperature sensor entity.
pub struct TemperatureHandler {}

#[async_trait]
impl Handler for TemperatureHandler {
    type Envelope = EventEnvelope<temperature::Event>;

    async fn process(&mut self, _envelope: Self::Envelope) -> Result<(), HandlerError> {
        // self.temperature_sender
        //     .send(Message::new(
        //         envelope.entity_id,
        //         temperature::Command::Register { secret },
        //     ))
        //     .await
        //     .map(|_| ())
        //     .map_err(|_| HandlerError)

        Ok(())
    }
}

/// Apply sensor observations to a remote consumer.
pub async fn task(
    commit_log: FileLog,
    _event_consumer_addr: Uri,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
    offsets_key_secret_path: String,
    receiver: mpsc::Receiver<Command>,
    state_storage_path: PathBuf,
) {
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

    let handler = TemperatureHandler {};

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
