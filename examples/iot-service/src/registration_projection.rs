//! Handle registration projection concerns
//!

use std::{path::PathBuf, sync::Arc, time::Duration};

use akka_persistence_rs::{EntityType, Message};
use akka_persistence_rs_commitlog::EventEnvelope;
use akka_projection_rs::{Handler, HandlerError};
use akka_projection_rs_commitlog::CommitLogSourceProvider;
use akka_projection_rs_storage::Command;
use async_trait::async_trait;
use streambed::commit_log::Topic;
use streambed_confidant::FileSecretStore;
use streambed_logged::FileLog;
use tokio::sync::mpsc;

use crate::{
    registration::{self, EventEnvelopeMarshaler},
    temperature,
};

/// A handler for forwarding on registration envelopes from a projection source to
/// our temperature sensor entity.
pub struct RegistrationHandler {
    temperature_sender: mpsc::Sender<Message<temperature::Command>>,
}

#[async_trait]
impl Handler for RegistrationHandler {
    type Envelope = EventEnvelope<registration::Event>;

    async fn process(&self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        let registration::Event::Registered { secret } = envelope.event;
        self.temperature_sender
            .send(Message::new(
                envelope.entity_id,
                temperature::Command::Register { secret },
            ))
            .await
            .map(|_| ())
            .map_err(|_| HandlerError)
    }
}

/// Apply sensor registrations to the temperature sensor entity.
pub async fn task(
    commit_log: FileLog,
    secret_store: FileSecretStore,
    events_key_secret_path: String,
    offsets_key_secret_path: String,
    receiver: mpsc::Receiver<Command>,
    state_storage_path: PathBuf,
    temperature_sender: mpsc::Sender<Message<temperature::Command>>,
) {
    // Establish our source of events as a commit log.
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

    // Declare a handler to forward projection events on to the temperature entity.
    let handler = RegistrationHandler { temperature_sender };

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