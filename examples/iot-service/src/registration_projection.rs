//! Handle registration projection concerns
//!

use std::sync::Arc;

use akka_persistence_rs::Message;
use akka_persistence_rs_commitlog::EventEnvelope;
use akka_projection_rs::{Handler, HandlerError};
use akka_projection_rs_commitlog::CommitLogSourceProvider;
use akka_projection_rs_storage::Command;
use async_trait::async_trait;
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
    receiver: mpsc::Receiver<Command>,
    temperature_sender: mpsc::Sender<Message<temperature::Command>>,
) {
    // Establish our source of events as a commit log.
    let source_provider = CommitLogSourceProvider::new(
        commit_log,
        EventEnvelopeMarshaler {
            events_key_secret_path: Arc::from(events_key_secret_path),
            secret_store,
        },
        "iot-service-projection",
        registration::EVENTS_TOPIC,
    );

    let handler = RegistrationHandler { temperature_sender };

    akka_projection_rs_storage::run(receiver, source_provider, handler).await
}
