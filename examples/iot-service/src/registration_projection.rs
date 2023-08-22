//! Handle registration projection concerns
//!

use std::sync::Arc;

use akka_persistence_rs::{slice_ranges, Message};
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
    let events_key_secret_path: Arc<str> = Arc::from(events_key_secret_path);

    // When it comes to having a projection sourced from a local
    // commit log, there's little benefit if having many of them.
    // We therefore manage all slices from just one projection.
    let slice_ranges = slice_ranges(1);

    // A closure to establish our source of events as a commit log.
    let source_provider = |slice| {
        Some(CommitLogSourceProvider::new(
            commit_log.clone(),
            EventEnvelopeMarshaler {
                events_key_secret_path: events_key_secret_path.clone(),
                secret_store: secret_store.clone(),
            },
            "iot-service-projection",
            registration::EVENTS_TOPIC,
            slice_ranges.get(slice as usize).cloned()?,
        ))
    };

    // Declare a handler to forward projection events on to the temperature entity.
    let handler = RegistrationHandler { temperature_sender };

    // Finally, start up a projection that will use Streambed storage
    // to remember the offset consumed. This then permits us to restart
    // from a specific point in the source given restarts.
    akka_projection_rs_storage::run(receiver, source_provider, handler).await
}
