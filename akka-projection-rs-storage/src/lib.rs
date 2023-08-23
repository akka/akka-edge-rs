#![doc = include_str!("../README.md")]

use std::path::Path;

use akka_persistence_rs::{EntityType, Offset, WithOffset};
use akka_projection_rs::{Handler, SourceProvider};
use log::error;
use serde::{Deserialize, Serialize};
use streambed::{commit_log::Offset as CommitLogOffset, secret_store::SecretStore};
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt;

/// The commands that a projection task is receptive to.
pub enum Command {
    Stop,
}

#[derive(Default, Deserialize, Serialize)]
struct StorableState {
    last_offset: Option<CommitLogOffset>,
}

/// Provides local file system based storage for projection offsets.
pub async fn run<E, FSP, H, SP>(
    secret_store: &impl SecretStore,
    secret_path: &str,
    state_storage_path: &Path,
    mut receiver: Receiver<Command>,
    mut source_provider: FSP,
    handler: H,
) where
    E: WithOffset,
    H: Handler<Envelope = E>,
    FSP: FnMut(u32) -> Option<SP>,
    SP: SourceProvider<Envelope = E>,
{
    // Source our last offset recorded, if we have one.

    let mut storable_state =
        streambed_storage::load_struct(state_storage_path, secret_store, secret_path, |bytes| {
            ciborium::de::from_reader::<StorableState, _>(bytes)
        })
        .await
        .unwrap_or_default();

    // For now, we're going to produce a source provider for just one slice.
    // When we implement the gRPC consumer, we will likely have to do more.

    if let Some(source_provider) = source_provider(0) {
        // FIXME what to provide for entity type.
        // FIXME what to provide for slice min/max.
        let mut source = source_provider
            .events_by_slices(
                EntityType::from(""),
                0,
                0,
                storable_state.last_offset.map(Offset::Sequence),
            )
            .await;

        let serializer = |state: &StorableState| {
            let mut buf = Vec::new();
            ciborium::ser::into_writer(state, &mut buf).map(|_| buf)
        };

        loop {
            tokio::select! {
                envelope = source.next() => {
                    if let Some(envelope) = envelope {
                        let Offset::Sequence(offset) = envelope.offset();
                        if  handler.process(envelope).await.is_ok() {
                            storable_state.last_offset = Some(offset);
                            if streambed_storage::save_struct(
                                state_storage_path,
                                secret_store,
                                secret_path,
                                serializer,
                                rand::thread_rng,
                                &storable_state
                            )
                            .await.is_err() {
                                error!("Cannot persist offsets");
                            }
                        }
                    }
                }
                _ = receiver.recv() => {
                    break;
                }
                else => {
                    break;
                }
            }
        }
    } else {
        error!("Cannot obtain a source provider. Exiting the projection runner.");
    }
}
