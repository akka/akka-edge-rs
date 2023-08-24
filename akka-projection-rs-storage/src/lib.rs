#![doc = include_str!("../README.md")]

use std::{path::Path, time::Duration};

use akka_persistence_rs::{Offset, WithOffset};
use akka_projection_rs::{Handler, SourceProvider};
use log::error;
use serde::{Deserialize, Serialize};
use streambed::{commit_log::Offset as CommitLogOffset, secret_store::SecretStore};
use tokio::{sync::mpsc::Receiver, time};
use tokio_stream::StreamExt;

/// The commands that a projection task is receptive to.
pub enum Command {
    Stop,
}

#[derive(Default, Deserialize, Serialize)]
struct StorableState {
    last_offset: Option<CommitLogOffset>,
}

/// Provides at-least-once local file system based storage for projection offsets,
/// meaning, for multiple runs of a projection, it is possible for events to repeat
/// from previous runs.
///
/// The `min_save_offset_interval` declares the minimum time between
/// having successfully handled an event by the handler to saving
/// the offset to storage. Therefore, high bursts of events are not
/// slowed down by having to persist an offset to storage. This strategy
/// works given the at-least-once semantics.
pub async fn run<E, H, SP>(
    secret_store: &impl SecretStore,
    secret_path: &str,
    state_storage_path: &Path,
    mut receiver: Receiver<Command>,
    source_provider: SP,
    handler: H,
    min_save_offset_interval: Duration,
) where
    E: WithOffset,
    H: Handler<Envelope = E>,
    SP: SourceProvider<Envelope = E>,
{
    let mut source = source_provider
        .source(|| async {
            streambed_storage::load_struct(state_storage_path, secret_store, secret_path, |bytes| {
                ciborium::de::from_reader::<StorableState, _>(bytes)
            })
            .await
            .ok()
            .and_then(|s| s.last_offset.map(Offset::Sequence))
        })
        .await;

    let serializer = |state: &StorableState| {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(state, &mut buf).map(|_| buf)
    };

    let mut next_save_offset_interval = Duration::MAX;
    let mut last_offset = None;

    loop {
        tokio::select! {
            envelope = source.next() => {
                if let Some(envelope) = envelope {
                    let Offset::Sequence(offset) = envelope.offset();
                    if  handler.process(envelope).await.is_ok() {
                        next_save_offset_interval = min_save_offset_interval;
                        last_offset = Some(offset);
                    }
                }
            }

            _ = time::sleep(next_save_offset_interval) => {
                if last_offset.is_some() {
                    let storable_state = StorableState {
                        last_offset
                    };
                    if streambed_storage::save_struct(
                        state_storage_path,
                        secret_store,
                        secret_path,
                        serializer,
                        rand::thread_rng,
                        &storable_state
                    ).await.is_err() {
                        error!("Cannot persist offsets");
                    }

                    next_save_offset_interval = Duration::MAX;
                    last_offset = None;
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
}
