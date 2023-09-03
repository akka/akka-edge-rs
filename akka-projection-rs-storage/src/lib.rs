#![doc = include_str!("../README.md")]

use std::{path::Path, time::Duration};

use akka_persistence_rs::{Offset, WithOffset};
use akka_projection_rs::{Handler, Handlers, PendingHandler, SourceProvider};
use log::error;
use serde::{Deserialize, Serialize};
use streambed::secret_store::SecretStore;
use tokio::{sync::mpsc::Receiver, time};
use tokio_stream::StreamExt;

/// The commands that a projection task is receptive to.
pub enum Command {
    Stop,
}

#[derive(Default, Deserialize, Serialize)]
struct StorableState {
    last_offset: Option<Offset>,
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
pub async fn run<A, B, E, IH, SP>(
    secret_store: &impl SecretStore,
    secret_path: &str,
    state_storage_path: &Path,
    mut receiver: Receiver<Command>,
    mut source_provider: SP,
    handler: IH,
    min_save_offset_interval: Duration,
) where
    A: Handler<Envelope = E> + Send,
    B: PendingHandler<Envelope = E> + Send,
    E: WithOffset,
    IH: Into<Handlers<A, B>>,
    SP: SourceProvider<Envelope = E>,
{
    let mut handler = handler.into();

    'outer: loop {
        let mut source = source_provider
            .source(|| async {
                streambed_storage::load_struct(
                    state_storage_path,
                    secret_store,
                    secret_path,
                    |bytes| ciborium::de::from_reader::<StorableState, _>(bytes),
                )
                .await
                .ok()
                .and_then(|s| s.last_offset)
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
                        let offset = envelope.offset();
                        match &mut handler {
                            Handlers::Completed(handler, _) => {
                                if handler.process(envelope).await.is_err() {
                                    break;
                                }
                            }
                            Handlers::Pending(handler, _) => {
                                if let Ok(pending) = handler.process_pending(envelope).await {
                                    if pending.await.is_err() {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                        next_save_offset_interval = min_save_offset_interval;
                        last_offset = Some(offset);
                    } else {
                        break;
                    }
                }

                _ = time::sleep(next_save_offset_interval) => {
                    next_save_offset_interval = Duration::MAX;
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
                        last_offset = None;
                    }
            }

                _ = receiver.recv() => {
                    break 'outer;
                }

                else => {
                    break 'outer;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, fs, future::Future, marker::PhantomData, pin::Pin};

    use super::*;
    use akka_persistence_rs::EntityId;
    use akka_persistence_rs_commitlog::EventEnvelope;
    use akka_projection_rs::{HandlerError, UnusedFlowingHandler};
    use async_stream::stream;
    use async_trait::async_trait;
    use serde::Deserialize;
    use streambed::secret_store::{
        AppRoleAuthReply, Error, GetSecretReply, SecretData, SecretStore, UserPassAuthReply,
    };
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_stream::Stream;

    // Scaffolding

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    struct MyEvent {
        value: String,
    }

    // Developers are expected to provide a marshaler of events.
    // The marshaler is responsible for more than just the serialization
    // of an envelope. Extracting/saving an entity id and determining other
    // metadata is also important. We would also expect to see any encryption
    // and decyption being performed by the marshaler.
    // The example here overrides the default methods of the marshaler and
    // effectively ignores the use of a secret key; just to prove that you really
    // can lay out an envelope any way that you would like to. Note that secret keys
    // are important though.

    #[derive(Clone)]
    struct NoopSecretStore;

    #[async_trait]
    impl SecretStore for NoopSecretStore {
        async fn approle_auth(
            &self,
            _role_id: &str,
            _secret_id: &str,
        ) -> Result<AppRoleAuthReply, Error> {
            panic!("should not be called")
        }

        async fn create_secret(
            &self,
            _secret_path: &str,
            _secret_data: SecretData,
        ) -> Result<(), Error> {
            panic!("should not be called")
        }

        async fn get_secret(&self, _secret_path: &str) -> Result<Option<GetSecretReply>, Error> {
            let mut data = HashMap::new();
            data.insert(
                "value".to_string(),
                "ed31e94c161aea6ff2300c72b17741f71b616463f294dac0542324bbdbf8a2de".to_string(),
            );

            Ok(Some(GetSecretReply {
                lease_duration: 10,
                data: SecretData { data },
            }))
        }

        async fn token_auth(&self, _token: &str) -> Result<(), Error> {
            panic!("should not be called")
        }

        async fn userpass_auth(
            &self,
            _username: &str,
            _password: &str,
        ) -> Result<UserPassAuthReply, Error> {
            panic!("should not be called")
        }

        async fn userpass_create_update_user(
            &self,
            _current_username: &str,
            _username: &str,
            _password: &str,
        ) -> Result<(), Error> {
            panic!("should not be called")
        }
    }

    const MIN_SAVE_OFFSET_INTERVAL: Duration = Duration::from_millis(100);

    struct MySourceProvider {
        entity_id: EntityId,
        event_value: String,
    }

    #[async_trait]
    impl SourceProvider for MySourceProvider {
        type Envelope = EventEnvelope<MyEvent>;

        async fn source<F, FR>(
            &mut self,
            offset: F,
        ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
        where
            F: Fn() -> FR + Send + Sync,
            FR: Future<Output = Option<Offset>> + Send,
        {
            Box::pin(stream! {
                if offset().await.is_none() {
                    yield EventEnvelope::new(self.entity_id.clone(), None, MyEvent { value:self.event_value.clone() }, 0);
                    time::sleep(MIN_SAVE_OFFSET_INTERVAL * 2).await;
                }
            })
        }
    }

    struct MyHandler {
        entity_id: EntityId,
        event_value: String,
    }

    #[async_trait]
    impl Handler for MyHandler {
        type Envelope = EventEnvelope<MyEvent>;

        /// Process an envelope.
        async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
            assert_eq!(
                envelope,
                EventEnvelope {
                    entity_id: self.entity_id.clone(),
                    timestamp: None,
                    event: MyEvent {
                        value: self.event_value.clone()
                    },
                    offset: 0
                }
            );
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn can_run() {
        let storage_path = env::temp_dir().join("can_run");
        let _ = fs::remove_dir_all(&storage_path);
        let _ = fs::create_dir_all(&storage_path);
        println!("Writing to {}", storage_path.to_string_lossy());

        // Scaffolding

        let entity_id = EntityId::from("some-entity");
        let event_value = "some value".to_string();

        // Process an event.
        let (_registration_projection_command, registration_projection_command_receiver) =
            mpsc::channel(1);

        let task_storage_path = storage_path.clone();
        tokio::spawn(async move {
            run(
                &NoopSecretStore,
                "some-secret-path",
                &task_storage_path,
                registration_projection_command_receiver,
                MySourceProvider {
                    entity_id: entity_id.clone(),
                    event_value: event_value.clone(),
                },
                Handlers::Completed(
                    MyHandler {
                        entity_id: entity_id.clone(),
                        event_value: event_value.clone(),
                    },
                    UnusedFlowingHandler {
                        phantom: PhantomData,
                    },
                ),
                MIN_SAVE_OFFSET_INTERVAL,
            )
            .await
        });

        // Wait until our storage file becomes available, which means
        // that an event will have to have been successfully processed.
        let mut file_found = false;
        for _ in 0..10 {
            if fs::metadata(&storage_path).is_ok() {
                file_found = true;
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        assert!(file_found);
    }
}
