#![doc = include_str!("../README.md")]

use std::{collections::VecDeque, path::Path, pin::Pin, time::Duration};

use akka_persistence_rs::{Offset, WithOffset};
use akka_projection_rs::{Handler, HandlerError, Handlers, PendingHandler, SourceProvider};
use futures::{self, future, stream, Future, Stream};
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
    E: WithOffset + Send,
    IH: Into<Handlers<A, B>>,
    SP: SourceProvider<Envelope = E>,
{
    let mut handler = handler.into();

    let mut handler_futures = VecDeque::with_capacity(B::MAX_PENDING);
    let mut always_pending_handler: Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>> =
        Box::pin(future::pending());

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

        let mut always_pending_source: Pin<Box<dyn Stream<Item = E> + Send>> =
            Box::pin(stream::pending());

        let serializer = |state: &StorableState| {
            let mut buf = Vec::new();
            ciborium::ser::into_writer(state, &mut buf).map(|_| buf)
        };

        let mut active_source = &mut source;
        let mut next_save_offset_interval = Duration::MAX;
        let mut last_offset = None;

        loop {
            tokio::select! {
                envelope = active_source.next() => {
                    if let Some(envelope) = envelope {
                        let offset = envelope.offset();
                        match &mut handler {
                            Handlers::Ready(handler, _) => {
                                if handler.process(envelope).await.is_ok() {
                                    next_save_offset_interval = min_save_offset_interval;
                                    last_offset = Some(offset);
                                } else {
                                    break;
                                }
                            }
                            Handlers::Pending(handler, _) => {
                                if let Ok(pending) = handler.process_pending(envelope).await {
                                    handler_futures.push_back((pending, offset));
                                    // If we've reached the limit on the pending futures in-flight
                                    // then back off sourcing more.
                                    if handler_futures.len() == B::MAX_PENDING {
                                        active_source = &mut always_pending_source;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }

                pending = handler_futures.get_mut(0).map_or_else(|| &mut always_pending_handler, |(f, _)| f) => {
                    // A pending future will never complete so this MUST mean that we have a element in our queue.
                    let (_, offset) = handler_futures.pop_front().unwrap();

                    // We've freed up a slot on the pending futures in-flight, so allow more events to be received.
                    active_source = &mut source;

                    // All is well with our pending future so we can finally cause the offset to be persisted.
                    if pending.is_ok() {
                        next_save_offset_interval = min_save_offset_interval;
                        last_offset = Some(offset);
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
    use std::{collections::HashMap, env, fs, future::Future, pin::Pin};

    use super::*;
    use akka_persistence_rs::EntityId;
    use akka_persistence_rs_commitlog::EventEnvelope;
    use akka_projection_rs::HandlerError;
    use async_stream::stream;
    use async_trait::async_trait;
    use chrono::Utc;
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
                "ed31e94c161aea6ff2300c72b17741f7".to_string(),
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
                    yield EventEnvelope::new(self.entity_id.clone(), 1, Utc::now(), MyEvent { value:self.event_value.clone() }, 0);
                }
            }.chain(stream::pending()))
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
            assert_eq!(envelope.entity_id, self.entity_id);
            assert_eq!(
                envelope.event,
                MyEvent {
                    value: self.event_value.clone()
                }
            );
            Ok(())
        }
    }

    struct MyHandlerPending {
        entity_id: EntityId,
        event_value: String,
    }

    #[async_trait]
    impl PendingHandler for MyHandlerPending {
        type Envelope = EventEnvelope<MyEvent>;

        const MAX_PENDING: usize = 1;

        /// Process an envelope.
        async fn process_pending(
            &mut self,
            envelope: Self::Envelope,
        ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError>
        {
            assert_eq!(envelope.entity_id, self.entity_id);
            assert_eq!(
                envelope.event,
                MyEvent {
                    value: self.event_value.clone()
                }
            );
            Ok(Box::pin(future::ready(Ok(()))))
        }
    }

    #[test(tokio::test)]
    async fn can_run_ready() {
        let storage_path = env::temp_dir().join("can_run_completed");
        let _ = fs::remove_dir_all(&storage_path);
        let _ = fs::create_dir_all(&storage_path);
        let storage_path = storage_path.join("offsets");
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
                MyHandler {
                    entity_id: entity_id.clone(),
                    event_value: event_value.clone(),
                },
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

    #[test(tokio::test)]
    async fn can_run_pending() {
        let storage_path = env::temp_dir().join("can_run_pending/offsets");
        let _ = fs::remove_dir_all(&storage_path);
        let _ = fs::create_dir_all(&storage_path);
        let storage_path = storage_path.join("offsets");
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
                MyHandlerPending {
                    entity_id: entity_id.clone(),
                    event_value: event_value.clone(),
                },
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
