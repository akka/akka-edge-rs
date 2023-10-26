#![doc = include_str!("../README.md")]

use std::{collections::VecDeque, pin::Pin};

use crate::{
    offset_store::{self},
    Handler, HandlerError, Handlers, PendingHandler, SourceProvider,
};
use akka_persistence_rs::{
    Offset, Source, TimestampOffset, WithOffset, WithPersistenceId, WithSeqNr, WithSource,
};
use futures::{self, future, stream, Future, Stream};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;

#[derive(Default, Deserialize, Serialize)]
struct StorableState {
    last_offset: Option<Offset>,
}

/// Provides at-least-once projections with storage for projection offsets,
/// meaning, for multiple runs of a projection, it is possible for events to repeat
/// from previous runs.
pub async fn run<A, B, E, IH, SP>(
    offset_store: mpsc::Sender<offset_store::Command>,
    mut kill_switch: oneshot::Receiver<()>,
    source_provider: SP,
    handler: IH,
) where
    A: Handler<Envelope = E> + Send,
    B: PendingHandler<Envelope = E> + Send,
    E: WithPersistenceId + WithOffset + WithSeqNr + WithSource + Send,
    IH: Into<Handlers<A, B>>,
    SP: SourceProvider<Envelope = E>,
{
    let mut handler = handler.into();

    let mut always_pending_handler: Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>> =
        Box::pin(future::pending());

    'outer: loop {
        let mut source = source_provider
            .source(|| async {
                let (reply_to, reply_to_receiver) = oneshot::channel();
                offset_store
                    .send(offset_store::Command::GetLastOffset { reply_to })
                    .await
                    .ok()?;
                reply_to_receiver.await.ok()?
            })
            .await;

        let mut always_pending_source: Pin<Box<dyn Stream<Item = E> + Send>> =
            Box::pin(stream::pending());

        let mut active_source = &mut source;

        let mut handler_futures = VecDeque::with_capacity(B::MAX_PENDING);

        loop {
            tokio::select! {
                envelope = active_source.next() => {
                    if let Some(envelope) = envelope {
                        let persistence_id = envelope.persistence_id().clone();

                        // Process the sequence number. If it isn't what we expect then we go round again.

                        let seq_nr = envelope.seq_nr();

                        let (reply_to, reply_to_receiver) = oneshot::channel();
                        if offset_store
                            .send(offset_store::Command::GetOffset { persistence_id: persistence_id.clone(), reply_to })
                            .await
                            .is_err()
                        {
                            warn!("Cannot send to the offset store: {}. Aborting stream.", persistence_id);
                            break;
                        }

                        let next_seq_nr = if let Ok(Some(Offset::Timestamp(TimestampOffset { seq_nr, .. }))) = reply_to_receiver.await {
                            seq_nr.wrapping_add(1)
                        } else {
                            warn!("Cannot receive from the offset store: {}. Aborting stream.", persistence_id);
                            break
                        };

                        let source = envelope.source();

                        if seq_nr > next_seq_nr && envelope.source() == Source::Backtrack {
                            // This shouldn't happen, if so then abort.
                            warn!("Back track received for a future event: {}. Aborting stream.", persistence_id);
                            break;
                        } else if seq_nr != next_seq_nr {
                            // Duplicate or gap
                            continue;
                        }

                        // If the sequence number is what we expect and the producer is backtracking, then
                        // request its payload. If we can't get its payload then we abort as it is an error.

                        let resolved_envelope = if source == Source::Backtrack {
                            if let Some(event) = source_provider.load_envelope(persistence_id.clone(), seq_nr)
                                .await
                            {
                                Some(event)
                            } else {
                                warn!("Cannot obtain an backtrack envelope: {}. Aborting stream.", persistence_id);
                                None
                            }
                        } else {
                            Some(envelope)
                        };

                        let Some(envelope) = resolved_envelope else { break; };

                        // We now have an event correctly sequenced. Process it.

                        let offset = envelope.offset();
                        match &mut handler {
                            Handlers::Ready(handler, _) => {
                                if handler.process(envelope).await.is_err()
                                    || offset_store
                                        .send(offset_store::Command::SaveOffset { persistence_id, offset })
                                        .await
                                        .is_err()
                                {
                                    break;
                                }
                                                }
                            Handlers::Pending(handler, _) => {
                                if let Ok(pending) = handler.process_pending(envelope).await {
                                    handler_futures.push_back((pending, persistence_id, offset));
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

                pending = handler_futures.get_mut(0).map_or_else(|| &mut always_pending_handler, |(f, _, _)| f) => {
                    // A pending future will never complete so this MUST mean that we have a element in our queue.
                    let (_, persistence_id, offset) = handler_futures.pop_front().unwrap();

                    // We've freed up a slot on the pending futures in-flight, so allow more events to be received.
                    active_source = &mut source;

                    // If all is well with our pending future so we can finally cause the offset to be persisted.
                    if pending.is_err()
                        || offset_store
                            .send(offset_store::Command::SaveOffset { persistence_id, offset })
                            .await
                            .is_err()
                    {
                        break;
                    }
                }

                _ = &mut kill_switch => {
                    debug!("storage killed.");
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
    use std::{env, fs, future::Future, pin::Pin};

    use super::*;
    use crate::{offset_store::LastOffset, HandlerError};
    use akka_persistence_rs::{EntityId, EntityType, PersistenceId};
    use async_stream::stream;
    use async_trait::async_trait;
    use serde::Deserialize;
    use test_log::test;
    use tokio_stream::Stream;

    // Scaffolding

    struct TestEnvelope {
        persistence_id: PersistenceId,
        seq_nr: u64,
        offset: u64,
        event: MyEvent,
    }

    impl WithPersistenceId for TestEnvelope {
        fn persistence_id(&self) -> &PersistenceId {
            &self.persistence_id
        }
    }

    impl WithOffset for TestEnvelope {
        fn offset(&self) -> Offset {
            Offset::Sequence(self.offset)
        }
    }

    impl WithSource for TestEnvelope {
        fn source(&self) -> akka_persistence_rs::Source {
            todo!()
        }
    }

    impl WithSeqNr for TestEnvelope {
        fn seq_nr(&self) -> u64 {
            self.seq_nr
        }
    }

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

    struct MySourceProvider {
        persistence_id: PersistenceId,
        event_value: String,
    }

    #[async_trait]
    impl SourceProvider for MySourceProvider {
        type Envelope = TestEnvelope;

        async fn source<F, FR>(
            &self,
            offset: F,
        ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
        where
            F: Fn() -> FR + Send + Sync,
            FR: Future<Output = Option<LastOffset>> + Send,
        {
            Box::pin(
                stream! {
                    if offset().await.is_none() {
                        yield TestEnvelope {
                            persistence_id: self.persistence_id.clone(),
                            seq_nr: 1,
                            event: MyEvent {
                                value: self.event_value.clone(),
                            },
                            offset: 0,
                        }
                    }
                }
                .chain(stream::pending()),
            )
        }
        async fn load_envelope(
            &self,
            _persistence_id: PersistenceId,
            _sequence_nr: u64,
        ) -> Option<Self::Envelope> {
            None // FIXME
        }
    }

    struct MyHandler {
        persistence_id: PersistenceId,
        event_value: String,
    }

    #[async_trait]
    impl Handler for MyHandler {
        type Envelope = TestEnvelope;

        /// Process an envelope.
        async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
            assert_eq!(envelope.persistence_id, self.persistence_id);
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
        persistence_id: PersistenceId,
        event_value: String,
    }

    #[async_trait]
    impl PendingHandler for MyHandlerPending {
        type Envelope = TestEnvelope;

        const MAX_PENDING: usize = 1;

        /// Process an envelope.
        async fn process_pending(
            &mut self,
            envelope: Self::Envelope,
        ) -> Result<Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>, HandlerError>
        {
            assert_eq!(envelope.persistence_id, self.persistence_id);
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

        let entity_type = EntityType::from("some-entity-type");
        let entity_id = EntityId::from("some-entity");
        let persistence_id = PersistenceId::new(entity_type, entity_id);
        let event_value = "some value".to_string();

        // Process an event.

        let (_registration_projection_command, registration_projection_command_receiver) =
            oneshot::channel();

        let (offset_store, _) = mpsc::channel(1); // FIXME
        tokio::spawn(async move {
            run(
                offset_store,
                registration_projection_command_receiver,
                MySourceProvider {
                    persistence_id: persistence_id.clone(),
                    event_value: event_value.clone(),
                },
                MyHandler {
                    persistence_id: persistence_id.clone(),
                    event_value: event_value.clone(),
                },
            )
            .await
        });

        // FIXME derive a test of the watch channel being set.
    }

    #[test(tokio::test)]
    async fn can_run_pending() {
        let storage_path = env::temp_dir().join("can_run_pending/offsets");
        let _ = fs::remove_dir_all(&storage_path);
        let _ = fs::create_dir_all(&storage_path);
        let storage_path = storage_path.join("offsets");
        println!("Writing to {}", storage_path.to_string_lossy());

        // Scaffolding

        let entity_type = EntityType::from("some-entity-type");
        let entity_id = EntityId::from("some-entity");
        let persistence_id = PersistenceId::new(entity_type, entity_id);
        let event_value = "some value".to_string();

        // Process an event.
        let (_registration_projection_command, registration_projection_command_receiver) =
            oneshot::channel();

        let (offset_store, _) = mpsc::channel(1); // FIXME
        tokio::spawn(async move {
            run(
                offset_store,
                registration_projection_command_receiver,
                MySourceProvider {
                    persistence_id: persistence_id.clone(),
                    event_value: event_value.clone(),
                },
                MyHandlerPending {
                    persistence_id: persistence_id.clone(),
                    event_value: event_value.clone(),
                },
            )
            .await
        });

        // FIXME derive a test of the watch channel being set.
    }
}
