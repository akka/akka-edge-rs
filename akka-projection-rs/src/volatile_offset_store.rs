//! A volatile offset store is provided for situations where
//! events are always sourced events from their earliest offset.
//! An example use case is when events are queried over HTTP from
//! a web browser that does not retain the offset where it is up to.
//!
//! All offset data for a given persistence id is retained in
//! memory.

use std::{collections::HashMap, io};

use futures::Future;
use tokio::sync::mpsc;

use crate::offset_store;

/// Provides an asynchronous task and a command channel that can run and drive an in-memory offset store.
pub fn task(
    keys_expected: usize,
) -> (
    impl Future<Output = io::Result<()>>,
    mpsc::Sender<offset_store::Command>,
) {
    let (sender, mut receiver) = mpsc::channel(1);
    let task = async move {
        let mut offsets = HashMap::with_capacity(keys_expected);
        while let Some(command) = receiver.recv().await {
            match command {
                offset_store::Command::GetLastOffset { reply_to } => {
                    let _ = reply_to.send(None);
                    offsets.clear();
                }
                offset_store::Command::GetOffset {
                    persistence_id,
                    reply_to,
                } => {
                    let _ = reply_to.send(offsets.get(&persistence_id).cloned());
                }
                offset_store::Command::SaveOffset {
                    persistence_id,
                    offset,
                } => {
                    offsets
                        .entry(persistence_id)
                        .and_modify(|v| *v = offset.clone())
                        .or_insert(offset);
                }
            }
        }
        Ok(())
    };
    (task, sender)
}

#[cfg(test)]
mod tests {
    use super::*;

    use akka_persistence_rs::{EntityId, EntityType, Offset, PersistenceId};
    use test_log::test;
    use tokio::sync::oneshot;

    #[test(tokio::test)]
    async fn test_basic_ops() {
        let (task, commands) = task(1);

        tokio::spawn(task);

        let (reply_to, reply_to_receiver) = oneshot::channel();
        assert!(commands
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .is_ok());
        assert_eq!(reply_to_receiver.await, Ok(None));

        let persistence_id =
            PersistenceId::new(EntityType::from("entity-type"), EntityId::from("entity-id"));

        let offset = Offset::Sequence(10);

        assert!(commands
            .send(offset_store::Command::SaveOffset {
                persistence_id: persistence_id.clone(),
                offset: offset.clone()
            })
            .await
            .is_ok());

        let (reply_to, reply_to_receiver) = oneshot::channel();
        assert!(commands
            .send(offset_store::Command::GetOffset {
                persistence_id: persistence_id.clone(),
                reply_to
            })
            .await
            .is_ok());
        assert_eq!(reply_to_receiver.await, Ok(Some(offset)));

        let (reply_to, reply_to_receiver) = oneshot::channel();
        assert!(commands
            .send(offset_store::Command::GetLastOffset { reply_to })
            .await
            .is_ok());
        assert_eq!(reply_to_receiver.await, Ok(None));

        let (reply_to, reply_to_receiver) = oneshot::channel();
        assert!(commands
            .send(offset_store::Command::GetOffset {
                persistence_id,
                reply_to
            })
            .await
            .is_ok());
        assert_eq!(reply_to_receiver.await, Ok(None));
    }
}
