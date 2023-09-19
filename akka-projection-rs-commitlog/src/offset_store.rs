//! An offset store for use with the Streambed commit log.

use std::num::NonZeroUsize;

use akka_persistence_rs::{entity_manager, EntityId, Message};
use akka_persistence_rs_commitlog::{CommitLogMarshaler, CommitLogTopicAdapter, EventEnvelope};
use akka_projection_rs::offset_store;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use smol_str::SmolStr;
use streambed::commit_log::{ConsumerRecord, Header, HeaderKey, Key, ProducerRecord, Topic};
use streambed_logged::{compaction::KeyBasedRetention, FileLog};
use tokio::sync::mpsc;

struct OffsetStoreEventMarshaler<F> {
    to_compaction_key: F,
}

#[async_trait]
impl<F> CommitLogMarshaler<offset_store::Event> for OffsetStoreEventMarshaler<F>
where
    F: Fn(&EntityId, &offset_store::Event) -> Option<Key> + Send + Sync,
{
    fn to_compaction_key(&self, entity_id: &EntityId, event: &offset_store::Event) -> Option<Key> {
        (self.to_compaction_key)(entity_id, event)
    }

    fn to_entity_id(&self, record: &ConsumerRecord) -> Option<EntityId> {
        let Header { value, .. } = record
            .headers
            .iter()
            .find(|header| header.key == "entity-id")?;
        std::str::from_utf8(value).ok().map(EntityId::from)
    }

    async fn envelope(
        &self,
        entity_id: EntityId,
        record: ConsumerRecord,
    ) -> Option<EventEnvelope<offset_store::Event>> {
        let value = u64::from_be_bytes(record.value.try_into().ok()?);
        let event = offset_store::Event::Saved { seq_nr: value };
        record.timestamp.map(|timestamp| EventEnvelope {
            entity_id,
            seq_nr: 0, // We don't care about sequence numbers with the offset store as they won't be projected anywhere
            timestamp,
            event,
            offset: record.offset,
        })
    }

    async fn producer_record(
        &self,
        topic: Topic,
        entity_id: EntityId,
        _seq_nr: u64,
        timestamp: DateTime<Utc>,
        event: offset_store::Event,
    ) -> Option<ProducerRecord> {
        let offset_store::Event::Saved { seq_nr } = event;
        let headers = vec![Header {
            key: HeaderKey::from("entity-id"),
            value: entity_id.as_bytes().into(),
        }];
        let key = self.to_compaction_key(&entity_id, &event)?;
        Some(ProducerRecord {
            topic,
            headers,
            timestamp: Some(timestamp),
            key,
            value: seq_nr.to_be_bytes().to_vec(),
            partition: 0,
        })
    }
}

/// Uniquely identifies an offset store.
pub type OffsetStoreId = SmolStr;

/// Runs an offset store.
pub async fn run(
    mut commit_log: FileLog,
    keys_expected: usize,
    offset_store_id: OffsetStoreId,
    offset_store_receiver: mpsc::Receiver<Message<offset_store::Command>>,
    to_compaction_key: impl Fn(&EntityId, &offset_store::Event) -> Option<Key> + Send + Sync + 'static,
) {
    let events_topic = Topic::from(offset_store_id.clone());

    commit_log
        .register_compaction(events_topic.clone(), KeyBasedRetention::new(keys_expected))
        .await
        .unwrap();

    let file_log_topic_adapter = CommitLogTopicAdapter::new(
        commit_log,
        OffsetStoreEventMarshaler { to_compaction_key },
        &offset_store_id,
        events_topic,
    );

    entity_manager::run(
        offset_store::Behavior,
        file_log_topic_adapter,
        offset_store_receiver,
        NonZeroUsize::new(keys_expected).unwrap(),
    )
    .await
}
