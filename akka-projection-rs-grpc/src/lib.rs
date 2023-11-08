#![doc = include_str!("../README.md")]

use akka_persistence_rs::{
    EntityId, Offset, PersistenceId, Source, Tag, TimestampOffset, WithOffset, WithPersistenceId,
    WithSeqNr, WithSource, WithTimestamp,
};
use akka_projection_rs::consumer_filter::{
    ComparableRegex, EntityIdOffset, FilterCriteria, TopicMatcher,
};
use bytes::Bytes;
use chrono::TimeZone;
use chrono::{DateTime, NaiveDateTime, Utc};
use prost::Message;
use regex::Regex;
use smol_str::SmolStr;

pub mod consumer;
mod delayer;
pub mod producer;

/// An envelope type that wraps a gRPC event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub persistence_id: PersistenceId,
    pub timestamp: DateTime<Utc>,
    pub seq_nr: u64,
    source: Source,
    pub event: E,
}

#[derive(Clone, Debug, PartialEq)]
struct SourceOnlyEventEnvelope {
    pub persistence_id: PersistenceId,
    pub timestamp: DateTime<Utc>,
    pub seq_nr: u64,
    source: Source,
}

#[derive(Clone, Debug, PartialEq)]
struct FilteredEventEnvelope {
    pub persistence_id: PersistenceId,
    pub timestamp: DateTime<Utc>,
    pub seq_nr: u64,
    source: Source,
}

/// An envelope for all types of gRPC events.
#[derive(Clone, Debug, PartialEq)]
pub struct Envelope<E>(Envelopes<E>);

#[derive(Clone, Debug, PartialEq)]
enum Envelopes<E> {
    Event(EventEnvelope<E>),
    Filtered(FilteredEventEnvelope),
    SourceOnly(SourceOnlyEventEnvelope),
}

impl<E> WithPersistenceId for Envelope<E> {
    fn persistence_id(&self) -> &PersistenceId {
        match &self.0 {
            Envelopes::Event(envelope) => &envelope.persistence_id,
            Envelopes::Filtered(envelope) => &envelope.persistence_id,
            Envelopes::SourceOnly(envelope) => &envelope.persistence_id,
        }
    }
}

impl<E> WithOffset for Envelope<E> {
    fn offset(&self) -> Offset {
        let (timestamp, seq_nr) = match &self.0 {
            Envelopes::Event(envelope) => (envelope.timestamp, envelope.seq_nr),
            Envelopes::Filtered(envelope) => (envelope.timestamp, envelope.seq_nr),
            Envelopes::SourceOnly(envelope) => (envelope.timestamp, envelope.seq_nr),
        };

        Offset::Timestamp(TimestampOffset { timestamp, seq_nr })
    }
}

impl<E> WithSeqNr for Envelope<E> {
    fn seq_nr(&self) -> u64 {
        match &self.0 {
            Envelopes::Event(envelope) => envelope.seq_nr,
            Envelopes::Filtered(envelope) => envelope.seq_nr,
            Envelopes::SourceOnly(envelope) => envelope.seq_nr,
        }
    }
}

impl<E> WithSource for Envelope<E> {
    fn source(&self) -> Source {
        match &self.0 {
            Envelopes::Event(envelope) => envelope.source,
            Envelopes::Filtered(envelope) => envelope.source,
            Envelopes::SourceOnly(envelope) => envelope.source,
        }
    }
}

impl<E> WithTimestamp for Envelope<E> {
    fn timestamp(&self) -> &DateTime<Utc> {
        match &self.0 {
            Envelopes::Event(envelope) => &envelope.timestamp,
            Envelopes::Filtered(envelope) => &envelope.timestamp,
            Envelopes::SourceOnly(envelope) => &envelope.timestamp,
        }
    }
}

/// Returned when the envelope cannot be represented as an event envelope.
pub struct NotAnEventEnvelope;

impl<E> TryFrom<Envelope<E>> for EventEnvelope<E> {
    type Error = NotAnEventEnvelope;

    fn try_from(value: Envelope<E>) -> Result<Self, Self::Error> {
        match value.0 {
            Envelopes::Event(envelope) => Ok(envelope),
            Envelopes::Filtered(_) | Envelopes::SourceOnly(_) => Err(NotAnEventEnvelope),
        }
    }
}

/// Identifies an event producer to a consumer
pub type OriginId = SmolStr;

/// The logical stream identifier, mapped to a specific internal entity type by
/// the producer settings
pub type StreamId = SmolStr;

pub mod proto {
    tonic::include_proto!("akka.projection.grpc");
}

impl From<FilterCriteria> for proto::FilterCriteria {
    fn from(value: FilterCriteria) -> Self {
        let message = match value {
            FilterCriteria::ExcludeTags { tags } => {
                proto::filter_criteria::Message::ExcludeTags(proto::ExcludeTags {
                    tags: tags.into_iter().map(|v| v.to_string()).collect(),
                })
            }
            FilterCriteria::RemoveExcludeTags { tags } => {
                proto::filter_criteria::Message::RemoveExcludeTags(proto::RemoveExcludeTags {
                    tags: tags.into_iter().map(|v| v.to_string()).collect(),
                })
            }
            FilterCriteria::IncludeTags { tags } => {
                proto::filter_criteria::Message::IncludeTags(proto::IncludeTags {
                    tags: tags.into_iter().map(|v| v.to_string()).collect(),
                })
            }
            FilterCriteria::RemoveIncludeTags { tags } => {
                proto::filter_criteria::Message::RemoveIncludeTags(proto::RemoveIncludeTags {
                    tags: tags.into_iter().map(|v| v.to_string()).collect(),
                })
            }
            FilterCriteria::ExcludeRegexEntityIds { matching } => {
                proto::filter_criteria::Message::ExcludeMatchingEntityIds(
                    proto::ExcludeRegexEntityIds {
                        matching: matching.into_iter().map(|v| v.0.to_string()).collect(),
                    },
                )
            }
            FilterCriteria::RemoveExcludeRegexEntityIds { matching } => {
                proto::filter_criteria::Message::RemoveExcludeMatchingEntityIds(
                    proto::RemoveExcludeRegexEntityIds {
                        matching: matching.into_iter().map(|v| v.0.to_string()).collect(),
                    },
                )
            }
            FilterCriteria::IncludeRegexEntityIds { matching } => {
                proto::filter_criteria::Message::IncludeMatchingEntityIds(
                    proto::IncludeRegexEntityIds {
                        matching: matching.into_iter().map(|v| v.0.to_string()).collect(),
                    },
                )
            }
            FilterCriteria::RemoveIncludeRegexEntityIds { matching } => {
                proto::filter_criteria::Message::RemoveIncludeMatchingEntityIds(
                    proto::RemoveIncludeRegexEntityIds {
                        matching: matching.into_iter().map(|v| v.0.to_string()).collect(),
                    },
                )
            }
            FilterCriteria::ExcludeEntityIds { entity_ids } => {
                proto::filter_criteria::Message::ExcludeEntityIds(proto::ExcludeEntityIds {
                    entity_ids: entity_ids.into_iter().map(|v| v.to_string()).collect(),
                })
            }
            FilterCriteria::RemoveExcludeEntityIds { entity_ids } => {
                proto::filter_criteria::Message::RemoveExcludeEntityIds(
                    proto::RemoveExcludeEntityIds {
                        entity_ids: entity_ids.into_iter().map(|v| v.to_string()).collect(),
                    },
                )
            }
            FilterCriteria::IncludeEntityIds { entity_id_offsets } => {
                proto::filter_criteria::Message::IncludeEntityIds(proto::IncludeEntityIds {
                    entity_id_offset: entity_id_offsets
                        .into_iter()
                        .map(
                            |EntityIdOffset { entity_id, seq_nr }| proto::EntityIdOffset {
                                entity_id: entity_id.to_string(),
                                seq_nr: seq_nr as i64,
                            },
                        )
                        .collect(),
                })
            }
            FilterCriteria::RemoveIncludeEntityIds { entity_ids } => {
                proto::filter_criteria::Message::RemoveIncludeEntityIds(
                    proto::RemoveIncludeEntityIds {
                        entity_ids: entity_ids.into_iter().map(|v| v.to_string()).collect(),
                    },
                )
            }
            FilterCriteria::IncludeTopics { expressions } => {
                proto::filter_criteria::Message::IncludeTopics(proto::IncludeTopics {
                    expression: expressions.into_iter().map(|v| v.to_string()).collect(),
                })
            }
            FilterCriteria::RemoveIncludeTopics { expressions } => {
                proto::filter_criteria::Message::RemoveIncludeTopics(proto::RemoveIncludeTopics {
                    expression: expressions.into_iter().map(|v| v.to_string()).collect(),
                })
            }
        };
        proto::FilterCriteria {
            message: Some(message),
        }
    }
}

/// Declares that a protobuf criteria is unable to be converted
/// due to there being no message.
pub struct NoMessage;

/// Attempt to convert from a protobuf filter criteria to a model
/// representation given an entity type.
impl TryFrom<proto::FilterCriteria> for FilterCriteria {
    type Error = NoMessage;

    fn try_from(value: proto::FilterCriteria) -> Result<Self, Self::Error> {
        match value.message {
            Some(message) => {
                let criteria = match message {
                    proto::filter_criteria::Message::ExcludeTags(proto::ExcludeTags { tags }) => {
                        FilterCriteria::ExcludeTags {
                            tags: tags.into_iter().map(Tag::from).collect(),
                        }
                    }
                    proto::filter_criteria::Message::RemoveExcludeTags(
                        proto::RemoveExcludeTags { tags },
                    ) => FilterCriteria::RemoveExcludeTags {
                        tags: tags.into_iter().map(Tag::from).collect(),
                    },
                    proto::filter_criteria::Message::IncludeTags(proto::IncludeTags { tags }) => {
                        FilterCriteria::IncludeTags {
                            tags: tags.into_iter().map(Tag::from).collect(),
                        }
                    }
                    proto::filter_criteria::Message::RemoveIncludeTags(
                        proto::RemoveIncludeTags { tags },
                    ) => FilterCriteria::RemoveIncludeTags {
                        tags: tags.into_iter().map(Tag::from).collect(),
                    },
                    proto::filter_criteria::Message::ExcludeMatchingEntityIds(
                        proto::ExcludeRegexEntityIds { matching },
                    ) => FilterCriteria::ExcludeRegexEntityIds {
                        matching: matching
                            .into_iter()
                            .flat_map(|m| Regex::new(&m).ok().map(ComparableRegex))
                            .collect(),
                    },
                    proto::filter_criteria::Message::RemoveExcludeMatchingEntityIds(
                        proto::RemoveExcludeRegexEntityIds { matching },
                    ) => FilterCriteria::RemoveExcludeRegexEntityIds {
                        matching: matching
                            .into_iter()
                            .flat_map(|m| Regex::new(&m).ok().map(ComparableRegex))
                            .collect(),
                    },
                    proto::filter_criteria::Message::IncludeMatchingEntityIds(
                        proto::IncludeRegexEntityIds { matching },
                    ) => FilterCriteria::IncludeRegexEntityIds {
                        matching: matching
                            .into_iter()
                            .flat_map(|m| Regex::new(&m).ok().map(ComparableRegex))
                            .collect(),
                    },
                    proto::filter_criteria::Message::RemoveIncludeMatchingEntityIds(
                        proto::RemoveIncludeRegexEntityIds { matching },
                    ) => FilterCriteria::RemoveIncludeRegexEntityIds {
                        matching: matching
                            .into_iter()
                            .flat_map(|m| Regex::new(&m).ok().map(ComparableRegex))
                            .collect(),
                    },
                    proto::filter_criteria::Message::ExcludeEntityIds(
                        proto::ExcludeEntityIds { entity_ids },
                    ) => FilterCriteria::ExcludeEntityIds {
                        entity_ids: entity_ids.into_iter().map(EntityId::from).collect(),
                    },
                    proto::filter_criteria::Message::RemoveExcludeEntityIds(
                        proto::RemoveExcludeEntityIds { entity_ids },
                    ) => FilterCriteria::RemoveExcludeEntityIds {
                        entity_ids: entity_ids.into_iter().map(EntityId::from).collect(),
                    },
                    proto::filter_criteria::Message::IncludeEntityIds(
                        proto::IncludeEntityIds { entity_id_offset },
                    ) => FilterCriteria::IncludeEntityIds {
                        entity_id_offsets: entity_id_offset
                            .into_iter()
                            .map(
                                |proto::EntityIdOffset { entity_id, seq_nr }| EntityIdOffset {
                                    entity_id: EntityId::from(entity_id),
                                    seq_nr: seq_nr as u64,
                                },
                            )
                            .collect(),
                    },
                    proto::filter_criteria::Message::RemoveIncludeEntityIds(
                        proto::RemoveIncludeEntityIds { entity_ids },
                    ) => FilterCriteria::RemoveIncludeEntityIds {
                        entity_ids: entity_ids.into_iter().map(EntityId::from).collect(),
                    },
                    proto::filter_criteria::Message::IncludeTopics(proto::IncludeTopics {
                        expression,
                    }) => FilterCriteria::IncludeTopics {
                        expressions: expression
                            .into_iter()
                            .flat_map(|e| TopicMatcher::new(e).ok())
                            .collect(),
                    },
                    proto::filter_criteria::Message::RemoveIncludeTopics(
                        proto::RemoveIncludeTopics { expression },
                    ) => FilterCriteria::RemoveIncludeTopics {
                        expressions: expression
                            .into_iter()
                            .flat_map(|e| TopicMatcher::new(e).ok())
                            .collect(),
                    },
                };
                Ok(criteria)
            }
            None => Err(NoMessage),
        }
    }
}

/// Declares a gRPC event cannot be mapped to an event envelope.
pub struct BadEvent;

impl<E> TryFrom<proto::Event> for Envelope<E>
where
    E: Default + Message,
{
    type Error = BadEvent;

    fn try_from(proto_event: proto::Event) -> Result<Self, Self::Error> {
        let persistence_id = proto_event
            .persistence_id
            .parse::<PersistenceId>()
            .map_err(|_| BadEvent)?;

        let event = if let Some(payload) = proto_event.payload {
            if !payload.type_url.starts_with("type.googleapis.com/") {
                return Err(BadEvent);
            }
            Some(E::decode(Bytes::from(payload.value)).map_err(|_| BadEvent)?)
        } else {
            None
        };

        let Some(offset) = proto_event.offset else {
            return Err(BadEvent);
        };

        let Some(timestamp) = offset.timestamp else {
            return Err(BadEvent);
        };

        let Some(timestamp) =
            NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
        else {
            return Err(BadEvent);
        };
        let timestamp = Utc.from_utc_datetime(&timestamp);

        let seq_nr = proto_event.seq_nr as u64;

        let source = proto_event.source.parse::<Source>().map_err(|_| BadEvent)?;

        let envelope = if let Some(event) = event {
            Envelopes::Event(EventEnvelope {
                persistence_id,
                timestamp,
                seq_nr,
                source,
                event,
            })
        } else {
            Envelopes::SourceOnly(SourceOnlyEventEnvelope {
                persistence_id,
                timestamp,
                seq_nr,
                source,
            })
        };

        Ok(Envelope(envelope))
    }
}

impl<E> TryFrom<proto::FilteredEvent> for Envelope<E>
where
    E: Default + Message,
{
    type Error = BadEvent;

    fn try_from(proto_event: proto::FilteredEvent) -> Result<Self, Self::Error> {
        let persistence_id = proto_event
            .persistence_id
            .parse::<PersistenceId>()
            .map_err(|_| BadEvent)?;

        let Some(offset) = proto_event.offset else {
            return Err(BadEvent);
        };

        let Some(timestamp) = offset.timestamp else {
            return Err(BadEvent);
        };

        let Some(timestamp) =
            NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
        else {
            return Err(BadEvent);
        };
        let timestamp = Utc.from_utc_datetime(&timestamp);

        let seq_nr = proto_event.seq_nr as u64;

        let source = proto_event.source.parse::<Source>().map_err(|_| BadEvent)?;

        Ok(Envelope(Envelopes::Filtered(FilteredEventEnvelope {
            persistence_id,
            timestamp,
            seq_nr,
            source,
        })))
    }
}
