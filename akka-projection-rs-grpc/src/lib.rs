#![doc = include_str!("../README.md")]

use akka_persistence_rs::{EntityId, Offset, PersistenceId, Tag, TimestampOffset, WithOffset};
use akka_projection_rs::consumer_filter::{
    ComparableRegex, EntityIdOffset, FilterCriteria, TopicMatcher,
};
use regex::Regex;
use smol_str::SmolStr;

pub mod consumer;
mod delayer;
pub mod producer;

/// An envelope wraps a gRPC event associated with a specific entity.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEnvelope<E> {
    pub persistence_id: PersistenceId,
    pub seq_nr: u64,
    pub event: Option<E>,
    pub offset: TimestampOffset,
}

impl<E> WithOffset for EventEnvelope<E> {
    fn offset(&self) -> Offset {
        Offset::Timestamp(self.offset.clone())
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
