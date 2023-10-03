//! The consumer may define declarative filters that are sent to the producer and evaluated on the producer side
//! before emitting the events.
//!
//! Consumer filters consists of exclude and include criteria. In short, the exclude criteria are evaluated first
//! and may be overridden by an include criteria. More precisely, they are evaluated according to the following rules:
//!
//! * Exclude criteria are evaluated first.
//! * If no matching exclude criteria the event is emitted.
//! * If an exclude criteria is matching the include criteria are evaluated.
//! * If no matching include criteria the event is discarded.
//! * If matching include criteria the event is emitted.
//!
//! The exclude criteria can be a combination of:
//!
//! * ExcludeTags - exclude events with any of the given tags
//! * ExcludeRegexEntityIds - exclude events for entities with entity ids matching the given regular expressions
//! * ExcludeEntityIds - exclude events for entities with the given entity ids
//!
//! To exclude all events you can use ExcludeRegexEntityIds with .*.
//!
//! The include criteria can be a combination of:
//!
//! * IncludeTopics - include events with any of the given matching topics
//! * IncludeTags - include events with any of the given tags
//! * IncludeRegexEntityIds - include events for entities with entity ids matching the given regular expressions
//! * IncludeEntityIds - include events for entities with the given entity ids

use akka_persistence_rs::EntityId;
use smol_str::SmolStr;

pub struct EntityIdOffset {
    pub entity_id: EntityId,
    // If this is defined (> 0) events are replayed from the given
    // sequence number (inclusive).
    pub seq_nr: u64,
}

/// A regex expression for matching entity ids.
pub type EntityIdMatcher = SmolStr;

pub type Tag = SmolStr;

/// A topic match expression according to MQTT specification, including wildcards
pub type TopicMatcher = SmolStr;

/// Exclude criteria are evaluated first.
/// If no matching exclude criteria the event is emitted.
/// If an exclude criteria is matching the include criteria are evaluated.
///   If no matching include criteria the event is discarded.
///   If matching include criteria the event is emitted.
pub enum FilterCriteria {
    /// Exclude events with any of the given tags, unless there is a
    /// matching include filter that overrides the exclude.
    ExcludeTags { tags: Vec<Tag> },
    /// Remove a previously added `ExcludeTags`.
    RemoveExcludeTags { tags: Vec<Tag> },
    /// Include events with any of the given tags. A matching include overrides
    /// a matching exclude.
    IncludeTags { tags: Vec<Tag> },
    /// Remove a previously added `IncludeTags`.
    RemoveIncludeTags { tags: Vec<Tag> },
    /// Exclude events for entities with entity ids matching the given regular expressions,
    /// unless there is a matching include filter that overrides the exclude.
    ExcludeRegexEntityIds { matching: Vec<EntityIdMatcher> },
    /// Remove a previously added `ExcludeRegexEntityIds`.
    RemoveExcludeRegexEntityIds { matching: Vec<EntityIdMatcher> },
    /// Include events for entities with entity ids matching the given regular expressions.
    /// A matching include overrides a matching exclude.
    IncludeRegexEntityIds { matching: Vec<EntityIdMatcher> },
    /// Remove a previously added `IncludeRegexEntityIds`.
    RemoveIncludeRegexEntityIds { matching: Vec<EntityIdMatcher> },
    /// Exclude events for entities with the given entity ids,
    /// unless there is a matching include filter that overrides the exclude.
    ExcludeEntityIds { entity_ids: Vec<EntityId> },
    /// Remove a previously added `ExcludeEntityIds`.
    RemoveExcludeEntityIds { entity_ids: Vec<EntityId> },
    /// Include events for entities with the given entity ids. A matching include overrides
    /// a matching exclude.
    ///
    /// For the given entity ids a `seq_nr` can be defined to replay all events for the entity
    /// from the sequence number (inclusive). If `seq_nr` is 0 events will not be replayed.
    IncludeEntityIds {
        entity_id_offsets: Vec<EntityIdOffset>,
    },
    /// Remove a previously added `IncludeEntityIds`.
    RemoveIncludeEntityIds { entity_ids: Vec<EntityId> },
    /// Include events with any of the given matching topics. A matching include overrides
    /// a matching exclude.
    IncludeTopics { expressions: Vec<TopicMatcher> },
    /// Remove a previously added `IncludeTopics`.
    RemoveIncludeTopics { expressions: Vec<TopicMatcher> },
}
