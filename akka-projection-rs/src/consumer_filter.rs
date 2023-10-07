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

use akka_persistence_rs::{EntityId, PersistenceId, Tag, WithPersistenceId, WithTags};
use mqtt::{TopicFilter, TopicNameRef};
use regex::Regex;

#[derive(Clone)]
pub struct PersistenceIdIdOffset {
    pub persistence_id: PersistenceId,
    // If this is defined (> 0) events are replayed from the given
    // sequence number (inclusive).
    pub seq_nr: u64,
}

/// Exclude criteria are evaluated first.
/// If no matching exclude criteria the event is emitted.
/// If an exclude criteria is matching the include criteria are evaluated.
///   If no matching include criteria the event is discarded.
///   If matching include criteria the event is emitted.
#[derive(Clone)]
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
    ExcludeRegexEntityIds { matching: Vec<Regex> },
    /// Remove a previously added `ExcludeRegexEntityIds`.
    RemoveExcludeRegexEntityIds { matching: Vec<Regex> },
    /// Include events for entities with entity ids matching the given regular expressions.
    /// A matching include overrides a matching exclude.
    IncludeRegexEntityIds { matching: Vec<Regex> },
    /// Remove a previously added `IncludeRegexEntityIds`.
    RemoveIncludeRegexEntityIds { matching: Vec<Regex> },
    /// Exclude events for entities with the given persistence ids,
    /// unless there is a matching include filter that overrides the exclude.
    ExcludePersistenceIds { persistence_ids: Vec<PersistenceId> },
    /// Remove a previously added `ExcludePersistenceIds`.
    RemoveExcludePersistenceIds { persistence_ids: Vec<PersistenceId> },
    /// Include events for entities with the given persistence ids. A matching include overrides
    /// a matching exclude.
    ///
    /// For the given entity ids a `seq_nr` can be defined to replay all events for the entity
    /// from the sequence number (inclusive). If `seq_nr` is 0 events will not be replayed.
    IncludePersistenceIds {
        persistence_id_offsets: Vec<PersistenceIdIdOffset>,
    },
    /// Remove a previously added `IncludePersistenceIds`.
    RemoveIncludePersistenceIds { persistence_ids: Vec<PersistenceId> },
    /// Include events with any of the given matching topics. A matching include overrides
    /// a matching exclude.
    IncludeTopics { expressions: Vec<TopicFilter> },
    /// Remove a previously added `IncludeTopics`.
    RemoveIncludeTopics { expressions: Vec<TopicFilter> },
}

/// Exclude events from all entity ids, convenience for combining with for example a topic filter
/// to include only events matching the topic filter.
pub fn exclude_all() -> FilterCriteria {
    FilterCriteria::ExcludeRegexEntityIds {
        matching: vec![Regex::new(".*").unwrap()],
    }
}

/// A collection of criteria
#[derive(Default)]
pub struct Filter {
    topic_tag_prefix: Tag,

    exclude_tags: Vec<Tag>,
    remove_exclude_tags: Vec<Tag>,
    include_tags: Vec<Tag>,
    remove_include_tags: Vec<Tag>,
    exclude_regex_entity_ids: Vec<Regex>,
    remove_exclude_regex_entity_ids: Vec<Regex>,
    include_regex_entity_ids: Vec<Regex>,
    remove_include_regex_entity_ids: Vec<Regex>,
    exclude_persistence_ids: Vec<PersistenceId>,
    remove_exclude_persistence_ids: Vec<PersistenceId>,
    include_persistence_ids: Vec<PersistenceIdIdOffset>,
    remove_include_persistence_ids: Vec<PersistenceId>,
    include_topics: Vec<TopicFilter>,
    remove_include_topics: Vec<TopicFilter>,
}

impl Filter {
    /// A function that matches an envelope with criteria and passes it through if matched.
    pub fn matches<E>(&self, envelope: &E) -> bool
    where
        E: WithPersistenceId + WithTags,
    {
        let persistence_id = envelope.persistence_id();
        let entity_id = persistence_id.entity_id.clone();
        let tags = envelope.tags();

        if self.matches_exclude_tags(&tags) || self.matches_exclude_persistence_ids(&persistence_id)
        {
            true
        } else {
            true
        }

        //   if (env.tags.intersect(excludeTags).nonEmpty ||
        //       excludePersistenceIds.contains(pid) ||
        //       matchesExcludeRegexEntityIds) {
        //     env.tags.intersect(includeTags).nonEmpty ||
        //     matchesTopics ||
        //     includePersistenceIds.contains(pid) ||
        //     matchesIncludeRegexEntityIds
        //   } else {
        //     true
        //   }
    }

    fn matches_exclude_regex_entity_ids(&self, entity_id: &EntityId) -> bool {
        Self::matches_regex_entity_ids(&self.exclude_regex_entity_ids, entity_id)
    }

    fn matches_include_regex_entity_ids(&self, entity_id: &EntityId) -> bool {
        Self::matches_regex_entity_ids(&self.include_regex_entity_ids, entity_id)
    }

    fn matches_exclude_persistence_ids(&self, persistence_id: &PersistenceId) -> bool {
        Self::matches_persistence_ids(&self.exclude_persistence_ids, persistence_id)
    }

    // fn matches_include_persistence_ids(&self, persistence_id: &PersistenceId) -> bool {
    //     Self::matches_persistence_ids(&self.include_persistence_ids, persistence_id)
    // }

    fn matches_exclude_tags(&self, tags: &[Tag]) -> bool {
        Self::matches_tags(&self.exclude_tags, tags)
    }

    fn matches_include_tags(&self, tags: &[Tag]) -> bool {
        Self::matches_tags(&self.include_tags, tags)
    }

    fn matches_regex_entity_ids(matching: &[Regex], entity_id: &EntityId) -> bool {
        matching.iter().any(|r| r.is_match(&entity_id))
    }

    fn matches_persistence_ids(
        persistence_ids: &[PersistenceId],
        persistence_id: &PersistenceId,
    ) -> bool {
        persistence_ids.iter().any(|pi| pi == persistence_id)
    }

    fn matches_tags(match_tags: &[Tag], tags: &[Tag]) -> bool {
        match_tags.iter().any(|mt| tags.iter().any(|t| t == mt))
    }
    fn matches_topics(
        expressions: &[TopicFilter],
        topic_tag_prefix: &Tag,
        tags: &Vec<Tag>,
    ) -> bool {
        let topic_tag_prefix_len = topic_tag_prefix.len();
        expressions.iter().any(|r| {
            let matcher = r.get_matcher();
            tags.iter()
                .filter(|t| t.starts_with(topic_tag_prefix.as_str()))
                .any(|t| {
                    let topic_name = TopicNameRef::new(&t[topic_tag_prefix_len..]);
                    if let Ok(topic_name) = topic_name {
                        matcher.is_match(topic_name)
                    } else {
                        false
                    }
                })
        })
    }
}

impl From<(Tag, Vec<FilterCriteria>)> for Filter {
    fn from(value: (Tag, Vec<FilterCriteria>)) -> Self {
        let (topic_tag_prefix, value) = value;

        value.into_iter().fold(Filter {topic_tag_prefix, ..Filter::default()}, |mut f, fc| {
            #[rustfmt::skip]
            match fc {
                FilterCriteria::ExcludeTags { mut tags } => f.exclude_tags.append(&mut tags),
                FilterCriteria::RemoveExcludeTags { mut tags } => f.remove_exclude_tags.append(&mut tags),
                FilterCriteria::IncludeTags { mut tags } => f.include_tags.append(&mut tags),
                FilterCriteria::RemoveIncludeTags { mut tags } => f.remove_include_tags.append(&mut tags),
                FilterCriteria::ExcludeRegexEntityIds { mut matching } => f.exclude_regex_entity_ids.append(&mut matching),
                FilterCriteria::RemoveExcludeRegexEntityIds { mut matching } => f.remove_exclude_regex_entity_ids.append(&mut matching),
                FilterCriteria::IncludeRegexEntityIds { mut matching } => f.include_regex_entity_ids.append(&mut matching),
                FilterCriteria::RemoveIncludeRegexEntityIds { mut matching } => f.remove_include_regex_entity_ids.append(&mut matching),
                FilterCriteria::ExcludePersistenceIds { mut persistence_ids } => f.exclude_persistence_ids.append(&mut persistence_ids),
                FilterCriteria::RemoveExcludePersistenceIds { mut persistence_ids } => f.remove_exclude_persistence_ids.append(&mut persistence_ids),
                FilterCriteria::IncludePersistenceIds { mut persistence_id_offsets } => f.include_persistence_ids.append(&mut persistence_id_offsets),
                FilterCriteria::RemoveIncludePersistenceIds { mut persistence_ids } => f.remove_include_persistence_ids.append(&mut persistence_ids),
                FilterCriteria::IncludeTopics { mut expressions } => f.include_topics.append(&mut expressions),
                FilterCriteria::RemoveIncludeTopics { mut expressions } => f.remove_include_topics.append(&mut expressions),
            };
            f
        })
    }
}
