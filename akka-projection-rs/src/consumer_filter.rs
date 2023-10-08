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

#[derive(Clone)]
pub struct ComparableRegex(pub Regex);

impl PartialEq for ComparableRegex {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for ComparableRegex {}

impl PartialOrd for ComparableRegex {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ComparableRegex {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_str().cmp(other.0.as_str())
    }
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
    ExcludeRegexEntityIds { matching: Vec<ComparableRegex> },
    /// Remove a previously added `ExcludeRegexEntityIds`.
    RemoveExcludeRegexEntityIds { matching: Vec<ComparableRegex> },
    /// Include events for entities with entity ids matching the given regular expressions.
    /// A matching include overrides a matching exclude.
    IncludeRegexEntityIds { matching: Vec<ComparableRegex> },
    /// Remove a previously added `IncludeRegexEntityIds`.
    RemoveIncludeRegexEntityIds { matching: Vec<ComparableRegex> },
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
        matching: vec![ComparableRegex(Regex::new(".*").unwrap())],
    }
}

/// A collection of criteria
pub struct Filter {
    topic_tag_prefix: Tag,
    exclude_tags: Vec<Tag>,
    include_tags: Vec<Tag>,
    exclude_regex_entity_ids: Vec<ComparableRegex>,
    include_regex_entity_ids: Vec<ComparableRegex>,
    exclude_persistence_ids: Vec<PersistenceId>,
    include_persistence_ids: Vec<PersistenceId>,
    include_topics: Vec<TopicFilter>,
}

impl Filter {
    pub fn new(topic_tag_prefix: Tag) -> Self {
        Self {
            topic_tag_prefix,
            exclude_tags: vec![],
            include_tags: vec![],
            exclude_regex_entity_ids: vec![],
            include_regex_entity_ids: vec![],
            exclude_persistence_ids: vec![],
            include_persistence_ids: vec![],
            include_topics: vec![],
        }
    }

    /// A function that matches an envelope with criteria and passes it through if matched.
    pub fn matches<E>(&self, envelope: &E) -> bool
    where
        E: WithPersistenceId + WithTags,
    {
        let tags = envelope.tags();
        let persistence_id = envelope.persistence_id();
        let entity_id = persistence_id.entity_id.clone();

        if self.matches_exclude_tags(&tags)
            || self.matches_exclude_persistence_ids(&persistence_id)
            || self.matches_exclude_regex_entity_ids(&entity_id)
        {
            self.matches_include_tags(&tags)
                || self.matches_include_topics(&tags)
                || self.matches_include_persistence_ids(&persistence_id)
                || self.matches_include_regex_entity_ids(&entity_id)
        } else {
            true
        }
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

    fn matches_include_persistence_ids(&self, persistence_id: &PersistenceId) -> bool {
        Self::matches_persistence_ids(&self.include_persistence_ids, persistence_id)
    }

    fn matches_exclude_tags(&self, tags: &[Tag]) -> bool {
        Self::matches_tags(&self.exclude_tags, tags)
    }

    fn matches_include_tags(&self, tags: &[Tag]) -> bool {
        Self::matches_tags(&self.include_tags, tags)
    }

    fn matches_include_topics(&self, tags: &[Tag]) -> bool {
        Self::matches_topics(&self.include_topics, &self.topic_tag_prefix, tags)
    }

    fn matches_regex_entity_ids(matching: &[ComparableRegex], entity_id: &EntityId) -> bool {
        matching.iter().any(|r| r.0.is_match(entity_id))
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

    fn matches_topics(expressions: &[TopicFilter], topic_tag_prefix: &Tag, tags: &[Tag]) -> bool {
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

    /// Updates the filter given commands to add or remove new criteria.
    pub fn update(&mut self, criteria: Vec<FilterCriteria>) {
        for criterion in criteria {
            match criterion {
                FilterCriteria::ExcludeTags { mut tags } => {
                    merge(&mut self.exclude_tags, &mut tags)
                }

                FilterCriteria::RemoveExcludeTags { tags } => remove(&mut self.exclude_tags, &tags),

                FilterCriteria::IncludeTags { mut tags } => {
                    merge(&mut self.include_tags, &mut tags)
                }

                FilterCriteria::RemoveIncludeTags { tags } => remove(&mut self.include_tags, &tags),

                FilterCriteria::ExcludeRegexEntityIds { mut matching } => {
                    merge(&mut self.exclude_regex_entity_ids, &mut matching)
                }

                FilterCriteria::RemoveExcludeRegexEntityIds { matching } => {
                    remove(&mut self.exclude_regex_entity_ids, &matching)
                }

                FilterCriteria::IncludeRegexEntityIds { mut matching } => {
                    merge(&mut self.include_regex_entity_ids, &mut matching)
                }

                FilterCriteria::RemoveIncludeRegexEntityIds { matching } => {
                    remove(&mut self.include_regex_entity_ids, &matching)
                }

                FilterCriteria::ExcludePersistenceIds {
                    mut persistence_ids,
                } => merge(&mut self.exclude_persistence_ids, &mut persistence_ids),

                FilterCriteria::RemoveExcludePersistenceIds { persistence_ids } => {
                    remove(&mut self.exclude_persistence_ids, &persistence_ids)
                }

                FilterCriteria::IncludePersistenceIds {
                    persistence_id_offsets,
                } => merge(
                    &mut self.include_persistence_ids,
                    &mut persistence_id_offsets
                        .into_iter()
                        .map(|PersistenceIdIdOffset { persistence_id, .. }| persistence_id)
                        .collect(),
                ),

                FilterCriteria::RemoveIncludePersistenceIds { persistence_ids } => {
                    remove(&mut self.include_persistence_ids, &persistence_ids)
                }

                FilterCriteria::IncludeTopics { mut expressions } => {
                    merge(&mut self.include_topics, &mut expressions)
                }

                FilterCriteria::RemoveIncludeTopics { expressions } => {
                    remove(&mut self.include_topics, &expressions)
                }
            };
        }
    }
}

fn merge<T>(l: &mut Vec<T>, r: &mut Vec<T>)
where
    T: Ord,
{
    l.append(r);
    l.sort();
    l.dedup();
}

fn remove<T>(l: &mut Vec<T>, r: &[T])
where
    T: PartialEq,
{
    l.retain(|existing| !r.contains(existing));
}

#[cfg(test)]
mod tests {

    use super::*;

    struct TestEnvelope {
        persistence_id: PersistenceId,
        tags: Vec<Tag>,
    }

    impl WithPersistenceId for TestEnvelope {
        fn persistence_id(&self) -> PersistenceId {
            self.persistence_id.clone()
        }
    }

    impl WithTags for TestEnvelope {
        fn tags(&self) -> Vec<Tag> {
            self.tags.clone()
        }
    }

    #[test]
    fn exclude_include_and_remove_include_tag_and_remove_exclude_tag() {
        let persistence_id = "a|1".parse::<PersistenceId>().unwrap();
        let tag = Tag::from("a");

        let envelope = TestEnvelope {
            persistence_id: persistence_id.clone(),
            tags: vec![tag.clone()],
        };

        let mut filter = Filter::new(Tag::from(""));

        let criteria = vec![
            FilterCriteria::ExcludeTags {
                tags: vec![tag.clone()],
            },
            FilterCriteria::IncludeTags {
                tags: vec![tag.clone()],
            },
        ];
        filter.update(criteria);
        assert!(filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveIncludeTags {
            tags: vec![tag.clone()],
        }];
        filter.update(criteria);
        assert!(!filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveExcludeTags { tags: vec![tag] }];
        filter.update(criteria);
        assert!(filter.matches(&envelope));
    }

    #[test]
    fn exclude_include_and_remove_include_persistence_id_and_remove_exclude_persistence_id() {
        let persistence_id = "a|1".parse::<PersistenceId>().unwrap();

        let envelope = TestEnvelope {
            persistence_id: persistence_id.clone(),
            tags: vec![],
        };

        let mut filter = Filter::new(Tag::from(""));

        let criteria = vec![
            FilterCriteria::ExcludePersistenceIds {
                persistence_ids: vec![persistence_id.clone()],
            },
            FilterCriteria::IncludePersistenceIds {
                persistence_id_offsets: vec![PersistenceIdIdOffset {
                    persistence_id: persistence_id.clone(),
                    seq_nr: 0,
                }],
            },
        ];
        filter.update(criteria);
        assert!(filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveIncludePersistenceIds {
            persistence_ids: vec![persistence_id.clone()],
        }];
        filter.update(criteria);
        assert!(!filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveExcludePersistenceIds {
            persistence_ids: vec![persistence_id.clone()],
        }];
        filter.update(criteria);
        assert!(filter.matches(&envelope));
    }

    #[test]
    fn exclude_include_and_remove_include_regex_entity_id_and_remove_exclude_regex_entity_id() {
        let persistence_id = "a|1".parse::<PersistenceId>().unwrap();
        let matching = ComparableRegex(Regex::new("1").unwrap());

        let envelope = TestEnvelope {
            persistence_id: persistence_id.clone(),
            tags: vec![],
        };

        let mut filter = Filter::new(Tag::from(""));

        let criteria = vec![
            FilterCriteria::ExcludeRegexEntityIds {
                matching: vec![matching.clone()],
            },
            FilterCriteria::IncludeRegexEntityIds {
                matching: vec![matching.clone()],
            },
        ];
        filter.update(criteria);
        assert!(filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveIncludeRegexEntityIds {
            matching: vec![matching.clone()],
        }];
        filter.update(criteria);
        assert!(!filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveExcludeRegexEntityIds {
            matching: vec![matching.clone()],
        }];
        filter.update(criteria);
        assert!(filter.matches(&envelope));
    }

    #[test]
    fn include_and_remove_include_topic() {
        let persistence_id = "a|1".parse::<PersistenceId>().unwrap();
        let tag = Tag::from("t:sport/abc/player1");
        let expression = TopicFilter::new("sport/+/player1").unwrap();

        let envelope = TestEnvelope {
            persistence_id: persistence_id.clone(),
            tags: vec![tag.clone()],
        };

        let mut filter = Filter::new(Tag::from("t:"));

        let criteria = vec![
            exclude_all(),
            FilterCriteria::IncludeTopics {
                expressions: vec![expression.clone()],
            },
        ];
        filter.update(criteria);
        assert!(filter.matches(&envelope));

        let criteria = vec![FilterCriteria::RemoveIncludeTopics {
            expressions: vec![expression.clone()],
        }];
        filter.update(criteria);
        assert!(!filter.matches(&envelope));
    }
}
