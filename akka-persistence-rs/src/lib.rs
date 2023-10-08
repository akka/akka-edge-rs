#![doc = include_str!("../README.md")]

use std::{
    fmt::{self, Display, Write},
    num::Wrapping,
    ops::Range,
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

pub mod effect;
pub mod entity;
pub mod entity_manager;

/// Uniquely identifies the type of an Entity.
pub type EntityType = SmolStr;

/// Uniquely identifies an entity, or entity instance.
pub type EntityId = SmolStr;

/// Tags annotate an entity's events
pub type Tag = SmolStr;

/// Implemented by structures that can return a persistence id.
pub trait WithPersistenceId {
    fn persistence_id(&self) -> PersistenceId;
}

/// Implemented by structures that can return tags.
pub trait WithTags {
    fn tags(&self) -> Vec<Tag>;
}

/// A slice is deterministically defined based on the persistence id.
/// `NUMBER_OF_SLICES` is not configurable because changing the value would result in
/// different slice for a persistence id than what was used before, which would
/// result in invalid events_by_slices call on a source provider.
pub const NUMBER_OF_SLICES: u32 = 1024;

/// Split the total number of slices into ranges by the given `number_of_ranges`.
/// For example, `NUMBER_OF_SLICES` is 1024 and given 4 `number_of_ranges` this method will
/// return ranges (0 to 255), (256 to 511), (512 to 767) and (768 to 1023).
pub fn slice_ranges(number_of_ranges: u32) -> Vec<Range<u32>> {
    let range_size = NUMBER_OF_SLICES / number_of_ranges;
    assert!(
        number_of_ranges * range_size == NUMBER_OF_SLICES,
        "number_of_ranges must be a whole number divisor of numberOfSlices."
    );
    let mut ranges = Vec::with_capacity(number_of_ranges as usize);
    for i in 0..number_of_ranges {
        ranges.push(i * range_size..i * range_size + range_size)
    }
    ranges
}

// Implementation of the JDK8 string hashcode:
// https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#hashCode
fn jdk_string_hashcode(s: &str) -> i32 {
    let mut hash = Wrapping(0i32);
    const MULTIPLIER: Wrapping<i32> = Wrapping(31);
    let count = s.len();
    if count > 0 {
        let mut chars = s.chars();
        for _ in 0..count {
            hash = hash * MULTIPLIER + Wrapping(chars.next().unwrap() as i32);
        }
    }
    hash.0
}

/// A namespaced entity id given an entity type.
#[derive(Clone, Debug, Deserialize, PartialOrd, Ord, Serialize, PartialEq, Eq, Hash)]
pub struct PersistenceId {
    pub entity_type: EntityType,
    pub entity_id: EntityId,
}

impl PersistenceId {
    pub fn new(entity_type: EntityType, entity_id: EntityId) -> Self {
        Self {
            entity_type,
            entity_id,
        }
    }

    pub fn slice(&self) -> u32 {
        (jdk_string_hashcode(&self.to_string()) % NUMBER_OF_SLICES as i32).unsigned_abs()
    }
}

impl Display for PersistenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.entity_type)?;
        f.write_char('|')?;
        f.write_str(&self.entity_id)
    }
}

#[derive(Debug)]
pub struct PersistenceIdParseError;

impl FromStr for PersistenceId {
    type Err = PersistenceIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let persistence_id = if let Some((entity_type, entity_id)) = s.split_once('|') {
            PersistenceId {
                entity_type: EntityType::from(entity_type),
                entity_id: EntityId::from(entity_id),
            }
        } else {
            PersistenceId {
                entity_type: EntityType::from(""),
                entity_id: EntityId::from(s),
            }
        };
        Ok(persistence_id)
    }
}

/// A message encapsulates a command that is addressed to a specific entity.
#[derive(Debug, PartialEq)]
pub struct Message<C> {
    pub entity_id: EntityId,
    pub command: C,
}

impl<C> Message<C> {
    pub fn new<EI>(entity_id: EI, command: C) -> Self
    where
        EI: Into<EntityId>,
    {
        Self {
            entity_id: entity_id.into(),
            command,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TimestampOffset {
    pub timestamp: DateTime<Utc>,
    pub seen: Vec<(PersistenceId, u64)>,
}

#[derive(Clone, Deserialize, Serialize)]
pub enum Offset {
    /// Corresponds to an ordered sequence number for the events. Note that the corresponding
    /// offset of each event is provided in an Envelope,
    /// which makes it possible to resume the stream at a later point from a given offset.
    ///
    /// The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
    /// in the returned stream. This means that you can use the offset that is returned in an `Envelope`
    /// as the `offset` parameter in a subsequent query.
    ///
    Sequence(u64),
    /// Timestamp based offset. Since there can be several events for the same timestamp it keeps
    /// track of what sequence numbers for every persistence id that have been seen at this specific timestamp.
    ///
    /// The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
    /// in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
    /// as the `offset` parameter in a subsequent query.
    Timestamp(TimestampOffset),
}

/// Implemented by structures that can return an offset.
pub trait WithOffset {
    fn offset(&self) -> Offset;
}

/// Implemented by structures that can return a timestamp offset.
pub trait WithTimestampOffset {
    fn timestamp_offset(&self) -> TimestampOffset;
}

/// Implemented by structures that can return a sequence number.
pub trait WithSeqNr {
    fn seq_nr(&self) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jdk_string_hashcode() {
        assert_eq!(jdk_string_hashcode(""), 0);
        assert_eq!(jdk_string_hashcode("howtodoinjava.com"), 1894145264);
        assert_eq!(jdk_string_hashcode("hello world"), 1794106052);
    }

    #[test]
    fn test_slice_for_persistence_id() {
        assert_eq!(
            PersistenceId::new(
                EntityType::from("some-entity-type"),
                EntityId::from("some-entity-id")
            )
            .slice(),
            451
        );
    }

    #[test]
    fn test_parse_for_persistence_id() {
        assert_eq!(
            "some-entity-type|some-entity-id"
                .parse::<PersistenceId>()
                .unwrap(),
            PersistenceId::new(
                EntityType::from("some-entity-type"),
                EntityId::from("some-entity-id")
            )
        );
    }

    #[test]
    fn test_slice_ranges() {
        assert_eq!(slice_ranges(4), vec![0..256, 256..512, 512..768, 768..1024]);
    }
}
