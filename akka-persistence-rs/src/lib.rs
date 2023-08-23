#![doc = include_str!("../README.md")]

use std::{
    fmt::{self, Display, Write},
    num::Wrapping,
    ops::Range,
};

pub mod effect;
pub mod entity;
pub mod entity_manager;

/// Uniquely identifies the type of an Entity.
pub type EntityType = smol_str::SmolStr;

/// Uniquely identifies an entity, or entity instance.
pub type EntityId = smol_str::SmolStr;

/// A namespaced entity id given an entity type.
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
}

impl Display for PersistenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.entity_type)?;
        f.write_char('|')?;
        f.write_str(&self.entity_id)
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

/// A slice is deterministically defined based on the persistence id.
/// `NUMBER_OF_SLICES` is not configurable because changing the value would result in
/// different slice for a persistence id than what was used before, which would
/// result in invalid events_by_slices call on a source provider.
pub const NUMBER_OF_SLICES: u32 = 1024;

/// A slice is deterministically defined based on the persistence id. The purpose is to
/// evenly distribute all persistence ids over the slices and be able to query the
/// events for a range of slices.
pub fn slice_for_persistence_id(persistence_id: &PersistenceId) -> u32 {
    (jdk_string_hashcode(&persistence_id.to_string()) % NUMBER_OF_SLICES as i32).unsigned_abs()
}

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

#[cfg(test)]
mod tests {
    use smol_str::SmolStr;

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
            slice_for_persistence_id(&PersistenceId::new(
                SmolStr::from("some-entity-type"),
                SmolStr::from("some-entity-id")
            )),
            451
        );
    }

    #[test]
    fn test_slice_ranges() {
        assert_eq!(slice_ranges(4), vec![0..256, 256..512, 512..768, 768..1024]);
    }
}
