use std::{
    hash::{BuildHasher, Hasher},
    num::Wrapping,
};

/// A hasher for hashing strings conformant with
/// https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#hashCode.
/// Undefined hash values will be produced if anything but a
/// Rust [String] is hashed.
pub struct StringHasher;

impl BuildHasher for StringHasher {
    type Hasher = StringHasherImpl;

    fn build_hasher(&self) -> Self::Hasher {
        StringHasherImpl {
            hash: Wrapping(0i32),
        }
    }
}

pub struct StringHasherImpl {
    hash: Wrapping<i32>,
}

const MULTIPLIER: Wrapping<i32> = Wrapping(31);

impl Hasher for StringHasherImpl {
    fn finish(&self) -> u64 {
        self.hash.0 as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        let s = std::str::from_utf8(bytes).unwrap_or_default();
        let count = s.len();
        if count > 0 {
            let mut chars = s.chars();
            for _ in 0..count {
                self.hash = self.hash * MULTIPLIER + Wrapping(chars.next().unwrap() as i32);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_hasher() {
        let mut hasher = StringHasher.build_hasher();
        hasher.write("".as_bytes());
        assert_eq!(hasher.finish(), 0);

        let mut hasher = StringHasher.build_hasher();
        hasher.write("howtodoinjava.com".as_bytes());
        assert_eq!(hasher.finish(), 1894145264);

        let mut hasher = StringHasher.build_hasher();
        hasher.write("hello world".as_bytes());
        assert_eq!(hasher.finish(), 1794106052);
    }
}
