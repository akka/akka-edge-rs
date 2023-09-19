# Akka Persistence adapter for Streambed commit logs

This crate adapts Streambed's CommitLog and SecretStore traits for the purposes of it being used with Akka Persistence of akka-edge-rs.

> A `u64` "key" field is used with Streambed's abstraction of commit logs as modelled off [Kafka](https://kafka.apache.org/). Keys are
used to distinguish and entity and its record type. Therefore, you must have an entity id that can be represented directly in 64 bits,
along with some bits reserved for the record type. Devices at the edge are often represented with a 32 bit address, and so this should
not present a problem at the edge in general. Hashes are to be avoided due to their potential for collisions.

[Streambed](https://github.com/streambed/streambed-rs) provides an implementation of a commit log named ["Logged"](https://github.com/streambed/streambed-rs/tree/main/streambed-logged). 
Logged is a library focused on conserving storage and is particularly suited for use at the edge that uses flash based
storage. Other forms of commit log are also supported by Streabmed, including a [Kafka](https://kafka.apache.org/)-like HTTP interface.

The encryption/decryption of records stored in a commit log are also handled through Streambed's SecretStore. Streambed also provides an implementation
of a file-based secret store named ["Confidant"](https://github.com/streambed/streambed-rs/tree/main/streambed-confidant). Confidant is also particularly
suited for use at the edge where flash storage is also used. Other forms of the secret store are supported, including [Hashicorp's Vault](https://www.vaultproject.io/).
