# Akka Persistence adapter for Streambed Filelog

[Streambed](https://github.com/streambed/streambed-rs) provides an implementation of a commit log named ["Logged"](https://github.com/streambed/streambed-rs/tree/main/streambed-logged). 
Logged is a library focused on conserving storage and is particularly suited for use at the edge that uses flash based
storage.

This crate adapts Logged for the purposes of it being used with Akka Persistence of 
akka-edge-rs.