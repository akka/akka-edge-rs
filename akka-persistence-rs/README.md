
# Akka Persistence

Akka Persistence enables stateful entities to persist their state so that it can be recovered when an entity is either restarted,
such as after a process crash, by a supervisor or a manual stop-start, and so forth. The key concept behind Akka Persistence is
that only the events that are persisted by the entity are stored, not the actual state of it. The events are persisted by appending
to storage (nothing is ever mutated) which allows for very high transaction rates and efficient replication. A stateful entity is
recovered by replaying the stored events to it, allowing it to rebuild its state.

An `EntityManager` is used to manage entities of a specific type. It is effectively an in-memory map of entity identifiers to their 
instances. When used at the edge, this map tends to contain all entity instances and so it is reasonble to posit why not just use
a map? The answer to this is that the `EntityManager` will reduce the amount code required to manage that map, and facilitate
dispatching commands along with sourcing their initial state from having been persisted. Events produced by entities can also be 
consumed as a stream and then persisted.