Akka projections
===

In Akka Projections you process a stream of event envelopes from a source to a projected model or external system.
Each envelope is associated with an offset representing the position in the stream. This offset is used for resuming
the stream from that position when the projection is restarted.
