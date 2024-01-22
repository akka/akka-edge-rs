# akka-edge-rs
Akka Edge support in Rust is designed to empower cloud developers at the edge. Akka Edge brings familiar concepts to
developers familiar with Akka when used elsewhere, while simultaneously recognizing the resource constraints present at the edge.

Akka Edge facilitates the building of [Reactive Systems](https://www.reactivemanifesto.org/) with a particular emphasis on resilience and responsiveness. 
Resilience is achieved by being able to recover in the face of an, say, an internet connection failing, and being able to continue local-first operations. Responsiveness is achieved primarily through being event-driven. As events occur at the edge, they can be pushed to the cloud, to the user, and other
parts of the edge-based system - at the time they occur.

## Minimum supported Rust

streambed-rs requires a minimum of Rust version 1.70.0 stable (June 2023), but the most recent stable version of Rust is recommended.
