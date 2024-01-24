# Akka Edge Rust

*Extending the power of Akka’s event-driven model to resource-constrained devices.*

Akka Edge support in Rust is designed to empower cloud developers at the edge. Akka Edge brings familiar concepts to developers familiar with Akka when used elsewhere, while simultaneously recognizing the resource constraints present at the edge.

## Documentation

The Akka documentation is available at [https://akka.io/docs/](akka.io/docs).

The Akka Edge guides are found via [akka.io/edge](https://doc.akka.io/docs/akka-edge/current/guide-rs.html).
The reference documentation is part of the [Akka Edge Rust API docs](https://doc.akka.io/api/akka-edge-rs/current/).

## Current versions of all Akka libraries

The current versions of all Akka libraries are listed on the [Akka Dependencies](https://doc.akka.io/docs/akka-dependencies/current/) page. Releases of the Akka Edge Rust libraries in this repository are listed on the [GitHub releases](https://github.com/akka/akka-edge-rs/releases) page.

Akka Edge Rust is currently [Incubating](https://doc.akka.io/docs/akka-dependencies/current/support-terminology.html#incubating), and we are eager for your feedback.

### Minimum supported Rust

This library requires a minimum of [Rust](https://www.rust-lang.org/) version 1.70.0 stable (June 2023), but the current stable version of Rust is recommended.

## Akka

[Akka](https://akka.io/) facilitates the building of [Reactive Systems](https://www.reactivemanifesto.org/) with a particular emphasis on resilience and responsiveness. 
Resilience is achieved by being able to recover in the face of, say, an internet connection failing, and being able to continue local-first operations. Responsiveness is achieved primarily through being event-driven. As events occur at the edge, they can be pushed to the cloud, to the user, and other parts of the edge-based system - at the time they occur.

The Akka family of projects is managed by teams at [Lightbend](https://lightbend.com) with help from the community.

## Contributing

Contributions are *very* welcome!

If you see an issue that you'd like to see fixed, the best way to make it happen is to help out by submitting a pull request.

Refer to the [Akka CONTRIBUTING.md](https://github.com/akka/.github/blob/master/CONTRIBUTING.md) file for more details about the workflow,
and general hints on how to prepare your pull request. You can also ask for clarifications or guidance in GitHub issues directly.

## Further reading

- [Akka Edge: Unifying the Cloud and Edge](https://www.lightbend.com/blog/akka-edge-unifying-the-cloud-and-edge) by @jboner
- [What is Akka Edge?](https://www.lightbend.com/blog/what-is-akka-edge) by @patriknw
- [Webinar: Akka Edge Sample Project Overview](https://www.lightbend.com/blog/webinar-akka-edge-sample-project-overview) by @johanandren

## License

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).
