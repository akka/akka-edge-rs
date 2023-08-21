#![doc = include_str!("../README.md")]

use akka_projection_rs::{Handler, SourceProvider};
use tokio::sync::mpsc::Receiver;

/// The commands that a projection task is receptive to.
pub enum Command {
    Stop,
}

/// Provides local file system based storage for projection offsets.
pub async fn run<E, H, SP>(_receiver: Receiver<Command>, _source_provider: SP, _handler: H)
where
    H: Handler<Envelope = E>,
    SP: SourceProvider<Envelope = E>,
{
}
