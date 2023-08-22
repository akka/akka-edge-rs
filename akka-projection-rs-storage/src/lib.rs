#![doc = include_str!("../README.md")]

use akka_projection_rs::{Handler, SourceProvider};
use tokio::sync::mpsc::Receiver;

/// The commands that a projection task is receptive to.
pub enum Command {
    Stop,
}

/// Provides local file system based storage for projection offsets.
pub async fn run<E, FSP, H, SP>(_receiver: Receiver<Command>, _source_provider: FSP, _handler: H)
where
    H: Handler<Envelope = E>,
    FSP: FnMut(u32) -> Option<SP>,
    SP: SourceProvider<Envelope = E>,
{
}
