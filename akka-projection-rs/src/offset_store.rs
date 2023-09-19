//! An offset store keeps track of the last offset used in relation to
//! an entity. It can be used in various places, for example, when consuming
//! event envelopes from a remote projection, where they be emitted by the
//! consumer out of order. This entity can track such situations.
use akka_persistence_rs::{
    effect::{emit_event, reply, Effect, EffectExt},
    entity::{Context, EventSourcedBehavior},
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Clone, Default)]
pub struct State {
    pub last_seq_nr: u64,
}

pub enum Command {
    Get { reply_to: oneshot::Sender<State> },
    Save { seq_nr: u64 },
}

#[derive(Clone, Deserialize, Serialize)]
pub enum Event {
    Saved { seq_nr: u64 },
}

pub struct Behavior;

impl EventSourcedBehavior for Behavior {
    type State = State;
    type Command = Command;
    type Event = Event;

    fn for_command(
        _context: &Context,
        state: &Self::State,
        command: Self::Command,
    ) -> Box<dyn Effect<Self>> {
        match command {
            Command::Get { reply_to } => reply(reply_to, state.clone()).boxed(),
            Command::Save { seq_nr } => emit_event(Event::Saved { seq_nr }).boxed(),
        }
    }

    fn on_event(_context: &Context, state: &mut Self::State, event: Self::Event) {
        let Event::Saved { seq_nr } = event;
        state.last_seq_nr = seq_nr;
    }
}
