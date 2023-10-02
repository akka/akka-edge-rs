// Handle temperature sensor state concerns

use std::collections::VecDeque;

use akka_persistence_rs::entity::Context;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

// Declare the state and how it is to be sourced from events

#[derive(Clone, Default)]
pub struct State {
    pub history: VecDeque<u32>,
    pub secret: SecretDataValue,
}

const MAX_HISTORY_EVENTS: usize = 10;

impl State {
    // We provide an event sourcing function so that we can
    // source state from events whether we are using the entity
    // manager or not.

    pub fn on_event(&mut self, _context: &Context, event: Event) {
        match event {
            Event::Registered { secret } => {
                self.secret = secret;
            }
            Event::TemperatureRead { temperature } => {
                if self.history.len() == MAX_HISTORY_EVENTS {
                    self.history.pop_front();
                }
                self.history.push_back(temperature);
            }
        }
    }
}

pub type SecretDataValue = SmolStr;

#[derive(Clone, Deserialize, Serialize)]
pub enum Event {
    Registered { secret: SecretDataValue },
    TemperatureRead { temperature: u32 },
}
