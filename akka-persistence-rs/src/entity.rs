//! An event sourced entity (also known as a persistent entity) receives a (non-persistent) command which is first validated
//! if it can be applied to the current state. Here validation can mean anything, from simple inspection of a command message’s
//! fields up to a conversation with several external services, for example. If validation succeeds, events are generated
//! from the command, representing the effect of the command. These events are then persisted and, after successful persistence,
//! used to change the entity's state. When the event sourced actor needs to be recovered, only the persisted events are replayed
//! of which we know that they can be successfully applied. In other words, events cannot fail when being replayed to a persistent
//! entity, in contrast to commands. Event sourced entities may also process commands that do not change application state such
//! as query commands for example.

use crate::effect::Effect;
use crate::EntityId;

/// A context provides information about the environment that hosts a specific entity.
pub struct Context {
    /// The entity's unique identifier.
    pub entity_id: EntityId,
}

/// An entity's behavior is the basic unit of modelling aspects of an Akka-Persistence-based application and
/// encapsulates how commands can be applied to state, including the emission of events. Events can
/// also be applied to state in order to produce more state.
pub trait EventSourcedBehavior {
    /// The state managed by the entity.
    type State: Default;
    /// The command(s) that are able to be processed by the entity.
    type Command;
    /// The event emitted having performed an effect.
    type Event;

    /// Given a state and command, optionally emit an effect that may cause an
    /// event transition. Events are responsible for mutating state.
    /// State can also be associated with the behavior so that other effects can be
    /// performed. For example, a behavior might be created with a channel sender
    /// so that data can be sent as an effect of performing a command.
    fn for_command(
        &self,
        context: &Context,
        state: Option<&Self::State>,
        command: Self::Command,
    ) -> Box<dyn Effect<Self::Event>>;

    /// Given a state and event, modify state, which could indicate transition to
    /// the next state. No side effects are to be performed. Can be used to replay
    /// events to attain a new state i.e. the major function of event sourcing.
    fn on_event(context: &Context, state: &mut Self::State, event: &Self::Event);
}
