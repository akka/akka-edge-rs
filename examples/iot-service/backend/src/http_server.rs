// Handle http serving concerns

use std::time::Duration;

use crate::temperature;
use akka_persistence_rs::{EntityId, Message};
use async_stream::stream;
use futures::{future, stream::StreamExt};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time,
};
use warp::{filters::sse::Event, Filter, Rejection, Reply};

// We use this to limit the maximum amount of time that an SSE connection can be held.
// Any consumer of the SSE events will automatically re-connect if they are still able
// to. This circumvents keeping a TCP socket open for many minutes in the absence of
// there being a connection. While producing our SSE events should relatively efficient,
// we don't do it unless we really need to, and TCP isn't great about detecting the
// loss of a network
const MAX_SSE_CONNECTION_TIME: Duration = Duration::from_secs(60);

// Declares routes to serve our HTTP interface.
pub fn routes(
    temperature_command: mpsc::Sender<Message<temperature::Command>>,
    temperature_events: broadcast::Sender<temperature::BroadcastEvent>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let get_temperature_route = {
        warp::get()
            .and(warp::path("events"))
            .and(warp::path::param())
            .and(warp::path::end())
            .map(move |entity_id: String| {
                // Request the current state, deconstruct the state back into events,
                // and then emit new events as they are broadcast.

                let mut temperature_events_receiver = temperature_events.subscribe();
                let task_temperature_command = temperature_command.clone();
                let event_stream = stream!({
                    let entity_id = EntityId::from(entity_id);
                    let (reply_to, reply) = oneshot::channel();
                    let result = task_temperature_command
                        .send(Message::new(
                            entity_id.clone(),
                            temperature::Command::Get { reply_to },
                        ))
                        .await;
                    if result.is_ok() {
                        if let Ok(state) = reply.await {
                            yield Event::default().id(entity_id.clone()).json_data(
                                temperature::Event::Registered {
                                    secret: state.secret,
                                },
                            );

                            for temperature in state.history {
                                yield Event::default()
                                    .json_data(temperature::Event::TemperatureRead { temperature });
                            }

                            while let Ok(temperature::BroadcastEvent::Saved {
                                entity_id: event_entity_id,
                                event,
                            }) = temperature_events_receiver.recv().await
                            {
                                if event_entity_id == entity_id {
                                    yield Event::default().json_data(event);
                                }
                            }
                        }
                    }
                });

                let event_stream = event_stream.take_until(time::timeout(
                    MAX_SSE_CONNECTION_TIME,
                    future::pending::<()>(),
                ));

                let event_stream = warp::sse::keep_alive().stream(event_stream);
                warp::sse::reply(event_stream)
            })
    };

    let routes = get_temperature_route;

    warp::path("api").and(warp::path("temperature").and(routes))
}
