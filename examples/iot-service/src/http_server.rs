//! Handle http serving concerns
//!
use crate::{
    registration::{self, SecretDataValue},
    temperature,
};
use akka_persistence_rs::Message;
use rand::RngCore;
use tokio::sync::{mpsc, oneshot};
use warp::{hyper::StatusCode, Filter, Rejection, Reply};

/// Declares routes to serve our HTTP interface.
pub fn routes(
    registration_command: mpsc::Sender<Message<registration::Command>>,
    temperature_command: mpsc::Sender<Message<temperature::Command>>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let get_temperature_route = {
        warp::get()
            .and(warp::path::param())
            .and(warp::path::end())
            .then(move |id: String| {
                let task_temperature_command = temperature_command.clone();
                async move {
                    let Ok(id) = id.parse::<u32>() else {
                        return warp::reply::with_status(
                            warp::reply::json(&"Invalid id - must be a number"),
                            StatusCode::BAD_REQUEST,
                        )
                    };

                    let (reply_to, reply) = oneshot::channel();
                    let Ok(_) = task_temperature_command
                        .send(Message::new(
                            id.to_string(),
                            temperature::Command::Get { reply_to },
                        ))
                        .await else {
                        return warp::reply::with_status(
                            warp::reply::json(&"Service unavailable"),
                            StatusCode::SERVICE_UNAVAILABLE,
                        )
                     };
                    let Ok(events) = reply.await else {
                        return warp::reply::with_status(
                            warp::reply::json(&"Id not found"),
                            StatusCode::NOT_FOUND,
                        )
                     };

                    warp::reply::with_status(warp::reply::json(&events), StatusCode::OK)
                }
            })
    };

    let post_registration_route = {
        warp::post()
            .and(warp::path::end())
            .and(warp::body::json())
            .then(move |id: String| {
                let task_registration_command_command = registration_command.clone();
                async move {
                    // Generate a random key - a real world app might provide this instead.
                    let mut key = vec![0; 16];
                    rand::thread_rng().fill_bytes(&mut key);

                    let Ok(_) = task_registration_command_command
                        .send(Message::new(
                            id,
                            registration::Command::Register { secret: SecretDataValue::from(hex::encode(key)) },
                        ))
                        .await else {
                        return warp::reply::with_status(
                            warp::reply::json(&"Service unavailable"),
                            StatusCode::SERVICE_UNAVAILABLE,
                        )
                     };

                    warp::reply::with_status(warp::reply::json(&"Secret submitted"), StatusCode::OK)
                }
            })
    };

    warp::path("api")
        .and(warp::path("temperature").and(get_temperature_route.or(post_registration_route)))
}
