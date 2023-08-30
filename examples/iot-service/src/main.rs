mod http_server;
#[cfg(feature = "local")]
mod registration;
mod registration_projection;
#[cfg(feature = "grpc")]
mod registration_proto;
mod temperature;
mod temperature_production;
mod udp_server;

use clap::Parser;
use git_version::git_version;
use log::info;
use rand::RngCore;
#[cfg(feature = "grpc")]
use registration_proto as registration;
use std::{collections::HashMap, error::Error, net::SocketAddr};
use streambed::secret_store::{SecretData, SecretStore};
use streambed_confidant::{args::SsArgs, FileSecretStore};
use streambed_logged::{args::CommitLogArgs, FileLog};
use tokio::{net::UdpSocket, sync::mpsc};
use tonic::transport::Uri;

/// This service receives IoT data re. temperature, stores it in the
/// commit log keyed by its sensor id, and provides an HTTP interface
/// to access it.
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, version = git_version ! ())]
struct Args {
    /// Logged commit log args
    #[clap(flatten)]
    cl_args: CommitLogArgs,

    /// A socket address for connecting to a GRPC event consuming
    /// service for temperature observations.
    #[clap(env, long, default_value = "http://127.0.0.1:8101")]
    event_consumer_addr: Uri,

    /// A socket address for connecting to a GRPC event producing
    /// service for registrations. Only relevant when building the
    /// example with the "grpc" feature.
    #[clap(env, long, default_value = "http://127.0.0.1:8101")]
    event_producer_addr: Uri,

    /// A socket address for serving our HTTP web service requests.
    #[clap(env, long, default_value = "127.0.0.1:8080")]
    http_addr: SocketAddr,

    /// Logged commit log args
    #[clap(flatten)]
    ss_args: SsArgs,

    /// A socket address for receiving telemetry from our ficticious
    /// sensor.
    #[clap(env, long, default_value = "127.0.0.1:8081")]
    udp_addr: SocketAddr,
}

#[cfg(feature = "local")]
const MAX_REGISTRATION_MANAGER_COMMANDS: usize = 10;

const MAX_REGISTRATION_PROJECTION_MANAGER_COMMANDS: usize = 1;
const MAX_TEMPERATURE_MANAGER_COMMANDS: usize = 10;
const MAX_TEMPERATURE_PRODUCTION_MANAGER_COMMANDS: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder().format_timestamp_millis().init();

    // Setup and authenticate our service with the secret store
    let ss = {
        let line = streambed::read_line(std::io::stdin()).unwrap();
        assert!(!line.is_empty(), "Failed to source a line from stdin");
        let (root_secret, ss_secret_id) = line.split_at(32);
        let root_secret = hex::decode(root_secret).unwrap();

        let ss = FileSecretStore::new(
            args.ss_args.ss_root_path,
            &root_secret.try_into().unwrap(),
            args.ss_args.ss_unauthorized_timeout.into(),
            args.ss_args.ss_max_secrets,
            args.ss_args.ss_ttl_field.as_deref(),
        );

        ss.approle_auth(&args.ss_args.ss_role_id, ss_secret_id)
            .await
            .unwrap();

        ss
    };

    // The path of our key we use to encrypt data at rest.
    let temperature_events_key_secret_path =
        format!("{}/secrets.temperature-events.key", args.ss_args.ss_ns);

    // The path of a key to use to encrypt offsets for registration projections. We'll
    // just re-use the above key for convenience, but ordinarily, these keys would be
    // different.
    let registration_offset_key_secret_path = temperature_events_key_secret_path.clone();

    // The path of a key to use to encrypt offsets for temperature productions. We'll
    // just re-use the above key for convenience, but ordinarily, these keys would be
    // different.
    let temperature_offset_key_secret_path = temperature_events_key_secret_path.clone();

    if let Ok(None) = ss.get_secret(&temperature_events_key_secret_path).await {
        // If we can't write this initial secret then all bets are off
        let mut key = vec![0; 16];
        rand::thread_rng().fill_bytes(&mut key);
        let data = HashMap::from([("value".to_string(), hex::encode(key))]);
        ss.create_secret(&temperature_events_key_secret_path, SecretData { data })
            .await
            .unwrap();
    }

    // Set up the commit log
    let cl = FileLog::new(args.cl_args.cl_root_path.clone());

    // Establish channels for the temperature
    let (temperature_command, temperature_command_receiver) =
        mpsc::channel(MAX_TEMPERATURE_MANAGER_COMMANDS);

    // Establish channels for the registrations
    #[cfg(feature = "local")]
    let (registration_command, registration_command_receiver) =
        mpsc::channel(MAX_REGISTRATION_MANAGER_COMMANDS);

    // Establish channels for the registration projection
    let (_registration_projection_command, registration_projection_command_receiver) =
        mpsc::channel(MAX_REGISTRATION_PROJECTION_MANAGER_COMMANDS);

    // Establish channels for the temperature production
    let (_temperature_production_command, temperature_production_command_receiver) =
        mpsc::channel(MAX_TEMPERATURE_PRODUCTION_MANAGER_COMMANDS);

    // Start up the http service
    let routes = http_server::routes(
        #[cfg(feature = "local")]
        registration_command,
        temperature_command.clone(),
    );
    tokio::spawn(warp::serve(routes).run(args.http_addr));
    info!("HTTP listening on {}", args.http_addr);

    // Start up the UDP service
    let socket = UdpSocket::bind(args.udp_addr).await?;
    tokio::spawn(udp_server::task(socket, temperature_command.clone()));
    info!("UDP listening on {}", args.udp_addr);

    // Start up a task to manage registrations
    #[cfg(feature = "local")]
    tokio::spawn(registration::task(
        cl.clone(),
        ss.clone(),
        temperature_events_key_secret_path.clone(),
        registration_command_receiver,
    ));

    // Start up a task to manage registration projections
    tokio::spawn(registration_projection::task(
        #[cfg(feature = "local")]
        cl.clone(),
        #[cfg(feature = "grpc")]
        args.event_producer_addr,
        ss.clone(),
        #[cfg(feature = "local")]
        temperature_events_key_secret_path.clone(),
        registration_offset_key_secret_path.clone(),
        registration_projection_command_receiver,
        args.cl_args.cl_root_path.join("registration-offsets"),
        temperature_command,
    ));

    // Start up a task to manage temperature productions
    tokio::spawn(temperature_production::task(
        cl.clone(),
        args.event_consumer_addr,
        ss.clone(),
        temperature_events_key_secret_path.clone(),
        temperature_offset_key_secret_path.clone(),
        temperature_production_command_receiver,
        args.cl_args.cl_root_path.join("temperature-offsets"),
    ));

    // All things started up but our temperature. We're running
    // that in our main task. Therefore, we will return once the
    // entity manager has finished.

    info!("IoT service ready");

    tokio::spawn(temperature::task(
        cl,
        ss,
        temperature_events_key_secret_path,
        temperature_command_receiver,
    ))
    .await?;

    // If we get here then we are shutting down. Any other task,
    // such as the projection one, will stop automatically given
    // that its sender will be dropped.

    Ok(())
}
