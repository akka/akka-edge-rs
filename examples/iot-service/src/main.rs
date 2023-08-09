use std::{error::Error, net::SocketAddr};

use clap::Parser;
use git_version::git_version;
use log::info;
use streambed_logged::{args::CommitLogArgs, FileLog};
use tokio::{net::UdpSocket, sync::mpsc};

mod http_server;
mod temperature;
mod udp_server;

/// This service receives IoT data re. temperature, stores it in the
/// commit log keyed by its sensor id, and provides an HTTP interface
/// to access it.
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, version = git_version ! ())]
struct Args {
    /// Logged commit log args
    #[clap(flatten)]
    cl_args: CommitLogArgs,

    /// A socket address for serving our HTTP web service requests.
    #[clap(env, long, default_value = "127.0.0.1:8080")]
    http_addr: SocketAddr,

    /// A socket address for receiving telemetry from our ficticious
    /// sensor.
    #[clap(env, long, default_value = "127.0.0.1:8081")]
    udp_addr: SocketAddr,
}

const MAX_TEMPERATURE_MANAGER_COMMANDS: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder().format_timestamp_millis().init();

    // Set up the commit log
    let cl = FileLog::new(args.cl_args.cl_root_path.clone());

    // Establish channels for the temperature
    let (temperature_command, temperature_command_receiver) =
        mpsc::channel(MAX_TEMPERATURE_MANAGER_COMMANDS);

    // Start up the http service
    let routes = http_server::routes(temperature_command.clone());
    tokio::spawn(warp::serve(routes).run(args.http_addr));
    info!("HTTP listening on {}", args.http_addr);

    // Start up the UDP service
    let socket = UdpSocket::bind(args.udp_addr).await?;
    tokio::spawn(udp_server::task(socket, temperature_command));
    info!("UDP listening on {}", args.udp_addr);

    // All things started up but our temperature. We're running
    // that in our main task.

    info!("IoT service ready");

    tokio::spawn(async { temperature::task(cl, temperature_command_receiver).await })
        .await?
        .map_err(|e| e.into())
}
