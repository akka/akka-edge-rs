fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["akka/projection/grpc/event_consumer.proto"], &["proto"])?;

    Ok(())
}
