IoT service example
===

This is an example largely based on [Streambed's](https://github.com/streambed/streambed-rs/tree/main/examples/iot-service) 
and illustrates the use of streambed that writes ficticious temperature sensor observations
to a commit log. A simple HTTP API is provided to query and scan the commit log. The main change
is the use of the Akka Persistence Entity Manager in place of the `Database` struct that
the Streambed example uses, and the integration with the [iot-service](https://github.com/akka/akka-projection/tree/main/samples/grpc/iot-service-scala) of Akka projections.

In terms of roles and responsibilities, the JVM service is responsible for registering sensors. The
Rust service will connect to the JVM service and consume these registration events as they occur. The Rust
service will also remember where it is up to and, in the case of a restart, it will re-connect and consume
any new registrations from where it left off. Observations can then be sent to the Rust service via UDP.
The Rust service will use its established connection with the JVM service to propogate these local observations.

The project is a complete example of how a Rust service can be written for the edge, and includes encryption. 
Encryption should normally be applied to data at rest (persisted by the commit log) and in flight 
(http and UDP). For simplicity, we apply encryption to the commit log data, but not http and UDP.

The example requires running the JVM [iot-service](https://github.com/akka/akka-projection/tree/main/samples/grpc/iot-service-scala) of Akka projections in parallel.

Running
---

To run via cargo, first `cd` into this directory and then:

```
mkdir -p /tmp/iot-service/var/lib/confidant
chmod 700 /tmp/iot-service/var/lib/confidant
mkdir -p /tmp/iot-service/var/lib/logged
echo -n "01234567890123456789012345678912some-secret-id" | \
RUST_LOG=debug cargo run -- \
  --cl-root-path=/tmp/iot-service/var/lib/logged \
  --ss-role-id="iot-service" \
  --ss-root-path=/tmp/iot-service/var/lib/confidant
```

We must first register the device ids that we wish to receive data for. This is a form
of authentication where, in the real-world, a shared key between the device and service
would be conveyed. That key would then be used to encrypt data. We simply use the key
as a registration mechanism and do not accept data for devices where we have no key.

Let's first query for a sensor's data... it will fail as we have no sensors yet:

```
curl -v "127.0.0.1:8080/api/temperature/1"
```

So, let's now provision one. To do this, we must start up the JVM-based iot-service. Please
follow steps 1 and 2 at the [iot-service](https://github.com/akka/akka-projection/blob/main/samples/grpc/iot-service-scala/README.md)'s
README. Once done, provision a sensor:

```
grpcurl -d '{"sensor_entity_id":"1", "secret":"foo"}' -plaintext 127.0.0.1:8101 iot.registration.RegistrationService.Register
```

You should now be able to query for the current state of a temperature sensor, although
they'll be no observations recorded for it yet:

```
curl -v "127.0.0.1:8080/api/temperature/1"
```

Let's now post database events to the UDP socket so that the sensor has observations. Note that
we're using Postcard to deserialize binary data. Postcard uses variable length
integers where the top bit, when set, indicates that the next byte also contains
data. See [Postcard](https://docs.rs/postcard/latest/postcard/) for more details.

```
echo -n "\x01\x02" | nc -w0 127.0.0.1 -u 8081
```

You should see a `DEBUG` log indicating that the post has been received. And
also be able to query the database again with the id that was sent (`1`):

```
curl -v "127.0.0.1:8080/api/temperature/1"
```

Back over in the JVM iot-service, you should also see these temperature observations
appear in its log.