IoT service example
===

This is an example largely based on [Streambed's](https://github.com/streambed/streambed-rs/tree/main/examples/iot-service) 
that illustrates the use of streambed that writes telemetry
to an append log. A simple HTTP API is provided to query and scan the commit log. The main change
is the use of the Akka Persistence Entity Manager in place of the `Database` struct that
the Streambed example uses.

The service is a complete example and includes encryption. Encryption should normally be applied to data
at rest (persisted by the commit log) and in flight (http and UDP). For simplicity, we apply encryption
to the commit log data, but not http and UDP.

Running locally
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

So, let's now provision one:

```
curl -v -d '"1"' -H"Content-Type: application/json" "127.0.0.1:8080/api/temperature"
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

You should see a `DEBUG` log indicating that the post has been received. You should
also be able to query the database again with the id that was sent (`1`):

```
curl -v "127.0.0.1:8080/api/temperature/1"
```

You should also be able to see events being written to the log file store itself:

```
hexdump /tmp/iot-service/var/lib/logged/temperature
```

Compaction
----

If you would like to see compaction at work then you can drive UDP traffic with
a script such as the following:

```bash
#!/bin/bash
for i in {0..2000}
do
  printf "\x01\x02" | nc -w0 127.0.0.1 -u 8081
done
```

The above will send 2,000 UDP messages to the service. Toward the end, if you watch
the log of the service, you will see various messages from the compactor at work.
When it has finished, you can observe that they are two log files for our topic e.g.:

```
ls -al /tmp/iot-service/var/lib/logged

drwxr-xr-x  4 huntc  wheel    128 Aug 10 12:04 .
drwxr-xr-x  4 huntc  wheel    128 Aug 10 12:02 ..
-rw-r--r--  1 huntc  wheel  42444 Aug 10 12:04 temperature
-rw-r--r--  1 huntc  wheel    540 Aug 10 12:04 temperature.history
```

The `history` file contains the compacted log. As each record is 54 bytes, this means
that compaction retained the last 10 records (540 bytes). The active file, or `temperature`,
contains several hundred records that continued to be written while compaction was
in progress. The compactor is designed to avoid back-pressuring the production of 
records. That said, if the production of events overwhelms compaction then
it will back-pressure on the producer. It is up to you, as the application developer,
to decide whether to always await the completion of producing a record. In some
real-time scenarios, waiting on confirmation that an event is written may be 
undesirable. However, with the correct dimensioning of the commit log in terms of
its buffers and how the compaction strategies are composed, compaction back-pressure
can also be avoided.

Running with gRPC
---

The example may also be run with the registration projection connecting over a gRPC 
connection to a Akka-JVM based remote service. The source code and instructions for
running the remote service can be found at:

https://github.com/akka/akka-projection/tree/main/samples/grpc/iot-service-scala

Please start the remote service first, and then run the local service with a slightly
different configuration of features:

```
mkdir -p /tmp/iot-service/var/lib/confidant
chmod 700 /tmp/iot-service/var/lib/confidant
mkdir -p /tmp/iot-service/var/lib/logged
echo -n "01234567890123456789012345678912some-secret-id" | \
RUST_LOG=debug cargo run --no-default-features --features="grpc" -- \
  --cl-root-path=/tmp/iot-service/var/lib/logged \
  --ss-role-id="iot-service" \
  --ss-root-path=/tmp/iot-service/var/lib/confidant
```
