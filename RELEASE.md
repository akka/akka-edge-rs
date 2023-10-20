Releasing
===

1. Update the cargo.toml and change the `workspace.package.version`.
2. Change the `dependency.akka-*` versions to be the same version number as per step 1.
3. Commit the changes and tag with the version using `v` as a prefix e.g. 1.0.0 would be "v1.0.0".
4. Perform the commands below, and in the same order...
5. To publish from CI, tag the release as per step 1 and [run the job manually](https://github.com/lightbend/akka-edge-rs/actions/workflows/publish.yml).

```
cargo publish -p akka-persistence-rs
cargo publish -p akka-persistence-rs-commitlog
cargo publish -p akka-projection-rs
cargo publish -p akka-projection-rs-commitlog
cargo publish -p akka-projection-rs-grpc
cargo publish -p akka-projection-rs-storage
```

### Publish to Cloudsmith
Ensure the following env vars are available:
```
export CARGO_REGISTRIES_AKKA_RS_INDEX=https://dl.cloudsmith.io/{entitlement-token}/lightbend/akka-rs/cargo/index.git
export CARGO_REGISTRIES_AKKA_RS_TOKEN={api-key}
```

Credentials bound to the `cloudsmith-machine` user should be used:
- `{entitlement-token` can be found [here](https://cloudsmith.io/~lightbend/repos/akka-rs/entitlements/)
- `{api-key}` can be found [here](https://cloudsmith.io/user/settings/api/)

Also make sure to specify the registry `--registry` accordingly, for example:
```
cargo publish -p akka-persistence-rs-commitlog --registry AKKA_RS
```