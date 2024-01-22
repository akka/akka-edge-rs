# Releasing

1. Update the cargo.toml and change the `workspace.package.version`.
2. Change the `dependency.akka-*` versions to be the same version number as per step 1.
3. Commit the changes.
4. Create a release from https://github.com/lightbend/akka-edge-rs/releases. Tag with the same version as in cargo.toml
   using `v` as a prefix e.g. 1.0.0 would be "v1.0.0".
5. [CI workflow](https://github.com/lightbend/akka-edge-rs/actions/workflows/publish.yml) will publish to
   http://repo.akka.io/cargo and https://doc.akka.io/api/akka-edge-rs/`<version>`/

## Publish to Cloudsmith
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
