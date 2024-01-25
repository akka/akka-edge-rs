# Releasing Rust crates

Create a new issue from the [Release Train Issue Template](scripts/release-train-issue-template.md):

```
$ sh ./scripts/create-release-issue.sh 0.x.y
```

## Manual releasing to Cloudsmith
Ensure the following env vars are available:
```
export CARGO_REGISTRIES_AKKA_RS_INDEX=https://dl.cloudsmith.io/{entitlement-token}/lightbend/akka-rs/cargo/index.git
export CARGO_REGISTRIES_AKKA_RS_TOKEN={api-key}
```

Credentials bound to the `cloudsmith-machine` user should be used:
- `{entitlement-token}` can be found [here](https://cloudsmith.io/~lightbend/repos/akka-rs/entitlements/)
- `{api-key}` can be found [here](https://cloudsmith.io/user/settings/api/)

Also make sure to specify the registry `--registry` accordingly, for example:
```
cargo publish -p akka-persistence-rs-commitlog --registry AKKA_RS
```

# Releasing docs

The [Akka Edge Rust guide](https://doc.akka.io/docs/akka-edge/current/guide-rs.html) is released as part of the [Akka Projections documentation](https://github.com/akka/akka-projection/blob/main/RELEASING.md).
