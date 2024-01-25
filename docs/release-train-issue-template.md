Release Akka Edge Rust $VERSION$

<!--
# Release Train Issue Template for Akka Edge Rust

(Liberally copied and adopted from Scala itself https://github.com/scala/scala-dev/blob/b11cd2e4a4431de7867db6b39362bea8fa6650e7/notes/releases/template.md)

For every release, use the `scripts/create-release-issue.sh` to make a copy of this file named after the release, and expand the variables.

Variables to be expanded in this template:
- $VERSION$=???

-->

### Cutting the release

- [ ] Check that open PRs and issues assigned to the milestone are reasonable
- [ ] Update the cargo.toml and change the `workspace.package.version` to `$VERSION$`. Change the `dependency.akka-*` versions to be the same version number.
- [ ] Update the version and change date in the LICENSE file.
- [ ] Create a new milestone for the [next version](https://github.com/akka/akka-edge-rs/milestones)
- [ ] Close the [$VERSION$ milestone](https://github.com/akka/akka-edge-rs/milestones?direction=asc&sort=due_date)
- [ ] Make sure all important PRs have been merged
- [ ] Wait until [main build finished](https://github.com/akka/akka-edge-rs/actions) after merging the latest PR
- [ ] Update the [draft release](https://github.com/akka/akka-edge-rs/releases) with the next tag version `v$VERSION$`, title and release description. Use the `Publish release` button, which will create the tag.
- [ ] Check that GitHub Actions release build has executed successfully (GitHub Actions will start a [CI build](https://github.com/akka/akka-edge-rs/actions) for the new tag and publish artifacts to https://repo.akka.io/cargo/)

### Check availability

- [ ] Check [API](https://doc.akka.io/api/akka-edge-rs/v$VERSION$/) documentation
- [ ] Check the release by updating the akka versions in https://github.com/lightbend/akka-projection-temp/tree/main/samples/grpc/iot-service-rs. Run `cargo check` in `iot-service-rs`.

### Update current links
  - [ ] Log into `gustav.akka.io` as `akkarepo` 
    - [ ] If this updates the `current` version, run `./update-akka-rs-current-version.sh $VERSION$`
    - [ ] otherwise check changes and commit the new version to the local git repository
         ```
         cd ~/www
         git status
         git add api/akka-edge-rs/current api/akka-edge-rs/$VERSION$
         git commit -m "akka-edge-rs $VERSION$"
         ```

### Announcements

For important patch releases, and only if critical issues have been fixed:

- [ ] Send a release notification to [Lightbend discuss](https://discuss.akka.io)
- [ ] Tweet using the [@akkateam](https://twitter.com/akkateam/) account (or ask someone to) about the new release
- [ ] Announce internally (with links to Tweet, discuss)

For minor or major releases:

- [ ] Include noteworthy features and improvements in Akka umbrella release announcement at akka.io. Coordinate with PM and marketing.

### Afterwards

- [ ] Update version for [Akka module versions](https://doc.akka.io/docs/akka-dependencies/current/) in [akka-dependencies repo](https://github.com/akka/akka-dependencies)
- [ ] Update [Akka Edge Rust samples](https://github.com/lightbend/akka-projection/tree/main/samples/grpc/iot-service-rs)
- [ ] The [Akka Edge Rust guide](https://doc.akka.io/docs/akka-edge/current/guide-rs.html) is released as part of the [Akka Projections documentation](https://github.com/akka/akka-projection/blob/main/RELEASING.md).
- Close this issue
