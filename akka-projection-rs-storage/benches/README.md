Benchmarking
===

To invoke a benchmark:

```
cd akka-projection-rs-storage
cargo bench
```

The above will compare with any previous benchmark run. Thus, you can checkout a commit, run the benchmark, then
re-run having applied any changes you have made.

To instrument on OS X (requires `cargo install cargo-instruments` and the Xcode dev tools):

```
cargo instruments --bench "benches" --profile "bench-debug" -t "time" -- --bench
```