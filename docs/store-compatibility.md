# Store metadata-node compatibility

The `fix/store-meta-node` branch is based on OpenRaft `v0.9.25` and supports the
`GatewayJ/store` metadata node with these features:

```toml
openraft = { path = "../../openraft/openraft", features = ["serde", "storage-v2", "generic-snapshot-data"] }
```

Keep the repositories in sibling directories:

```text
workspace/
  store/
  openraft/       # fix/store-meta-node
  async-spdk/     # fix/store-data-node
```

The custom `SnapshotData` in `store` carries a Tonic stream. It requires
`generic-snapshot-data`; snapshots are transported through `full_snapshot`.
The metadata node implements the separate `RaftLogStorage` and `RaftStateMachine`
interfaces enabled by `storage-v2`.

`RaftLogStorage::save_vote` accepts a `LogFlushed` callback so the store can share
its background WAL flusher with log appends. Raft waits for this callback before
updating durable vote state or acknowledging the vote. Flush failures and dropped
callbacks become vote-storage errors. Call `RaftLogStorageExt::blocking_save_vote`
when a caller needs the durable completion result directly. The legacy storage
adapter completes the callback after its synchronous persistence method succeeds.

Build on Linux with a Rust stable toolchain, a C/C++ compiler, Clang/libclang,
CMake, and `protoc`. From the workspace directory:

```sh
RUSTUP_TOOLCHAIN=stable cargo test --manifest-path openraft/Cargo.toml \
  -p openraft --lib --features serde,storage-v2,generic-snapshot-data
RUSTUP_TOOLCHAIN=stable cargo test --manifest-path openraft/Cargo.toml -p tests
RUSTUP_TOOLCHAIN=stable cargo check --manifest-path store/Cargo.toml \
  -p msc_meta_node -p msc_log_client
```

These commands were validated against store revision
`4f23e4e0b4ed09653d77c763fbb4359ec319b036`.
