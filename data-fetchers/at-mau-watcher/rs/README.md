# at-mau-watcher (Rust)

Track accounts seen on the ATProto firehose and write periodic JSON snapshots.

## Usage

```sh
cargo run --release -- --relay wss://bsky.network/xrpc
```

Load a large snapshot produced elsewhere, but write to a new file:

```sh
cargo run --release -- --snapshot-read-file /path/to/python_snapshot.json --snapshot-file accounts_snapshot.json
```

Show all options:

```sh
cargo run --release -- --help
```
