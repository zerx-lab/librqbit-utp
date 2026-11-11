# librqbit-utp

A uTP library in Rust inspired by (and much copy-pasted from!) awesome [smoltcp](https://github.com/smoltcp-rs/smoltcp)

Intended for use in [rqbit](https://github.com/ikatson/rqbit) torrent client.

See example usage in [examples](https://github.com/ikatson/librqbit-utp/blob/main/examples/echo_minimal.rs).

## Congestion control

Two congestion controllers are available, chosen per socket with `SocketOpts::congestion`:

- `CongestionControllerKind::Cubic` (the default): loss-based like TCP ([RFC 8312](https://datatracker.ietf.org/doc/html/rfc8312)). It fills the bottleneck queue until packets drop and competes with other traffic for an equal share.
- `CongestionControllerKind::Ledbat`: the delay-based controller of uTP ([BEP 29](https://www.bittorrent.org/beps/bep_0029.html), [RFC 6817](https://datatracker.ietf.org/doc/html/rfc6817)), as in libutp. It uses the one-way delay samples carried in every uTP header to keep the queuing delay it adds near 100 ms and backs off when other traffic fills the queue, so a torrent does not slow down the rest of the network. On an idle link it is as fast as CUBIC.

```rust
use librqbit_utp::{CongestionConfig, CongestionControllerKind, SocketOpts};

let opts = SocketOpts {
    congestion: CongestionConfig {
        kind: CongestionControllerKind::Ledbat,
        ..Default::default()
    },
    ..Default::default()
};
```

## Run example

This will run the example server and client talking to each other

```
RUST_LOG=info cargo run --example echo
```
