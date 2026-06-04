# exch

`exch` is the start of a Rust mock exchange: a standard central limit order book with
some market-data flavor from Kraken and Nasdaq.

## Shape

- `exchange_core`: deterministic matching engine and book event model.
- `exchange_server`: dependency-free local TCP gateway for early experiments.
- Future `exchange_api`: public HTTP/WebSocket API for placing orders and reading the book.
- Future `exchange_gateway`: local-network UDP multicast publisher for exchange-style feeds.
- `exchange_lean`: Lean model of the core invariants, plus a bridge from Rust tests to
  the verified model where practical.

## Product Direction

The first venue should be small and boring on purpose:

- one equities/currency venue;
- integer prices and quantities;
- limit order entry, cancellation, and book snapshots;
- market-data book feed that includes executions directly, Nasdaq-style;
- checksum snapshots inspired by Kraken so clients can detect missed or corrupt updates.

Later venues can add separate fee schedules, asset lists, matching policies, and feed formats.

## Local Experiments

Run the local line-protocol server:

```powershell
cargo run -p exchange_server
```

See `docs/protocol.md` for the command format. The first gateway is intentionally not WebSocket or
HTTP: it is a tiny harness for spinning up a configurable venue, placing orders, canceling orders,
and reading the whole book with a checksum.

Run the first single-bin simulator/benchmark:

```powershell
cargo run --release -p exchange_sim -- --commands 1000000 --traders 1000 --feed-subscribers 1
```

See `docs/benchmarking.md` for what it measures.

## Configuration Philosophy

Venue topology should be configurable without changing matching code:

- star graphs for equities-style venues where one quote asset, such as fake USD, intermediates
  all symbols;
- complete or sparse currency graphs where cycles are natural;
- deterministic generated graphs for experiments across many venue shapes.

Prices and quantities are integer ticks/lots. Asset-specific decimal display is a presentation
concern; matching should avoid floating point.

## Deployment Tracks

### Public API

Start with a low-cost deployment that is difficult to accidentally turn into a large bill:

- containerized Rust service;
- strict request limits;
- no external matching dependencies;
- small fixed universe of instruments;
- read-only market-data endpoints plus authenticated order entry;
- explicit cloud budget alarms before public exposure.

No website is needed for the first public version. The minimum useful surface is an API for
placing orders, canceling orders, reading snapshots, and streaming book events.

### Local Exchange Lab

The local-network path should model real exchange distribution:

- order entry over TCP or HTTP at first;
- sequenced book feed over UDP multicast;
- periodic snapshots and checksums;
- packet format kept simple enough to inspect with Wireshark;
- later split feeds by venue, instrument group, or event type.

## Verification Direction

The core should stay small enough to reason about formally. Good initial invariants:

- total quantity only decreases through executions or cancellations;
- every resting order appears on exactly one side and one price level;
- bids are matched against the lowest ask and asks against the highest bid;
- marketable incoming orders never rest while a matchable opposite level remains;
- emitted sequence numbers are strictly increasing.

The Rust core should prefer deterministic state transitions and plain data structures over
throughput tricks until the Lean model and conformance tests are established.
