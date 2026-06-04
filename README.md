# exch

`exch` is the start of a Rust mock exchange: a standard central limit order book with
some market-data flavor from Kraken and Nasdaq.

## Shape

- `exchange_core`: deterministic matching engine and book event model.
- Future `exchange_api`: public HTTP/WebSocket API for placing orders and reading the book.
- Future `exchange_gateway`: local-network UDP multicast publisher for exchange-style feeds.
- Future `exchange_lean`: Lean model of the core invariants, plus a bridge from Rust tests to
  the verified model where practical.

## Product Direction

The first venue should be small and boring on purpose:

- one equities/currency venue;
- integer prices and quantities;
- limit order entry, cancellation, and book snapshots;
- market-data book feed that includes executions directly, Nasdaq-style;
- checksum snapshots inspired by Kraken so clients can detect missed or corrupt updates.

Later venues can add separate fee schedules, asset lists, matching policies, and feed formats.

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
