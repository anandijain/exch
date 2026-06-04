# Implementation Plan

## Next Order-Entry Work

Keep order entry TCP-based, but make WebSocket the main public-client protocol.

1. Add sessions.
   A client connects, receives a session id, and sends heartbeats. The server closes idle sessions.

2. Add authentication.
   Local mode can use static fake API keys. Public mode should use generated API keys stored in a
   config file or database outside the public repo.

3. Add idempotent client order ids.
   If a client disconnects and retries the same order id, the exchange should return the previous
   result instead of duplicating the order.

4. Add pre-trade risk.
   Start with cash balances, positions, open-order reservations, max order notional, and max open
   orders. Reject before sequencing into the matcher.

5. Add replace.
   Most real participants need cancel/replace. Implement it as an atomic command in the matching
   lane, even if internally it removes and re-adds.

## Next Market-Data Work

Keep public internet market data WebSocket-style. Add UDP multicast only for the LAN lab.

1. Add an append-only feed log.
   Persist every public feed event with instrument id, partition id, sequence, and checksum.

2. Add replay.
   A client that detects a sequence gap can request events after a known sequence.

3. Add Kraken-style subscriptions.
   Allow clients to subscribe to selected instruments and depths.

4. Add ITCH-style channels.
   Publish many instruments on one partition/feed channel with compact instrument ids.

5. Add LAN multicast.
   Send feed-channel packets over UDP multicast. Clients detect gaps and recover through TCP replay
   or a fresh snapshot.

## Partitioning Rule

Do not build a global sequencer first.

Use one serial matching lane per partition. A partition owns one or more instruments. Inside a
partition, sequence is total. Across partitions, sequence is not globally ordered.

This is simpler, faster, and closer to how a scalable exchange should feel.

## Interesting Experiments

- Intentionally unbalanced symbol bins.
  Put one group of instruments on a hot/slow partition and another group on a quiet/fast partition.
  Then run cross-symbol or currency-cycle strategies to see whether delayed feed publication creates
  a profitable stale-information exploit.

- Single-bin throughput.
  Keep all symbols in one serial matcher and measure accepted commands per second, public feed
  events per second, and matcher-to-feed publication latency.
