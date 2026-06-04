# Benchmarking

`exchange_sim` is the first local load generator.

Run:

```powershell
cargo run --release -p exchange_sim -- --commands 1000000 --traders 1000 --feed-subscribers 1
```

It currently measures a single-bin, in-process path:

```text
synthetic trader command -> Venue::submit_limit -> public event formatting/fanout
```

This intentionally excludes:

- TCP parsing;
- TLS;
- authentication;
- kernel network buffers;
- real subscriber backpressure;
- persistence/replay.

So the result is a core baseline, not a public internet capacity number.

Useful reported values:

- `commands_per_sec`: full synthetic loop throughput;
- `matcher_commands_per_sec`: time spent inside `Venue::submit_limit`;
- `public_events_per_sec`: events emitted into the in-process feed sink;
- `feed_latency_ns_*`: time from matcher return to public feed sink publication.

On one local release-mode run after adding balance reservations and fee accounting, with 100,000
commands, 1,000 traders, and 1 feed subscriber, the simulator reported about 809k commands/sec
overall, 1.09M matcher/risk commands/sec, and 200 ns p50 in-process feed publication latency.
Treat this as a rough baseline only.
