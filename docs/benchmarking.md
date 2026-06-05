# Benchmarking

`exchange_sim` is the first local load generator.

Run:

```powershell
cargo run --release -p exchange_sim -- --commands 1000000 --traders 1000 --feed-subscribers 1
```

The default world measures a single-bin, in-process path:

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

## Generated Worlds

The simulator also has a first multi-venue world:

```powershell
cargo run --release -p exchange_sim -- --world global-lob --commands 1000000 --traders 1000 --feed-subscribers 1
```

`global-lob` is a rough shape model of the global limit-order-book landscape, not a factual clone of
specific real exchanges. It creates a lopsided universe with a few large equity-style venues, more
regional venues, many local/specialist venues, several large crypto-style venues, and a crypto long
tail. The first generated shape is:

```text
4 equity-global venues, 500 instruments each, high command weight
16 equity-regional venues, 120 instruments each, medium command weight
32 equity-local venues, 25 instruments each, low command weight
12 crypto-major venues, 350 instruments each, medium-high command weight
32 crypto-tail venues, 40 instruments each, low command weight
```

That yields 96 venues and 10,200 instruments. The point is to make venue count, symbol count, and
activity distribution uneven enough to support later experiments with routing, stale feeds,
latency perturbations, trader population mixes, and cross-venue arbitrage.

In this world, `--feed-subscribers` currently creates omniscient trader market-data views. Each such
trader subscribes to every generated venue/instrument edge and receives public events tagged with
the source edge. Later worlds should make this information profile configurable instead of assuming
full coverage.

Generate a clickable standalone viewer for a loaded run:

```powershell
cargo run -p exchange_sim -- --world global-lob --commands 1000 --traders 40 --feed-subscribers 1 --visualization data/artifacts/visualizations/global-lob-viewer.html
```

Open `data/artifacts/visualizations/global-lob-viewer.html` in a browser. The viewer starts with
venue nodes, lets you click into a venue's asset graph, then click an edge to inspect and play the
recorded book states for that edge.

That loaded-run viewer is generated after the simulation finishes. For a live local demo, run:

```powershell
cargo run -p exchange_sim -- --world live-demo --live 127.0.0.1:8088
```

Then open `http://127.0.0.1:8088`. The live runner serves a small browser UI and a `/state` JSON
endpoint from the same Rust process that owns the live books. It uses four venues and 24 books with
high-activity synthetic order flow so the event tape and book ladders move continuously.

Current runtime boundary: `exchange_sim` live books are real `Venue`/`OrderBook` instances, but
they are not yet the same books served by `exchange_ws`. External connector/order-entry scripts
still connect to `exchange_ws`, which owns its own venue. The next integration step is to extract a
shared world runtime so WebSocket order entry, market-data clients, and the simulator UI can all
attach to the same venue set.

## Capacity Notes

Book count is not the only memory driver. The current generated world stores venue configs, symbol
strings, accounts/balances, market-data views, and active resting orders in addition to empty or
active order books.

One local release-mode run:

```powershell
/usr/bin/time -v target/release/exchange_sim --world global-lob --commands 10000 --traders 100 --feed-subscribers 0
```

reported `venues=96`, `instruments=10200`, about 158k commands/sec, and maximum resident set size
of 157,108 KB. Treat this as a rough first read: it says 10k books are comfortable locally, but it
does not yet tell us the hard ceiling. A dedicated capacity command should sweep venue/book/account
counts and report memory per book and per seeded account.

## Shock Demo

For a small visual scenario instead of a pure throughput run:

```powershell
cargo run -p exchange_sim -- --world shock-demo --commands 160 --traders 40 --feed-subscribers 1 --visualization data/artifacts/visualizations/shock-demo.html
```

`shock-demo` seeds one book with bid/ask depth, runs background order flow, injects one large buy
shock halfway through the run, and then lets ask-side replenishment orders arrive. The optional
`--visualization` path writes a standalone HTML/SVG chart with midprice, spread, and top-five
bid/ask depth over the run.

On one local release-mode run after adding balance reservations and fee accounting, with 100,000
commands, 1,000 traders, and 1 feed subscriber, the simulator reported about 809k commands/sec
overall, 1.09M matcher/risk commands/sec, and 200 ns p50 in-process feed publication latency.
Treat this as a rough baseline only.
