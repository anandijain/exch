# exchange_research

`exchange_research` is a small CLI for collecting real-world market-structure reference data that
can seed local simulation universes.

It is intentionally separate from `exchange_sim`: this crate studies real exchange shapes, while
the simulator generates synthetic worlds from those lessons.

## Commands

```powershell
cargo run -p exchange_research -- sources
cargo run -p exchange_research -- profile global-lob
cargo run -p exchange_research -- fetch bis-fx-2025-annex data/research/raw/bis-fx-2025-annex.pdf
```

`fetch` shells out to `curl` so the crate can stay dependency-light while the source list is still
changing.

## Research Questions

- How many venues should a plausible synthetic world have?
- How concentrated is command flow by venue tier?
- Which symbols are stars, dense graphs, or sparse graphs?
- How different are tick sizes, lot sizes, fees, rate limits, and market-data depth across venues?
- How stale is each trader's view of each venue from each simulated location?
- When is a union graph useful as a trader's derived view, and when does it hide the venue-local
  queue, fee, and latency realities that matter?
