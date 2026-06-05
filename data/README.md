# Local Data And Artifacts

This directory is the local lab notebook for research inputs and generated simulation outputs.

Tracked files should be small curated notes, schemas, or reproducible recipes. Raw downloads and
generated visualizations are intentionally ignored so we do not accidentally commit bulky data,
licensed market data, private captures, or one-off experiment output.

Ignored local paths:

```text
data/research/raw/
data/artifacts/
```

Useful commands:

```powershell
cargo run -p exchange_research -- sources
cargo run -p exchange_research -- profile global-lob
cargo run -p exchange_sim -- --world shock-demo --commands 240 --traders 40 --feed-subscribers 1 --visualization data/artifacts/visualizations/shock-demo.html
```

