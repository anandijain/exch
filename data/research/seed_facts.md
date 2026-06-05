# Simulation Seed Facts

Snapshot date: 2026-06-05

These notes are not paid market data. They are public calibration points for choosing synthetic
venue counts, graph shapes, activity weights, and liquidity tiers.

## Equity Venues

- WFE dashboard data for December 2024 reports global equity market capitalisation of
  120,809,774.84 USD millions and 53,601 listed companies across its covered exchange data.
- WFE says its covered exchanges were home to over 55,000 listed companies and over $111 trillion
  in market capitalisation at end-2023.
- A current public exchange market-cap ranking lists 51 stock exchanges, 31,707 listed companies,
  and $165.47 trillion combined market cap. Treat this as a rough public ranking seed, not an
  authoritative replacement for WFE statistics.

Simulation implication: equity-style universes should be strongly concentrated. A few venues should
dominate market cap/activity, with a meaningful regional/local tail.

Sources:

- https://focus.world-exchanges.org/issue/december-2024/dashboard
- https://www.world-exchanges.org/news/articles/wfe-data-trading-value-and-volumes-surge-investors-flock-markets
- https://marketcap.company/stock-exchanges-by-market-cap/

## Crypto Venues

- CoinMarketCap currently reports tracking 242 spot exchanges.
- CoinMarketCap ranks spot exchanges using traffic, liquidity, trading volume, and confidence in
  reported volume.
- Its current top spot exchange table is highly concentrated, with Binance materially larger than
  the next listed venues by reported 24h volume.

Simulation implication: crypto-like universes should have a long venue tail, many listed markets,
and noisier venue-quality assumptions than regulated equity venues.

Sources:

- https://coinmarketcap.com/rankings/exchanges/
- https://support.coinmarketcap.com/hc/en-us/articles/360043304571-FAQ-on-Exchanges
- https://support.coinmarketcap.com/hc/en-us/articles/360043289451-Ranking-Exchanges-by-Liquidity

## FX Graphs

- BIS Triennial Survey data is the best public seed for OTC FX turnover.
- The BIS tables include foreign exchange turnover by instrument, country, and currency.
- The 2025 BIS FX publication notes that the euro share declined to 28.9% from 30.6% in 2022; this
  is useful as one anchor for currency graph weights.

Simulation implication: FX should not be modeled as one central exchange. It should be a graph of
currency pairs across dealer/ECN-style venues, with USD-heavy activity, regional latency, and sparse
access/information profiles.

Sources:

- https://www.bis.org/statistics/rpfx25_fx.htm
- https://data.bis.org/topics/DER/tables-and-dashboards
- https://www.bis.org/statistics/rpfx25_fx.pdf

## Book-Depth Seeds

Full real depth-of-book and order-flow data is usually paid or venue-restricted. The simulator
should generate synthetic books from calibrated distributions instead of depending on proprietary
feeds.

First-pass synthetic depth parameters to research:

- spread in ticks by venue tier and symbol liquidity tier;
- top-of-book quantity;
- decay rate from best level to deeper levels;
- imbalance distribution;
- order arrival, cancel, and replacement rates;
- depth response after shocks;
- different tick/lot/min-notional rules per venue.

Simulation implication: the local lab should store seed profiles like `large_equity`,
`regional_equity`, `major_crypto`, `tail_crypto`, and `major_fx_pair`, then generate deterministic
initial books from those profiles.
