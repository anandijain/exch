# Local Line Protocol

`exchange_server` exposes a deliberately small TCP line protocol for local experiments.

Start the server:

```powershell
cargo run -p exchange_server
```

By default it listens on `127.0.0.1:7001`. Override with `EXCH_ADDR`.

Each command is one line. Each response is one line.

## Commands

```text
help
instruments
book <instrument_id> [depth]
order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity>
cancel <instrument_id> <order_id>
```

## Examples

```text
instruments
book 0 10
order 0 1 100 buy 10000 25
order 0 2 101 sell 10000 10
book 0 10
cancel 0 1
```

The book response includes the current sequence number, checksum, bid levels, and ask levels.
Executions are emitted directly in the order response events, matching the project goal that the
book feed should tell clients when book updates were caused by trades.

This protocol is not the final public API. It is a local test harness that keeps the first server
dependency-free and easy to drive from scripts, terminals, and property tests.
