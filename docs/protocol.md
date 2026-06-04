# Local Line Protocol

`exchange_server` exposes two deliberately small TCP line protocols for local experiments:

- order entry on `127.0.0.1:7001`;
- market data on `127.0.0.1:7002`.

Start the server:

```powershell
cargo run -p exchange_server
```

Override with `EXCH_ORDER_ADDR` and `EXCH_FEED_ADDR`.

Each command is one line. Each response is one line.

## Order Entry Commands

```text
help
instruments
book <instrument_id> [depth]
order <instrument_id> <order_id> <account_id> <buy|sell> <price> <quantity>
cancel <instrument_id> <order_id>
```

Order-entry responses are private to the client that sent the command. They include accepts,
rejects, cancels, and executions for that connection's commands.

## Market Data Commands

```text
help
subscribe <instrument_id> [depth]
```

Market-data clients receive:

```text
snapshot instrument=<instrument_id> seq=<seq> checksum=<checksum> bids=<levels> asks=<levels>
event instrument=<instrument_id> <public-book-event>
```

Accepted and rejected messages are not public feed events. Resting orders, cancels, and executions
are public because they change the visible book.

## Example Cycle

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
book feed should tell clients when book updates were caused by trades. A feed subscriber receives
the same public execution as an `event`.

This protocol is not the final public API. It is a local test harness that keeps the first server
dependency-free and easy to drive from scripts, terminals, and property tests.
