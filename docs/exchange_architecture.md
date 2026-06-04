# Exchange Architecture Notes

These notes map real exchange patterns onto this mock exchange. The goal is not to clone any
production protocol. The goal is to copy the useful shape while keeping the implementation small.

## Two Protocol Surfaces

Real venues usually separate order entry from public market data.

Order entry is participant-facing. A participant sends new, replace, and cancel requests to the
exchange, then receives private acknowledgments, rejects, cancels, and executions for their own
orders. Nasdaq OUCH is an example of this style: it is a low-level native protocol for entering,
replacing, and canceling orders and receiving executions for the participant's orders.

Market data is observer-facing. A subscriber receives an append-only stream of public book events:
adds, cancels, replaces, executions, trading status, and similar messages. Nasdaq TotalView-ITCH is
an example of this style. It is how a client reconstructs the book, not how a client sends orders.

For this project:

- `exchange_server` can stay as the first local order-entry harness.
- A future `exchange_feed` should publish a sequenced append-only book stream.
- The same core `BookEvent` should feed both private order responses and public book updates, but
  those should be delivered over separate channels.

## UDP Multicast

UDP is a connectionless packet protocol. Unlike TCP, it does not promise delivery, ordering, or
retransmission. Multicast is a network delivery mode where one sender publishes packets to a
multicast group address and many receivers can subscribe to that group. The sender does not open a
separate connection per subscriber.

That makes UDP multicast a natural fit for exchange market data:

- the same packet can reach many subscribers;
- a slow subscriber does not directly slow the publisher;
- packet loss is visible through sequence gaps;
- recovery can be handled separately through replay or snapshot services.

It is not a good fit for order entry. Order entry needs private, authenticated, reliable,
participant-specific responses. That is why the local project should model:

- order entry as point-to-point TCP first;
- market data as TCP fanout first;
- later market data as UDP multicast plus snapshot/replay.

## Sequencing and Fairness

Matching for a single instrument should be serial. There is one authoritative order of accepted
commands, and the book applies them one at a time. That is what gives deterministic price-time
priority and makes the system verifiable.

This does imply a maximum throughput for a matching lane. Real exchanges scale by reducing work per
message, using native binary protocols, partitioning independent symbols across matching engines,
keeping risk checks fast, and making outbound publication efficient. They do not make two orders
race through the same book in parallel and then hope price-time priority survives.

For this project:

- start with one serial command queue per venue;
- later move to one serial command queue per instrument or instrument partition;
- stamp every accepted command with a monotonically increasing sequence number;
- derive book-feed sequence numbers from committed state changes, not from network arrival alone.

Network arrival is not the same thing as fairness. The exchange chooses a concrete acceptance point:
for example, the order in which a gateway reads valid messages and submits them to the sequencer.
That acceptance order is the order the matching engine sees.

## Pre-Trade Risk and Negative Capital

A trader can absolutely have stale local state. They may send an order after earlier executions
have happened but before their client has processed the private execution report or public market
data update.

The exchange should not trust the trader's local book or local capital calculation. The exchange,
broker, clearing layer, or a pre-trade risk gateway keeps authoritative account/risk state and
checks each incoming order before it reaches matching.

For this project, model risk checks as a pre-matcher stage:

```text
network read -> decode -> authenticate -> risk/limits -> sequencer -> matcher -> private/public events
```

If an order would exceed buying power, position limits, max order size, or other configured limits,
it should be rejected before matching. The trader later receives a private reject. Public market
data subscribers see nothing because the order never touched the book.

Outstanding order notional is usually part of the risk calculation. A simple cash account model can
reserve buying power when a buy order is accepted, release it on cancel, and convert it into a
position/cash change on execution. A margin or broker-sponsored access model can be more complex,
but the same idea holds: risk state must include live orders, not only completed trades.

In US equities market access, SEC Rule 15c3-5 requires broker-dealers with market access to maintain
risk management controls reasonably designed to systematically limit financial exposure, including
pre-set credit or capital thresholds. We should copy that design shape, even though this project is
a mock exchange and not legal/compliance software.

## Fixed-Point Integers

Prices and quantities should not use floating point.

Use fixed-point integer values:

- `raw`: the integer stored in the message and book;
- `scale`: the number of decimal places implied for display/parsing;
- `tick`: the allowed increment in raw units;
- `lot`: the allowed quantity increment in raw units.

For example, a price displayed as `1.0010` with scale `4` is stored as raw integer `10010`.
The next tick might be `10011`, not `2.0000`. This keeps matching exact while still supporting
currency precision.

Each instrument should eventually define:

- base asset and quote asset;
- price scale and quantity scale;
- tick size;
- lot size;
- minimum notional value;
- allowed order types and time-in-force values.

## Simulation Plan

The first useful simulator does not need a rich public API.

Build three roles:

- exchange process: owns venue config, sequencer, books, and feed log;
- participant process: connects to order entry and sends valid or intentionally invalid orders;
- market-data process: subscribes to the feed, reconstructs the book, and verifies checksum.

Then benchmark:

- accepted commands per second;
- book events per second;
- latency from accepted command to emitted book event;
- rejected commands per second for validation pressure;
- replay speed for rebuilding a book from the feed log.

This keeps the early project focused on the thing that matters most: accurate, timely book state.
