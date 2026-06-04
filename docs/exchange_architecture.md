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
