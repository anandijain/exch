# Risk, Fees, and Exchange Revenue

The first economics model is intentionally small.

## Instrument Constraints

Each instrument carries:

- price tick;
- quantity step;
- minimum notional;
- maker fee in basis points;
- taker fee in basis points.

Orders are rejected before matching if they violate tick, lot, minimum-notional, or balance checks.

## Maker/Taker Fees

The resting order is the maker. The incoming order is the taker.

Fees are charged in quote asset units:

```text
fee = trade_notional * fee_bps / 10000
```

Default generated venues currently use:

- maker fee: 0 bps;
- taker fee: 10 bps.

Exchange revenue is accumulated per asset.

## Balance and Reservation Model

Balances have two components:

- available;
- reserved.

Buy orders reserve quote notional at the order limit price plus taker fee. Sell orders reserve base
quantity. On execution, reserved funds are spent and fees move into exchange revenue. On cancel,
remaining reservation is released. If a buy executes below its limit price, the unused reservation is
released back to available balance.

This is the first proof target for risk:

```text
available >= 0
reserved >= 0
accepted orders reserve enough asset before they can rest or execute
matching never spends more than reserved
```

Because the Rust model uses unsigned integers, underflow panics in debug builds, but the real proof
should be stronger: every transition preserves nonnegative available and reserved balances.

## Rate Limits

The local TCP and WebSocket order-entry gateways currently enforce a simple per-connection limit of
100 order-entry commands per second. This is not a production limiter, but it gives the simulator a
place to model spam pressure.

Public deployment should add:

- per-API-key limits;
- per-IP connection limits;
- max open orders per account;
- max order notional;
- max messages per second;
- disconnect or cool-down after repeated rejects.
