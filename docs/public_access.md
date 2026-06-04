# Public Access Plan

The first public venue should be invite-only.

## Access Model

Use manually issued API keys at first:

1. Operator generates a random key.
2. Operator maps that key to an account id.
3. User receives the key out-of-band.
4. User connects to the order-entry WebSocket.
5. User sends `auth <api_key>` before trading.

Market data can be public or key-gated. To control cloud bills, start with key-gated market data too
or a very small unauthenticated connection limit.

## Local Key File

Real keys must stay out of the public repository.

Use:

```text
config/local/api_keys.txt
```

Format:

```text
key account_id label
```

Example:

```text
dev-key-100 100 demo-account-100
```

`config/local/` is ignored by git.

The checked-in example keys are only for local development. A public deployment must use random
unguessable keys in `config/local/api_keys.txt` or another private secret store.

## Public Deployment Limits

Before exposing the service:

- require API keys for order entry;
- rate-limit per key and per IP;
- cap open connections;
- cap open orders per account;
- cap order notional;
- cap market-data subscriptions per key;
- disconnect clients that fall behind feed queues;
- run with a small fake venue and fake balances;
- set cloud budget alerts.

## Key Distribution

For the first version, do not build self-serve signup.

Use a manual flow:

```text
friend asks for access -> operator creates account/key -> operator sends key privately
```

Later, a small admin API can create/revoke keys, but it should not be exposed until auth, logging,
and rate limits are stable.
