# Native inbound notifications

KC3 will receive signed directory-change notifications at `POST /webhooks/people-native`. Implementation is pending the machine-read contracts described below; this document does not enable intake.

The parity reference is Bilimbi's [`connector/docs/webhooks.md`](https://github.com/BelimbingApp/b-dom-people-connector/blob/dc6e1cbeca39be8eb9211a333088fa726c09b603/connector/docs/webhooks.md) and [`Connector.Webhooks`](https://github.com/BelimbingApp/b-dom-people-connector/blob/dc6e1cbeca39be8eb9211a333088fa726c09b603/connector/lib/connector/webhooks.ex).

## Signed request contract

Each of these six headers must appear exactly once, including when names differ only by case:

| Header | Value |
| --- | --- |
| `x-people-tenant` | Positive decimal tenant ID |
| `x-people-company` | Positive decimal platform company ID |
| `x-people-timestamp` | Positive decimal Unix seconds |
| `x-people-nonce` | Unique attempt key |
| `x-people-delivery` | Stable delivery key across attempts |
| `x-people-signature` | 64 lowercase hexadecimal HMAC-SHA256 characters |

Numbers have no leading zero and at most 18 digits. Attempt and delivery keys contain 1–100 ASCII characters. They start with a letter or digit and otherwise allow letters, digits, `.`, `_`, `:` and `-`.

The signature authenticates these bytes, with one LF between lines and one LF before the exact raw body:

```text
people-native:v1
<tenant>
<platform company>
<timestamp>
<nonce>
<delivery>
<raw body>
```

The decoded body must contain only `{"event":"directory.changed"}`. Whitespace remains significant for signing and delivery identity. Decode the body only after constant-time signature verification.

## Company policy

| Setting | Rule |
| --- | --- |
| `people-connector.webhook.secret` | Company-scoped, encrypted, nullable; a nonblank UTF-8 secret of 32–4096 bytes, with no reveal path |
| `people-connector.webhook.enabled` | Company-scoped, default `false`; enabling requires a stored secret |
| `people-connector.webhook.max_skew_seconds` | Company-scoped integer, 1–86400 seconds, default 300 |

Changing these settings requires `people-connector.connections.manage` and Core Company reach. Clearing the secret requires disabled intake in the same save. Summaries expose only whether the secret is stored, the switch, the clock allowance and the last receipt time.

Verification requires a live company in the validated tenant, an enabled native connection, current Workforce mapping, enabled intake and a timestamp within the configured clock allowance. Handling must recheck these conditions and the signature under the connection row lock. Provider or mapping changes clear all three setting overrides. Removing a connection clears its settings and receipt history.

## Replay and audit

Migration 003 will own `people_connector_webhook_nonces` and `people_connector_webhook_deliveries`. Both are tenant-owned and cascade from the connection. Store SHA-256 hashes of keys and raw body bytes; never store raw keys, signatures or payloads. Named unique indexes enforce one nonce and one delivery key per connection.

A used nonce always refuses. A retry needs a new nonce and current timestamp. The same delivery key with identical raw body bytes acknowledges without a second receipt; different bytes refuse. Decide these outcomes while holding the connection row lock. Nonce consumption, first receipt and audit commit atomically through `MutationBoundary`; a refusal rolls back all of them.

The scoped `people-connector.webhook` action records `received` or `duplicate`, the platform company and guest actor 0. The host separately owns uniform HTTP responses, limits and unscoped refusal audit. A notification records receipt only. It never enqueues directory work or borrows a login account to synchronise.

## Platform needs

The Platform checked for this slice is `14a237ad37625b6c6b38738e33624833caa0d140`. Its webhook callback has no login scope. Bilimbi's verifier creates a tenant-only scope, but Bilibi's sealed `Scope` always carries an authenticated account.

Two required reads therefore have no supported machine caller:

1. `Settings::resolve_many` and `Settings::get` require `&Scope`. Company resolution calls `PartyProofs::organisation`. The signing secret cannot be read through its owner without a login scope (`apps/base/settings/src/lib.rs`).
2. `Ports::call` and `PortProvider::call` require `&Scope` (`apps/base/ports/src/lib.rs`). The required current mapping read through `people/workforce.directory` cannot be called by the webhook. The People checkout inspected here, `9dd78f00630c94eedfeb96fab7eaf6c94877878e`, also has not yet contributed that port; PW2 owns its implementation.

The proposed prerequisite is a narrowly scoped machine proof for a live tenant and platform company, an owner-authorized Settings read for this module's three definitions, and a company-identity port read that accepts the machine proof. Receipt handling can then use guest attribution without inventing a user. The existing `organisation::require_writable_in` can hold the company lock, and `MutationBoundary` can capture the guest receipt writes.

Do not work around the missing read authority by selecting a manager account, persisting a login scope, reading People tables, assuming the native company axes are equal, or copying Settings' ciphertext decoder into this Domain. Those approaches lose either the machine identity contract, current mapping validation, or owner boundaries.
