# Native inbound notifications

`POST /webhooks/people-native` receives signed directory-change notifications and records their receipt for an operator to review. An operator still uses **Synchronise now** to refresh the directory.

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

| Connection policy field | Rule |
| --- | --- |
| `webhook_secret` | Encrypted, nullable; a nonblank UTF-8 secret of 32–4096 bytes, with no reveal path |
| `webhook_enabled` | Default `false`; enabling requires a stored secret |
| `webhook_max_skew_seconds` | Integer, 1–86400 seconds, default 300 |

These values belong to one connection and its platform company. Changing them requires `people-connector.connections.manage` and Core Company reach. Clearing the secret requires disabled intake in the same save. Summaries expose only whether the secret is stored, the switch, the clock allowance and the last receipt time.

Verification requires an active tenant, a live writable company in that tenant, an enabled native connection, enabled intake and a timestamp within the configured clock allowance. Handling rechecks these conditions and the signature under the tenant, company and connection row locks. Provider or mapping changes clear all three policy fields. Removing a connection clears its policy and receipt history.

`Connector::put_webhook_settings(scope, platform_company_id, WebhookSettingsUpdate)` saves policy for the connections page. Its `enabled`, `secret` and `max_skew_seconds` fields are optional; omitted values stay unchanged. An empty secret clears it. The facade checks the manager's authority again under the company lock and checks the stored Workforce mapping through the directory port before saving.

`Connector::webhook_summary(scope, platform_company_id)` returns `WebhookSummary`: `enabled`, `secret_stored`, `max_skew_seconds` and `last_received_at`. It follows `status`'s Workforce read policy. A disconnected company has disabled intake, no secret and no receipt. The summary never decrypts a secret.

## Replay and audit

Migration `people-connector-connector-003-webhooks` owns `people_connector_webhook_nonces` and `people_connector_webhook_deliveries`. Both are tenant-owned and cascade from the connection. Only SHA-256 hashes of keys and raw body bytes are stored; raw keys, signatures and payloads are never stored. Named unique indexes enforce one nonce and one delivery key per connection. Composite foreign keys refuse receipts attributed to a different tenant than their connection.

A used nonce always refuses. A retry needs a new nonce and current timestamp. The same delivery key with identical raw body bytes acknowledges without a second receipt; different bytes refuse. Decide these outcomes while holding the connection row lock. Nonce consumption, first receipt and audit commit atomically through `MutationBoundary`; a refusal rolls back all of them.

The scoped `people-connector.webhook` action records `received` or `duplicate`, `platform_company_id` and guest actor 0. The mutation boundary also records the receipt transaction as `people-connector.webhook.receipt`. The host separately owns uniform HTTP responses, limits and unscoped refusal audit. A notification records receipt only. It never reads the People directory, enqueues directory work or borrows a login account to synchronise.

## Bilibi storage deviation

Bilibi's webhook callback has no login scope. `Settings::resolve_many/get` require one, so the Bilimbi Settings keys `people-connector.webhook.secret`, `.enabled` and `.max_skew_seconds` are deliberately not registered or used. Their values live on the Connector's own connection row instead. There is no machine principal, persisted login account, or Platform change.

`Settings::seal_owned/open_owned` encrypts the signing secret with `BILIBI_SETTINGS_KEY`, bound to the module's webhook owner, tenant and connection ID. Storing or verifying needs that installation key. Disabling or clearing ciphertext needs no key. Audit redacts `webhook_secret`, including its ciphertext; receipts and summaries never contain it.

The handler resolves policy by tenant and platform company from its own connection. It checks tenant liveness and uses Core Company's `organisation::require_writable_in` for the company lock. It never assumes the two company identity axes are equal. Only actor-authorized configuration and synchronisation read current Workforce identity; changing that stored mapping clears intake authority.

## Walkthrough

Set `DATABASE_URL` to an empty disposable database connection string, then run the signed-receipt walkthrough with an ephemeral installation key:

```sh
export BILIBI_SETTINGS_KEY=$(openssl rand -hex 32)
cargo run -p bilibi-people-connector-walkthrough --example webhooks
```

The proof installs the Platform fixture, configures a connection through the authorized facade with deliberately unequal company axes, and drives the registered webhook service. It covers header and body validation, signed bytes, skew, replay, parallel retries, rotation between callbacks, audit rollback, redaction and connection cleanup. Its fixture company port is used only by operator calls; it proves receipt intake never calls it. No fixture adapter is contributed to the running app.

To include the HTTP host, run `bilibi-web run` on the same database and key with your own plain HTTP port, then set `WEBHOOK_PROOF_PORT` to that port when running the walkthrough. It proves 202 for the first delivery and an identical retry with a fresh nonce, and 403 for replay, tampering and conflicting bytes.
