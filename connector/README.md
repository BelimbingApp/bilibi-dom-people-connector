# Connector

`people_connector/connector` owns the integration contracts, each company's
provider connection, directory synchronisation into Connector-owned
projections, private directory file exchange, and backup and recovery. Inbound notifications and
operations follow in later slices. This README is the owner contract; the Bilimbi
source it ports is
[`connector/docs/README.md`](https://github.com/BelimbingApp/b-dom-people-connector/blob/main/connector/docs/README.md).

Connections, synchronisation and file exchange are available through `Connector`. The Files page is at `/integrations/people/files`. The Backups page is at `/integrations/people/backups`. The connections page (KC2), inbound notifications (KC3), and operations (KC5) follow separately.

## Contracts

`Capability`, `Provider` and `Registry` (`contracts.rs`) describe
provider-declared read and write ports. A capability is one of the twelve
keys of `CAPABILITY_KEYS` (`company_directory`, `employee_directory`,
`organization_directory`, `manager_hierarchy`, `user_directory`, `payroll`,
`attendance`, `leave`, `claims`, `training`, `documents`,
`single_sign_on`) with an explicit `Direction`, so a read never implies a
write; any other key is `InvalidCapability`. A provider descriptor has an
id (lowercase segments joined by `.` or `-`, at most 100 bytes), a name, a
contract version `1.x.y` and a distinct set of declarations, else
`InvalidProvider`; it also says whether a connection needs a `Credential`
(`None` or `Secret`), never the secret itself. A registry is immutable:
`register` returns a new one and refuses a `DuplicateProvider`; `fetch` and
`permit` answer `Unsupported` for an unregistered provider or an undeclared
capability and direction.

`Providers::installed()` is the catalog operators choose from: only the
co-located native People provider, `people.native` ("People (this
installation)", contract `1.0.0`), which reads through the People directory
port, declares `company_directory` and `employee_directory` reads and needs
no credential. It declares `organization_directory` once the native adapter
serves positions (KA1). Remote and third-party providers are not offered.

## Company axes

Every call takes the sealed `bilibi_tenancy::Scope` and an explicit platform
company id: a live Core Company in that tenant. People Workforce validates
that company through the directory port and answers the separate workforce
company identity (`workforce_source_id` plus `workforce_company_id`). Stale
or unavailable identity is refused as `NotCurrent(freshness)`, never used as
a mapping. A stored connection records both axes. When Workforce later maps
the platform company differently, `status` refuses with `MappingChanged`
until an operator chooses the provider again, which records the current
mapping and leaves the connection disabled. A login actor is never a
company or employee identity.

## The People directory port

The connector depends on `base/ports`, never on a People crate, and calls
`people/workforce.directory` (`people.rs`) with its own serde types. The
shape it reads, which People Workforce documents as the provider:

Request, the company's native identity:

```json
{ "read": "company", "company_id": 1 }
```

Answer, Workforce's `Company` as it serializes it, with its freshness:

```json
{
  "value": {
    "reference": { "source_id": "people/native", "type": "company", "stable_id": "1" },
    "platform_company_id": 1,
    "workforce_company_id": 1,
    "name": "Company 001",
    "code": "C001"
  },
  "freshness": "current"
}
```

`freshness` is `"current"`, `{ "stale": "<RFC 3339 instant>" }` or
`{ "unavailable": "<reason>" }`; `value` is `null` only when unavailable.
The provider refuses a company the scope cannot see with `NotFound` and an
actor it does not serve with `Forbidden`; it needs no capability for this
read, as Workforce's `company` does. While no mounted module answers the
port, every call that needs workforce identity is `PeopleUnavailable`, and
`Connector::people_available` tells a page to say so instead of offering
controls. The employee and position pages of the port (KA1) extend this
request with `"read": "employees"` and `"read": "positions"`.

## Storage

The module owns fresh Bilibi-only tables. `people_connector_connections`
(migration `people-connector-connector-001-connections`) holds connections:
a platform company has at most one, one workforce company identity backs at
most one platform company in a tenant, and the platform company is a
company of the row's tenant; the database enforces all three. Every read
binds the caller's tenant beside the company.

A provider credential is the encrypted company-scoped Base Setting
`people-connector.connection.credential` (`CREDENTIAL_KEY`): no editor on
the generic settings screen and no reveal path. The facade stores or clears
it and reports only whether one exists (`Status::credential_stored`);
changing provider or removing the connection deletes it. Storing one needs
`BILIBI_SETTINGS_KEY`; clearing ciphertext does not. Every table and settings
write goes through `MutationBoundary` and is recorded by Base Audit as a
`people_connector_connection` change.

## Public API

Resolve `Connector` from the registry as
`people_connector/connector.connector` (`SERVICE`).

| Function | Capability | Does |
| --- | --- | --- |
| `status` | none | The company's `Status`: `Disconnected` without a stored connection, else `Disabled` or `Enabled`, both company axes, the provider and whether a credential is stored; refuses `NotCurrent`, `PeopleUnavailable`, `MappingChanged`, `NotFound` |
| `configure_connection` | `people-connector.connections.manage` | Chooses the provider and records the current mapping; a new or changed provider starts disabled; changing provider discards the previous credential; a changed provider or mapping disables the connection; refuses `Unsupported`, `WorkforceCompanyTaken` |
| `put_credential` | manage | Stores a secret for a connection whose provider declares one: non-blank, at most 4,096 bytes; refuses `Disconnected`, `CredentialNotRequired`, `InvalidCredential` |
| `clear_credential` | manage | Clears the stored secret and disables a connection that needs one |
| `set_enabled` | manage | Enables or disables; enabling needs a current mapping, an installed provider and, when it declares one, a stored credential (`CredentialMissing`) |
| `remove_connection` | manage | Removes the connection and its credential; a company without one is `Disconnected` |
| `registry`, `adapters`, `people_available` | | The installed providers, the adapters mounted modules contribute, and whether the People port is mounted |

Every write needs the scope's signed-in actor to hold
`people-connector.connections.manage` with Core Company reach to the target
(`organisation::authorize_target`: the actor's own company, or a same-tenant
sibling only with tenant-wide company reach); a system principal cannot
reach the facade. Each write holds the company's writable lock (an archived
company is `CompanyArchived`) and the connection row `FOR UPDATE`.
`ConnectorError`'s `Display` is the sentence Bilimbi's connections page
shows for each refusal, so a page renders the error itself.

## Read-port seam

An adapter implements `ReadPort` (`adapters.rs`). It receives a
`PortAuthorization` that only the Connector builds (the scope, both company
axes, provider, capability, direction) and a `PortRequest` (`Bootstrap` or
`Changes`, the resume cursor, the page cursor and the page limit), and
answers a `Page` (entries, cursors, the watermark `as_of`, `Freshness`,
`snapshot`) or a `ReadRefusal` whose fixed code is all the Connector records.
`WritePort` is a neutral placeholder; no writer is activated.

The Connector also supplies a `PortContext` for each read or write call. It carries the caller's database client (`PortClient`) and port directory (`Ports`). For example, the native adapter uses these to ask `people/workforce.directory` about a company on the same connection or transaction. Use this context for calls to People; adapters keep no global service handle or database pool.

A mounted adapter module (the Connector cannot depend on it) contributes one
`Adapter { provider_id, port }` under the `ADAPTERS` catalog
(`people_connector/connector.adapters`) with `inventory`, keyed by the
provider it serves, from a module that depends on
`people_connector/connector`. This replaces Bilimbi's `Adapters.register/2`
at application start: Bilibi validates the catalog at boot and the facade
reads it once (`Adapters::from_catalog`) when its service is installed.
Until an adapter serves a provider, a pass for it is refused with
`ConnectorError::AdapterUnavailable`. `people_connector/native_people_adapter`
contributes the native provider's adapter (KA1).

## Menu and capabilities

The Authz domain is `people-connector` with `people-connector.connections.view`
and `people-connector.connections.manage`; Bilimbi grants neither to a
contributed role, so an administrator grants them. The menu leaf
**Administration › System › Integrations › People connections**
(`admin.system.integrations.people-connections`, order 50, route
`/integrations/people/connections`) carries the view capability; the route
enforces it again when the page lands (KC2).

## Synchronisation

An enabled connection can copy its provider's directory into Connector-owned records. It never changes People records.

For example, synchronise a company with a key minted for that request:

```rust,ignore
let run = connector.synchronise(scope, company_id, "directory:request-1", SyncOptions::default()).await?;
let directory = connector.workforce(scope, company_id).await?;
let summary = connector.sync_summary(scope, company_id).await?;
```

Repeating the key returns the recorded `SyncRun`, including a running or unsuccessful outcome. It does not read the adapter again. Use a new key for a new attempt. Keys contain 1–100 ASCII letters, digits, dots, underscores, colons or hyphens, beginning with a letter or digit. `SyncOptions { full: true, ..Default::default() }` requests a complete read. `expected_checkpoint_version` fences recovery requests; zero means no checkpoint.

The first pass is `bootstrap`. Later passes are `incremental`, with adapter requests using `Pass::Changes` and the last resume cursor. The employee-directory stream contains company and employee records. When the provider declares `organization_directory`, the engine also reads that stream. Every page must be current and obey the page limit and cursor contract. A repeated cursor, adapter refusal or panic records a fixed failure code. No adapter error text is stored.

**All streams are read before any records or checkpoints change.** The engine then locks the company and connection again, rechecks authority, mapping, connection identity and checkpoint, and applies the pass in one audited transaction. A concurrent connection edit refuses the result; a moved checkpoint fails it. A second pass cannot run while one is in flight. Both a unique partial database index and the connection lock enforce this.

| Outcome | Effect |
| --- | --- |
| `Succeeded` | Accepted records apply and the checkpoint advances once |
| `Stale`, `Unavailable`, `Failed` | No projection or checkpoint changes |
| `Refused` | The connection changed, or every record was refused; checkpoint stays put |
| `Unknown` | The run exceeded the company's timeout; no late result can apply |

A timed-out pass appears unknown in summaries immediately. The next synchronisation persists that outcome and opens `unknown_outcome`; an adapter read that reaches the timeout does so itself. The same key keeps returning that outcome.

### Records and issues

`Page.entries` contains JSON serialised from `WorkforceRecord` or `Deactivation`. `RecordKind` is `company`, `employee` or `position`. A record carries its source, stable ID, workforce company ID, name, code, observation time and active state. Optional employee fields are email and supervisor stable ID. Position fields are parent stable ID, positive version, vacancy, incomplete-assignment flag and up to 500 unique `AssignmentRecord` facts. Non-position records leave these organisation fields empty. Times use RFC3339 UTC instants. A deactivation has `kind`, `source_id`, `stable_id`, `observed_at` and `deactivated: true`; only changes pages may carry one.

Foreign sources, another workforce company, undeclared kinds, malformed records and unknown deactivation references become reconciliation issues. A refused identity still counts as present in a full read, so it cannot accidentally deactivate its existing record. Older observations are counted as superseded. Identical content is unchanged; assignment order does not change the content hash. A later valid record resolves its earlier refusal. If every record is refused, `feed_refused` opens and the checkpoint stays put.

A bootstrap or a changes pass whose every page declares `snapshot: true` deactivates missing company and employee records. Positions deactivate only when their complete stream fits in **one page**. An unavailable organisation stream leaves all positions untouched, applies the directory stream and opens `organisation_unavailable`. Undeclared organisation with active positions opens the same issue. A successful organisation read resolves it. Empty full reads open `empty_bootstrap`. Deactivation retains records and People history stays with People.

`resolve_issue(scope, company_id, issue_id)` marks an open issue of that company's current connection resolved. The next occurrence reopens it and increments its count. Summary lists at most 50 open issues.

### Freshness and policy

`workforce` returns `ReadResult<Vec<WorkforceRecord>>`, the Connector's copy of the People read-result contract: a current value, a stale value with its watermark, or no value with an unavailable reason. Disabled or absent connections are `disconnected`; an enabled connection without a checkpoint is `never_synchronised`. Freshness uses the oldest page watermark across all streams and the company's maximum age. `sync_summary` returns that freshness, checkpoint version and watermark, last run, open issues and policy. Both reads ask Workforce for the current company mapping first.

| Company setting | Default | Inclusive range |
| --- | --- | --- |
| `people-connector.sync.page_limit` | 250 | 1–1,000 records |
| `people-connector.sync.max_age_minutes` | 1,440 | 5–43,200 minutes |
| `people-connector.sync.run_timeout_minutes` | 30 | 1–1,440 minutes |

`put_sync_policy(scope, company_id, SyncPolicyUpdate { .. })` accepts partial updates. Every supplied field is validated before any setting changes. The shared settings editor also exposes these values to connection managers.

Every write requires `people-connector.connections.manage`, a writable company and a locked connection when one exists. `request_port` checks a provider declaration and enabled connection but always refuses: callers cannot obtain an adapter or activate writes through it. Unsupported direction or capability is `Unsupported`; a disabled or different provider is `Disconnected`; an enabled declared port is `AdapterUnavailable`. Only `synchronise` invokes an adapter.

Migration `people-connector-connector-002-sync` adds `people_connector_sync_checkpoints`, `people_connector_sync_runs`, `people_connector_workforce_records` and `people_connector_reconciliation_issues`. Their foreign keys bind both tenant and connection. A mapping or provider change clears the old records and checkpoint, preserving runs and issues. Removal audits deletion of all sync rows before deleting the connection.

## Proving a sync change

Run the database walkthrough on a new disposable database:

```sh
BILIBI_ENV=development DATABASE_URL=postgres://... cargo run -p bilibi-people-connector-walkthrough --example sync
```

The example installs the Platform fixture, then uses `Connector::from_catalog` with an explicit fixture deployment catalog. Its provider reports distinct platform and workforce company IDs. Its adapter produces current, stale, unavailable, refused and interrupted reads. These fixtures never enter application inventory or production seeds. The regular service factory uses the same constructor with the installed catalog and `Providers::installed()`.

## Directory file exchange

Connection managers can export the synchronised directory and import JSON files for private review at `/integrations/people/files`. An import never changes People records, connector projections or checkpoints. Enable file exchange and Directory JSON format on that page, then choose the company's byte, record and unfinished-exchange limits. Import and export require an enabled native connection and a current Workforce mapping. Export also requires a current synchronised directory.

| Company setting | Default | Range |
| --- | --- | --- |
| `people-connector.files.enabled` | `false` | Boolean |
| `people-connector.files.json_enabled` | `true` | Boolean |
| `people-connector.files.max_bytes` | 1048576 | 1–10485760 bytes |
| `people-connector.files.max_records` | 1000 | 1–100000 records |
| `people-connector.files.stale_minutes` | 15 | 1–1440 minutes |

Private storage and expiry use Base Artifacts. Its installation byte limit also applies. Administrators configure its private folder and positive retention period in Operator Settings; connection management does not grant installation settings access. The page offers that link only with both settings and artifact management permissions.

The immutable JSON envelope has exactly `format`, `tenant_id`, `platform_company_id`, `workforce_source_id`, `workforce_company_id` and `records`. Import accepts `people-directory-v1` and `people-directory-v2`; export chooses v2 when positions are present, otherwise v1. A record has exactly `kind`, `source_id`, `stable_id`, `workforce_company_id`, `name`, `code`, `email`, `supervisor_stable_id`, `observed_at` and `active`. The first version allows company and employee records. The second also allows positions, whose five additional fields are `parent_stable_id`, `version`, `vacant`, `assignments_incomplete` and `assignments`. On import, a position may omit parent and version (both default to null) and assignments (defaulting to an empty list); vacant and assignments_incomplete must be booleans. Each of at most 500 assignments has exactly `source_id`, `stable_id`, `employee_stable_id` and `kind`. Names and codes obey the directory contract above; observations are RFC3339 instants. Duplicate JSON keys, extra fields, repeated kind/stable-ID pairs, foreign company/source identities and malformed facts are refused before storage.

Receipts in `people_connector_file_exchanges` record both company axes, connection, direction, SHA-256, count, state, expiry and an opaque artifact ID. **SHA-256 covers the exact bytes**, together with connection and direction as the replay identity. An unexpired replay returns the original receipt without extending expiry. Different whitespace is a different import. Expired files create new receipts. Storage failures can retry the failed receipt; a pending operation refuses a concurrent retry. Once its company timeout passes, the next exchange of those bytes marks the old receipt abandoned, audits `people-connector.files.stale` and reserves a new receipt. Late publication of an abandoned receipt is refused; cleanup remains tracked by Artifacts.

Every artifact creation and read rechecks company reach, the receipt's original connection, enabled native provider and current mapping. Connection removal preserves receipts with an empty connection reference. Recreating or remapping the connection cannot reopen earlier files. Managers may remove expired bytes after disconnecting. This action uses a confirmation and Base's bounded retention batch; receipt history remains. Downloads use the authenticated `DOWNLOADS` route `/integrations/people/files/{company_id}/{id}`, reauthorize each read and return an attachment with private/no-store and nosniff headers. Audit captures receipt IDs, artifact IDs, counts and fixed outcomes, never document content or filenames.

Run the disposable-database file walkthrough with `DATABASE_URL=… BILIBI_FILES_PROOF_ROOT=/absolute/private/directory cargo run -p bilibi-people-connector-walkthrough --example files`. Its fixture company port proves separate company axes even while the native directory port is not yet available. The separate `files-server` example serves the real Files page in the connector-only composition using that fixture port and a fixture-only directory descriptor. Do not run that example with People mounted; the product registers neither fixture.

## Backup and recovery

Connection managers can save a private snapshot at `/integrations/people/backups`, review its changes, then restore it after confirmation. Call `connector.backups()` for the same workflow through `Backup::{summary, configure, create, preview, restore, recover, purge_expired, retry_purge}`. Each call takes the sealed scope and an explicit platform company ID.

A backup includes the native provider contract, both company axes, enabled state, non-secret sync/file/webhook settings, checkpoint, and all Connector directory records, including inactive records and position assignments. It excludes People business data, credentials, webhook signing secrets, sync outcomes, reconciliation history, audit history, and replay guards. `create` saves JSON through Base Artifacts and records its SHA-256. Failed publication leaves a failed receipt. Set private artifact storage in Operator Settings first.

| Company setting | Default | Range |
| --- | --- | --- |
| `people-connector.backup.retention_days` | 30 days | 1–3650 |
| `people-connector.backup.preview_minutes` | 10 minutes | 1–60 |

The page edits both settings inline. A backup keeps the expiry set when it was created, capped by the installation's artifact retention. Changing retention affects new backups. Expired backups cannot be previewed or restored.

### Restore a backup

1. Choose **Preview restore** to check the private bytes and review current and saved values.
2. Choose **Restore backup** and confirm the replacement in the shared dialog.
3. Re-enter any required secrets on People connections. Restore clears credentials and webhook signing secrets, and disables webhook intake.
4. Choose **Sync from checkpoint**. A disabled connection must be enabled first.

The restore preview names the current installation through the shared Instance banner. The preview token stays on the server. Its stored hash binds it to the account, impersonator, current connection state, and preview expiry. A new preview invalidates the previous token. Restore checks authorization, the active company, mapping, bytes, idle sync state, token, and reviewed state again under the company and connection locks.

**Restore replaces configuration, directory records, and checkpoint atomically.** Every changed row goes through MutationBoundary. An audit failure rolls back the complete replacement. Sync outcomes, issues, replay guards, and audit history remain. The checkpoint generation advances past both the saved and current generation. A backup taken before the first sync restores a `bootstrap_required` checkpoint marker so its generation still fences recovery while the next pass performs a bootstrap read.

A backup can be restored once. Repeating the exact actor/token request returns the recorded restore without applying it again. A different actor/token is refused. Removed and recreated connections cannot use old backups, even when their company mapping matches.

Recovery calls the ordinary sync engine with the restored connection identity, expected generation, and `recovery:<backup UUID>` key. A repeated recovery returns its recorded run. If another sync advanced the generation before recovery started, use normal synchronisation on People connections. A recorded unsuccessful outcome is shown as incomplete recovery.

Webhook definitions can land independently in KC3. Snapshots capture those non-secret keys when registered. Restore clears the webhook secret and disables intake whenever those definitions are present, including for backups created before KC3 was installed.

### Clean expired backups

**Clean expired backups** deletes private bytes through Base Artifacts and keeps the receipt with its artifact reference and `purged` state. The installation's artifact batch size, retry interval, and maximum attempts also govern backup cleanup. Each attempt is claimed under the company and receipt locks before file deletion.

A failed cleanup records its fixed error code, attempt count, and time, then waits for the retry interval. At the maximum attempts it moves to **Held cleanup** and leaves automatic batches. Correct private storage, then choose **Retry cleanup** to release the hold and try again. Cleanup remains available after a connection is removed or People is unmounted. Base Artifacts separately reconciles unpublished storage reservations.

The receipt table is `people_connector_backups`, added by migration `people-connector-connector-006-backups`. `fixtures/walkthrough/examples/backups.rs` drives the real facade on a disposable database. `backups-server.rs` serves the real web host with an isolated fixture directory port for browser interaction; run it without People mounted. Neither contributes product records or a production adapter.
