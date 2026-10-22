# Connector

`people_connector/connector` owns the integration contracts, each company's
provider connection and, in later slices, directory synchronisation into
Connector-owned projections, inbound notifications, file exchange,
operations and backups. This README is the owner contract; the Bilimbi
source it ports is
[`connector/docs/README.md`](https://github.com/BelimbingApp/b-dom-people-connector/blob/main/connector/docs/README.md).

This is the scaffold slice (KS0 of the port plan): contracts, connections,
the credential, status and the read-port seam. KC1 adds the sync engine
(`synchronise`, `workforce`, `sync_summary`, `resolve_issue`,
`put_sync_policy`, `request_port`), KC2 the connections page, KC3 inbound
notifications, KC4 file exchange, KC5 the doctor and retention, KC6 backups.

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
`adapter_unavailable` (KC1). `people_connector/native_people_adapter`
contributes the native provider's adapter (KA1).

## Menu and capabilities

The Authz domain is `people-connector` with `people-connector.connections.view`
and `people-connector.connections.manage`; Bilimbi grants neither to a
contributed role, so an administrator grants them. The menu leaf
**Administration › System › Integrations › People connections**
(`admin.system.integrations.people-connections`, order 50, route
`/integrations/people/connections`) carries the view capability; the route
enforces it again when the page lands (KC2).
