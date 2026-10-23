# Native People adapter

This module lets the People connector read the workforce in the same installation.

It serves `people.native` through the Connector’s `ADAPTERS` catalog. Boot installs one `NativePeopleAdapter` contribution under `people_connector/native_people_adapter`. Removing this module leaves the Connector with `adapter_unavailable`.

## Read a directory

Configure and enable a native connection on **People connections**, then choose **Synchronise now**. The Connector reads company and employee pages under `employee_directory`, followed by positions under `organization_directory`. **Full read** starts another complete scan. Every successful native page has `snapshot: true`; a changes pass also enumerates the whole current directory.

The adapter calls `people/workforce.directory` with the caller’s scope and database client. It declares only Platform dependencies and the Connector edge. It owns no tables, settings or routes. People may be unmounted without breaking composition.

## Company and employee pages

The first page contains the company. Later pages contain at most the requested number of working employees, ordered by immutable employment ID. A company-only page makes a limit of one work without changing Workforce’s employee scan size between pages. Employee requests are bounded to 1,000 records.

Company records carry the source, stable identity, workforce company, name and code. Employee records also carry the employee number, display name, permitted work email and supervisor’s employee identity. Login identities never stand in for companies or employees.

Every page checks both company axes against the company returned by Workforce. The platform and workforce IDs need not be equal. Employee and position company references must match the returned company reference.

## Positions

Position requests are bounded to 100 records. The adapter preserves the parent identity, title or code, version, vacancy, assignment identities and employee references, and the incomplete-assignment flag. The sync engine validates each record’s content, including the 500-assignment bound.

A pass uses the UTC date of its first observation throughout the position scan. The Connector keeps its existing conservative rule: absent positions deactivate only after a complete stream that fits in one page.

## Cursors and freshness

The adapter’s cursor wraps Workforce’s opaque continuation. It binds the tenant, both company axes, stream, requested limit and observation watermark. Reusing it with another stream, company or size returns `invalid_cursor`. Every record and page in one stream shares that watermark; the final resume cursor is its RFC 3339 timestamp.

The resume cursor is an observation token. `since` never filters a native changes pass. Workforce’s employee cursor fixes the high-water ID and working-status configuration; its position cursor fixes the scan’s company, date and bounds. These are live reads across calls, rather than a database snapshot. A new complete scan picks up changes after the previous high-water ID.

**An unavailable or stale stream stops the entire pass before any records or checkpoint are applied.** Without People, the adapter returns an empty page marked `people_unavailable`. Without Organisation, it returns an empty page marked `organisation_unavailable`. Neither is an empty successful snapshot. Provider error text is never forwarded or persisted.

## Refusals

| Code | Cause |
| --- | --- |
| `invalid_authorization` | Foreign provider or source; unsupported capability or direction; non-positive company ID |
| `invalid_request` | Zero page limit, or an initial request refused by Workforce |
| `invalid_cursor` | Malformed cursor, changed scan bounds, or refused Workforce continuation |
| `not_found` | Company missing, archived or outside the caller’s reach |
| `mapping_mismatch` | Returned company axes, source or reference kinds do not match the authorization |
| `unexpected_result` | Incompatible answer shape, invalid successful page or database failure |

`company_directory` is a provider declaration for company records, not a separate adapter stream. As in Bilimbi, the adapter accepts only `employee_directory` and `organization_directory` read authorizations.

## Verify it

Mount the container in Bilibi, discover modules and run `scripts/domain-ci.sh` on an empty disposable database. Run the fault-injection proof on a separate empty database:

```sh
BILIBI_ENV=development cargo run -p bilibi-people-connector-walkthrough --example native-adapter
```

The proof installs the Platform fixture and uses an explicit directory-port catalog. It drives the real adapter and Connector, including unequal company axes, cursor replay and refusals, position assignments, and an unavailable stream leaving the projection and checkpoint unchanged. Scripted answers never enter application inventory.

For the real People port, mount People beside the connector and use another empty database:

```sh
BILIBI_ENV=development cargo run -p bilibi-people-walkthrough --bin workforce-settings -- setup
BILIBI_ENV=development cargo run -p bilibi-people-connector-walkthrough --example native-adapter -- --live
```

The first command creates employments through People’s facade. The second proves inventory registration, real directory paging and bootstrap, changes and full passes through the Connector.

## Where to go next

See [the Connector](../connector/README.md) for the sync contract and the mounted People `workforce/README.md`, “Connector directory port”, for the wire shapes.
