# bilibi-dom-people-connector

People connector Domain for [Bilibi](https://github.com/BelimbingApp/bilibi):
how a company's People workforce directory is connected to a provider, kept
in step and reviewed, ported with UI parity from Bilimbi's
[b-dom-people-connector](https://github.com/BelimbingApp/b-dom-people-connector).
It is one optional, independently versioned repository that an adopter
mounts into a Bilibi checkout beside
[bilibi-dom-people](https://github.com/BelimbingApp/bilibi-dom-people); it
does not build on its own.

| Module | Descriptor ID | Crate | Owns |
| --- | --- | --- | --- |
| [`connector/`](connector/README.md) | `people_connector/connector` | `bilibi-people-connector-connector` | the provider-neutral contracts (`Capability`, `Provider`, `Registry`, `Providers::installed`: the native `people.native` provider only), each company's connection with both company axes (`people_connector_connections`), the encrypted provider credential (`people-connector.connection.credential`, stored or cleared, never shown), the `people-connector` Authz domain with `connections.view` and `connections.manage`, the menu leaf Administration › System › Integrations › People connections, and the read-port seam (`ReadPort`, `PortAuthorization`, `PortRequest`, `Page`) with the `Adapters` catalog adapter modules contribute to |

The second Bilimbi module, `people_connector/native_people_adapter`, lands
beside it in a later slice of the port: it contributes the adapter that
serves `people.native` from the People directory port. The sync engine,
inbound notifications, file exchange, operations and backups follow in the
connector module (port plan rows KC1 to KC6), each with its page.

## People is reached through a port

Bilimbi's Connector depended on `people/workforce`. Bilibi forbids a Cargo
dependency, descriptor edge or SQL reference into another container, so the
connector asks People Workforce by port id, `people/workforce.directory`
(Bilibi `apps/base/ports/README.md`), with its own serde types for the
documented shape, and composes without People. The standing call "the
connector requires People mounted" holds at run time: while no mounted
module answers the port, every call that needs workforce identity is refused
as `people_unavailable` and the connections page says so. The shape the
connector reads is documented in
[the connector contract](connector/README.md#the-people-directory-port).

## Mount

Clone People and this repository into a Bilibi checkout (real nested
checkouts, never symlinks) and work from the Bilibi root:

```sh
git clone https://github.com/BelimbingApp/bilibi-dom-people.git apps/domains/people
git clone https://github.com/BelimbingApp/bilibi-dom-people-connector.git apps/domains/people_connector
python3 scripts/discover-modules.py
cargo run -p bilibi-web -- migrate
```

Bilibi discovers each mounted container from its `bilibi.container.toml`;
no list names them. The connector also composes alone. Removing a folder
removes that Domain's code from the next build and keeps its data: the
migration ledger keeps the applied versions, and a remount replays nothing.
The mount rules are in Bilibi's `apps/domains/AGENTS.md`. Commit and push
only inside this repository; the Platform's `apps/domains/Cargo.toml`,
`apps/domains/src/lib.rs` and `Cargo.lock` change while mounted and are
never committed there.

## Walkthrough

Proofs never use a seeded database. `fixtures/walkthrough` is an
example-only crate: against a freshly created disposable database it
migrates the composed app, installs the Platform walkthrough fixture (its
tenants, accounts, Persons, affiliations and companies), grants
`manager@example.test` both connector capabilities and
`viewer@example.test` the view capability, prints the installed providers
and the adapters serving them, and drives the facade: the company's status,
a provider choice without the grant, an uninstalled provider, the native
provider, the credential paths, enable and disable, removal, a sibling
company without reach and another tenant's company. While no mounted module
answers the People directory port, each step that needs workforce identity
is refused with the connector's own sentence and the walkthrough says so;
with the port mounted the same steps store, enable, disable and remove the
connection. It ends with PostgreSQL refusing a second connection for a
platform company, a second platform company on one workforce company, and a
company of another tenant.

```sh
createdb -h 127.0.0.1 -p 5438 -U bilibi bilibi_people_connector_walkthrough
export DATABASE_URL=postgres://bilibi:bilibi@127.0.0.1:5438/bilibi_people_connector_walkthrough
export BILIBI_SETTINGS_KEY=$(openssl rand -hex 32)
cargo run -p bilibi-people-connector-walkthrough
APP_ORIGIN=http://localhost:3000 cargo run -p bilibi-web -- run
```

The settings key is an ephemeral key for the encrypted credential setting;
never reuse a production key. Sign in at <http://localhost:3000> as
`manager@example.test` (password `bilibi-dev`): the sidebar shows
Administration › System › Integrations › People connections. The page
behind the leaf lands with the connections page slice (KC2).

## Checks

There are no unit tests. The gate is the Platform's `scripts/domain-ci.sh`,
run from the Bilibi root with this repository mounted and `DATABASE_URL`
naming an empty PostgreSQL 18 database: discovery, the committed CSS, the
mounted `Cargo.lock` pin, `cargo fmt`, `cargo clippy -D warnings`, the
release build (`DOMAIN_CI_BUILD=check` boots the debug build instead),
`migrate`, `seed-production`, and a boot that answers `/`. Run it once with
the connector mounted alone and once with People mounted beside it, as CI
does.

## CI

`.github/workflows/ci.yml` is Bilibi's reference Domain workflow
(`docs/domain-ci/ci.yml`) with the container ID set, plus a second job. Both
check out Bilibi at the revision in
[`.github/bilibi-revision`](.github/bilibi-revision), move this repository
to `apps/domains/people_connector`, run `scripts/domain-ci.sh` and verify the
gates left the repository unchanged. `Mounted in Bilibi` runs the connector
alone (the job Bilibi's catalogue looks for). `Mounted in Bilibi with People`
also clones `bilibi-dom-people` at `main` into `apps/domains/people` first,
the topology an adopter runs. Bump the revision to build against a newer
Platform.

## Naming

Optional Bilibi repositories are named `bilibi-<role>-<id>`: `dom-` for a
Domain. The mount folder is the container ID, so
`bilibi-dom-people-connector` mounts at `apps/domains/people_connector`.
Module crates are `bilibi-people-connector-<module>` (underscores become
hyphens), capability keys `people-connector.<module>.*` under the Authz
domain `people-connector` (a capability segment allows hyphens, not
underscores), tables `people_connector_*` and migrations
`people-connector-<module>-NNN-<slug>`, all as Bilimbi names them; module
IDs (`people_connector/connector`) are Bilimbi's, kept because IDs are
persisted data.

## License

[MIT](LICENSE).
