# Connector composition sweep

Run the connector against the real People directory, then remove and remount People while keeping the database.

The `fixtures/walkthrough` example `sweep` uses the installed services and native adapter. It adds no fixture provider to the app and has no dependency on a People crate. It mirrors Bilimbi's connection, file and backup workflows.

## Prepare a disposable installation

Mount both repositories in a Platform checkout. Set `DATABASE_URL` to an empty disposable database and configure PostgreSQL authentication in the shell. Keep `PGPASSFILE` available to each command when using password-file authentication.

Choose a private directory outside the repositories for proof output:

```sh
export BILIBI_ENV=development
export BILIBI_SETTINGS_KEY=$(openssl rand -hex 32)
export KV1_WEBHOOK_SECRET=$(openssl rand -hex 32)
export KV1_PROOF_ROOT=/tmp/connector-sweep
mkdir -m 700 "$KV1_PROOF_ROOT"
python3 scripts/discover-modules.py
cargo run -p bilibi-people-walkthrough --bin workforce-settings -- setup
cargo run -p bilibi-people-connector-walkthrough --example sweep -- mounted
```

People's walkthrough creates its own employments. The connector sweep configures private artifact storage and grants connector access to the fixture manager. It checks sync replay, full read, export and import replay, backup, preview, restore replay, secret clearing, checkpoint generation, recovery replay and Doctor. Exported directory bytes and the data-preservation baseline stay in `KV1_PROOF_ROOT`.

## Check signed delivery through HTTP

Start the product web binary on your own plain HTTP port, keeping the same environment:

```sh
export APP_ORIGIN=http://localhost:3192
export BIND_ADDR=127.0.0.1:3192
cargo build -p bilibi-web
./target/debug/bilibi-web seed-production
./target/debug/bilibi-web run
```

The Dev seed records the installation role that permits loopback HTTP. Setting `BILIBI_ENV` alone does not change that stored role.

In another shell with the same database, installation key and proof directory, generate a fresh signing secret and run:

```sh
export KV1_WEBHOOK_SECRET=$(openssl rand -hex 32)
export WEBHOOK_PROOF_PORT=3192
cargo run -p bilibi-people-connector-walkthrough --example sweep -- delivery
```

This mode saves the secret through the connector and invokes `curl`. It requires HTTP 202 for an accepted delivery, 403 for a reused nonce, 202 for an identical delivery with a fresh nonce, and 403 for tampered bytes. Authentication headers travel through curl's standard input and are never printed.

## Use the four pages

Sign in as `manager@example.test` with the disposable fixture password `bilibi-dev`. Open these pages in both light and dark themes:

| Page | Try |
| --- | --- |
| `/integrations/people/connections` | Synchronise now, Full read, policy form, signing policy and last receipt; table customization, compact rows and sorting |
| `/integrations/people/files` | Export, download, upload the saved `directory.json`, import replay and expired-file confirmation |
| `/integrations/people/operations` | Run check, findings, retention settings and purge confirmation |
| `/integrations/people/backups` | Back up, preview, restore confirmation and Sync from checkpoint |

Keep screenshots and notes outside the repositories. Never capture a signing secret, installation key, session or restore token.

## Remove and remount People

Stop only the app process you started. Move the People clone out of `apps/domains` into your private scratch directory, then rebuild the same app and sweep example:

```sh
python3 scripts/discover-modules.py
cargo build -p bilibi-web
cargo run -p bilibi-people-connector-walkthrough --example sweep -- absent
```

The absent mode requires a typed `PeopleUnavailable` refusal and compares stored connection, checkpoint, directory, file and backup rows with the saved baseline. Start the rebuilt app and visit all four pages again. They must explain that People is absent and withhold actions that require it.

Move the same People clone back, rediscover and rebuild. Run `sweep -- remount` before changing any connector data. It requires unchanged rows, no reapplied migrations and an enabled connection.

Run `scripts/domain-ci.sh` on a separate empty database for each composition. The repository workflow runs the same two compositions. Keep one build directory and reuse it throughout the sweep.
