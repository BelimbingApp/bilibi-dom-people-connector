//! KV1: Bilimbi Connector workflows through the installed catalog and real People port.
//! This proof consumes People's own walkthrough records; it never owns a People dependency.
use bilibi_authz::grants::Grants;
use bilibi_database::{pool, Error, MutationBoundary};
use bilibi_module_registry::Services;
use bilibi_people_connector_connector::*;
use bilibi_tenancy::{Authentication, Scope};
use bilibi_ui::page::{PageRequest, PageService};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn prove(ok: bool, label: &str) -> Result<()> {
    if !ok {
        return Err(format!("KV1 FAILED: {label}").into());
    }
    println!("KV1 proved: {label}");
    Ok(())
}

// Read only Connector-owned tables, including immutable receipt IDs and checkpoint generation.
async fn retained(client: &tokio_postgres::Client) -> Result<Value> {
    let mut value = serde_json::Map::new();
    for table in [
        "connections",
        "sync_checkpoints",
        "sync_runs",
        "workforce_records",
        "reconciliation_issues",
        "webhook_nonces",
        "webhook_deliveries",
        "file_exchanges",
        "retention_attempts",
        "backups",
    ] {
        let sql = format!("SELECT coalesce(jsonb_agg(to_jsonb(r)-'webhook_secret' ORDER BY id), '[]'::jsonb) FROM people_connector_{table} r WHERE tenant_id=1");
        value.insert(table.into(), client.query_one(&sql, &[]).await?.get(0));
    }
    Ok(Value::Object(value))
}

async fn intake(c: &Connector, scope: &Scope) -> Result<()> {
    c.put_webhook_settings(
        scope,
        1,
        WebhookSettingsUpdate {
            enabled: Some(true),
            secret: Some(std::env::var("KV1_WEBHOOK_SECRET")?),
            max_skew_seconds: Some(300),
        },
    )
    .await?;
    Ok(())
}

fn curl(
    secret: &str,
    port: u16,
    nonce: &str,
    delivery: &str,
    signed: &str,
    sent: &str,
) -> Result<String> {
    let timestamp = jiff::Timestamp::now().as_second();
    let message = format!("people-native:v1\n1\n1\n{timestamp}\n{nonce}\n{delivery}\n{signed}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(message.as_bytes());
    let signature = data_encoding::HEXLOWER.encode(&mac.finalize().into_bytes());
    // Keep authentication headers out of argv, evidence and stdout.
    let config = format!("url = \"http://localhost:{port}/webhooks/people-native\"\nrequest = \"POST\"\nheader = \"Content-Type: application/json\"\nheader = \"x-people-tenant: 1\"\nheader = \"x-people-company: 1\"\nheader = \"x-people-timestamp: {timestamp}\"\nheader = \"x-people-nonce: {nonce}\"\nheader = \"x-people-delivery: {delivery}\"\nheader = \"x-people-signature: {signature}\"\ndata-binary = {}\n", serde_json::to_string(sent)?);
    let mut child = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--connect-timeout",
            "3",
            "--max-time",
            "10",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "--config",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("curl stdin missing")?
        .write_all(config.as_bytes())?;
    let output = child.wait_with_output()?;
    prove(output.status.success(), "curl completed")?;
    Ok(String::from_utf8(output.stdout)?)
}

pub(super) async fn run(url: &str, action: &str) -> Result<()> {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = pool(url)?;
    let modules = bilibi_module_registry::discover()?;
    let migrations = bilibi_database::migrate(&db, &modules).await?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    services
        .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
        .set_enforced(false);
    let auth = services.get::<Authentication>("base/tenancy.authentication")?;
    let mut client = db.get().await?;
    let admin = auth
        .authenticate(&**client, 1, 4, "KV1 walkthrough".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "KV1 walkthrough".into())
        .await?;
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    grants.set_direct(&admin, 2, VIEW, Some(true)).await?;
    let c = services.get::<Connector>(SERVICE)?;
    let root = std::env::var("KV1_PROOF_ROOT")?;
    let baseline = Path::new(&root).join("retained.json");
    match action {
        "absent" => {
            prove(!c.people_available(), "People directory is unmounted")?;
            let before = retained(&client).await?;
            prove(
                matches!(
                    c.synchronise(&manager, 1, "kv1-absent", SyncOptions::default())
                        .await,
                    Err(ConnectorError::PeopleUnavailable)
                ),
                "sync refuses PeopleUnavailable",
            )?;
            prove(
                matches!(
                    c.status(&manager, 1).await,
                    Err(ConnectorError::PeopleUnavailable)
                ),
                "status explains People is absent",
            )?;
            prove(
                before == retained(&client).await?,
                "refused sync preserves Connector data",
            )?;
            prove(
                serde_json::from_slice::<Value>(&std::fs::read(&baseline)?)? == before,
                "unmount retains connection, directory, files, backups and checkpoint",
            )?;
            let path = std::collections::HashMap::new();
            let query = std::collections::HashMap::from([("company_id".into(), "1".into())]);
            for (operation, message, withheld) in [
                (
                    "connections",
                    "People is not mounted",
                    "id=\"people-connections-synchronise\"",
                ),
                (
                    "files",
                    "People is not mounted",
                    "id=\"people-files-export\"",
                ),
                ("operations", "People is absent", ""),
                (
                    "backups",
                    "People is absent",
                    "id=\"people-backups-create\"",
                ),
            ] {
                let service = format!("people_connector/connector.{operation}_pages");
                let page = services.get::<PageService<Scope, Error>>(&service)?;
                let rendered = page
                    .0
                    .render(PageRequest {
                        operation,
                        scope: Some(&manager),
                        path: &path,
                        query: &query,
                        body: &[],
                        framed: false,
                    })
                    .await?;
                prove(
                    rendered.html.contains(message),
                    &format!("{operation} page explains People is absent"),
                )?;
                prove(
                    withheld.is_empty() || !rendered.html.contains(withheld),
                    &format!("{operation} withholds actions requiring People"),
                )?;
            }
            return Ok(());
        }
        "remount" => {
            prove(c.people_available(), "People directory is mounted again")?;
            prove(
                migrations.applied.is_empty(),
                "remount reapplies no migrations",
            )?;
            prove(
                serde_json::from_slice::<Value>(&std::fs::read(&baseline)?)?
                    == retained(&client).await?,
                "remount retains exact Connector data",
            )?;
            prove(
                c.status(&manager, 1).await?.state == ConnectionState::Enabled,
                "remounted connection remains enabled",
            )?;
            return Ok(());
        }
        "delivery" => {
            intake(&c, &manager).await?;
            let secret = std::env::var("KV1_WEBHOOK_SECRET")?;
            let port = std::env::var("WEBHOOK_PROOF_PORT")?.parse()?;
            let body = r#"{"event":"directory.changed"}"#;
            let key = uuid::Uuid::new_v4().simple().to_string();
            let nonce = format!("kv1-{key}");
            let delivery = format!("delivery-{key}");
            prove(
                curl(&secret, port, &nonce, &delivery, body, body)? == "202",
                "signed delivery accepted over HTTP",
            )?;
            prove(
                curl(&secret, port, &nonce, &delivery, body, body)? == "403",
                "replayed nonce refused over HTTP",
            )?;
            prove(
                curl(
                    &secret,
                    port,
                    &format!("retry-{key}"),
                    &delivery,
                    body,
                    body,
                )? == "202",
                "identical delivery with fresh nonce acknowledged",
            )?;
            prove(
                curl(
                    &secret,
                    port,
                    &format!("bad-{key}"),
                    &format!("bad-delivery-{key}"),
                    body,
                    "{}",
                )? == "403",
                "tampered body refused over HTTP",
            )?;
            std::fs::write(
                &baseline,
                serde_json::to_vec_pretty(&retained(&client).await?)?,
            )?;
            return Ok(());
        }
        "mounted" => (),
        _ => return Err("use mounted, delivery, absent or remount".into()),
    }
    prove(
        c.people_available() && c.adapters().serves(NATIVE_PROVIDER_ID),
        "real People port and inventory adapter are installed",
    )?;
    std::fs::create_dir_all(&root)?;
    let storage = Path::new(&root).join("artifacts");
    std::fs::create_dir_all(&storage)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for directory in [Path::new(&root), storage.as_path()] {
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    // Disposable installation owner, as in the existing file/backup walkthroughs.
    client.execute("INSERT INTO platform_operator(singleton,tenant_id) VALUES(true,1) ON CONFLICT DO NOTHING", &[]).await?;
    let settings = services.get::<bilibi_base::settings::Settings>("base/settings.store")?;
    let boundary = services.get::<MutationBoundary>("base/database.mutations")?;
    let mut tx = boundary
        .begin(&mut client, admin.mutation_context("kv1.fixture.storage"))
        .await?;
    for (key, value) in [
        ("artifacts.storage_root", json!(storage)),
        ("artifacts.retention_days", json!(7)),
    ] {
        settings
            .put(
                &mut tx,
                &admin,
                bilibi_base::settings::Target::Global,
                key,
                value,
            )
            .await?;
    }
    tx.commit().await?;
    c.configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    c.set_enabled(&manager, 1, true).await?;
    c.put_sync_policy(
        &manager,
        1,
        SyncPolicyUpdate {
            page_limit: Some(1),
            ..Default::default()
        },
    )
    .await?;
    let key = format!("kv1-{}", uuid::Uuid::new_v4());
    let first = c
        .synchronise(&manager, 1, &key, SyncOptions::default())
        .await?;
    prove(
        first.state == RunState::Succeeded && first.refused == 0 && first.applied >= 3,
        "native sync applies working employees and company",
    )?;
    prove(
        c.synchronise(&manager, 1, &key, SyncOptions::default())
            .await?
            == first,
        "same sync key returns the recorded run",
    )?;
    let full = c
        .synchronise(
            &manager,
            1,
            &format!("{key}-full"),
            SyncOptions {
                full: true,
                ..Default::default()
            },
        )
        .await?;
    prove(
        full.state == RunState::Succeeded && full.refused == 0,
        "full native read succeeds",
    )?;
    let records = c.workforce(&manager, 1).await?.require_current()?;
    prove(
        records.iter().any(|r| r.kind == RecordKind::Company)
            && records
                .iter()
                .filter(|r| r.kind == RecordKind::Employee)
                .count()
                >= 2,
        "projection contains real Workforce records",
    )?;
    c.put_file_policy(
        &manager,
        1,
        FilePolicy {
            enabled: true,
            json_enabled: true,
            max_bytes: 1_048_576,
            max_records: 1000,
            stale_minutes: 15,
        },
    )
    .await?;
    let export = c.export_file(&manager, 1).await?;
    let bytes = c
        .download_file(&manager, 1, &export.receipt.id)
        .await?
        .bytes;
    let document: Value = serde_json::from_slice(&bytes)?;
    prove(
        document["records"]
            .as_array()
            .is_some_and(|v| v.len() == records.len()),
        "export downloads the complete native projection",
    )?;
    std::fs::write(Path::new(&root).join("directory.json"), &bytes)?;
    let imported = c.import_file(&manager, 1, &bytes).await?;
    prove(
        imported.receipt.state == "ready" && !imported.replayed,
        "native export imports for review",
    )?;
    prove(
        c.import_file(&manager, 1, &bytes).await?.replayed,
        "identical import replays its receipt",
    )?;
    intake(&c, &manager).await?;
    let backup = c.backups();
    let saved = backup.create(&manager, 1).await?;
    prove(
        saved.state == "ready",
        "backup publishes a private snapshot",
    )?;
    let generation = c.sync_summary(&manager, 1).await?.checkpoint_version;
    let preview = backup.preview(&manager, 1, &saved.id).await?;
    prove(
        !backup
            .restore(&manager, 1, &saved.id, &preview.token, true)
            .await?
            .replayed,
        "previewed restore commits once",
    )?;
    let webhook = c.webhook_summary(&manager, 1).await?;
    prove(
        !webhook.enabled && !webhook.secret_stored,
        "restore disables intake and clears signing secret",
    )?;
    prove(
        c.sync_summary(&manager, 1).await?.checkpoint_version > generation,
        "restore advances checkpoint generation",
    )?;
    prove(
        backup
            .restore(&manager, 1, &saved.id, &preview.token, true)
            .await?
            .replayed,
        "restore retry acknowledges without applying twice",
    )?;
    let recovered = backup.recover(&manager, 1, &saved.id).await?;
    prove(
        recovered.state == RunState::Succeeded,
        "recovery synchronises the restored native checkpoint",
    )?;
    prove(
        backup.recover(&manager, 1, &saved.id).await? == recovered,
        "recovery key replays its recorded run",
    )?;
    prove(
        !c.run_doctor(&manager, 1).await?.checks.is_empty(),
        "doctor checks the real native connection",
    )?;
    std::fs::write(
        &baseline,
        serde_json::to_vec_pretty(&retained(&client).await?)?,
    )?;
    Ok(())
}
