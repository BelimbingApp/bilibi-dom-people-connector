//! KC6 disposable-database proof of Bilimbi Connector.Backup. No product registrations.
use bilibi_authz::grants::Grants;
use bilibi_database::{Error, MutationBoundary};
use bilibi_module_registry::{
    Catalog, Consumer, Contribution, Layer, Module, Provenance, Services,
};
use bilibi_people_connector_connector::*;
use bilibi_ports::{Port, PortClient, PortFuture, PortProvider, Ports, PORTS};
use bilibi_tenancy::{Authentication, Scope};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
static REMAPPED: AtomicBool = AtomicBool::new(false);
/// Fixture-only company port with deliberately different platform/workforce identities.
pub struct BackupCompanyPort;
impl PortProvider for BackupCompanyPort {
    fn call<'a>(&'a self, client: PortClient<'a>, scope: &'a Scope, body: Value) -> PortFuture<'a> {
        Box::pin(async move {
            let company = body["company_id"].as_i64().ok_or(Error::Invalid)?;
            bilibi_ports::on_client!(client, |sql| {
                bilibi_company::organisation::authorize_target(sql, scope, company, VIEW).await
            })?;
            Ok(
                json!({"value":{"reference":{"source_id":"people/native","type":"company","stable_id":company.to_string()},"platform_company_id":company,"workforce_company_id":company+if REMAPPED.load(Ordering::SeqCst){2000}else{1000},"name":"Fixture workforce company","code":"FIXTURE"},"freshness":"current"}),
            )
        })
    }
}
static PORT: Contribution = Contribution::new(
    PORTS,
    DIRECTORY_PORT,
    Provenance {
        module: "people/workforce",
        source: file!(),
        line: line!(),
    },
    &Port {
        provider: &BackupCompanyPort,
    },
);
pub struct BackupDirectory;
impl ReadPort for BackupDirectory {
    fn read<'a>(
        &'a self,
        _context: PortContext<'a>,
        auth: &'a PortAuthorization<'a>,
        _request: &'a PortRequest,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            let at = jiff::Timestamp::now();
            let kinds = if auth.capability == "organization_directory" {
                vec!["position"]
            } else {
                vec!["company", "employee"]
            };
            let entries=kinds.into_iter().map(|kind|{let mut value=json!({"kind":kind,"source_id":"people/native","stable_id":kind,"workforce_company_id":1001,"name":"Fixture directory fact","code":"FIXTURE","email":null,"supervisor_stable_id":null,"observed_at":at.to_string(),"active":true});if kind=="position"{value["parent_stable_id"]=Value::Null;value["version"]=json!(1);value["vacant"]=json!(true);value["assignments_incomplete"]=json!(false);value["assignments"]=json!([]);}value}).collect();
            Ok(Page {
                entries,
                next_cursor: None,
                resume_cursor: Some("backups-fixture".into()),
                as_of: at,
                freshness: Freshness::Current,
                snapshot: true,
            })
        })
    }
}
static ADAPTER: Contribution = Contribution::new(
    ADAPTERS,
    NATIVE_PROVIDER_ID,
    Provenance {
        module: MODULE,
        source: file!(),
        line: line!(),
    },
    &Adapter {
        provider_id: NATIVE_PROVIDER_ID,
        port: &BackupDirectory,
    },
);
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(format!("FAIL {message}").into());
    }
    println!("PASS {message}");
    Ok(())
}
/// Run only against the task's disposable database and private evidence directory.
pub async fn backup_walkthrough() -> Result<()> {
    let url = std::env::var("DATABASE_URL")?;
    let root = std::env::var("BILIBI_BACKUPS_PROOF_ROOT")?;
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = bilibi_database::pool(&url)?;
    let modules = bilibi_module_registry::discover()?;
    bilibi_database::migrate(&db, &modules).await?;
    bilibi_walkthrough_fixture::install(&db).await?;
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
        .authenticate(&**client, 1, 4, "backups-fixture".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "backups-fixture".into())
        .await?;
    let viewer = auth
        .authenticate(&**client, 1, 2, "backups-fixture".into())
        .await?;
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    std::fs::create_dir_all(&root)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    }
    // Fixture-only operator registration; production administration uses its owner.
    client.execute("INSERT INTO platform_operator(singleton,tenant_id) VALUES(true,1) ON CONFLICT DO NOTHING",&[]).await?;
    let settings = services.get::<bilibi_base::settings::Settings>("base/settings.store")?;
    let boundary = services.get::<MutationBoundary>("base/database.mutations")?;
    let mut tx = boundary
        .begin(
            &mut client,
            admin.mutation_context("backups.fixture.storage"),
        )
        .await?;
    for (key, value) in [
        ("artifacts.storage_root", json!(root)),
        ("artifacts.retention_days", json!(1)),
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
    let fixture_workforce = Module {
        id: "people/workforce",
        layer: Layer::Domains,
        dependencies: &["base/ports"],
        migrations: &[],
        services: &[],
    };
    let mut deployment: Vec<&Module> = modules
        .iter()
        .copied()
        .filter(|m| m.id != "people/workforce")
        .collect();
    deployment.push(&fixture_workforce);
    let consumers: Vec<&Consumer> = inventory::iter::<Consumer>
        .into_iter()
        .filter(|c| [ADAPTERS, PORTS].contains(&c.id))
        .collect();
    let catalog = Catalog::build(&deployment, consumers, vec![&PORT, &ADAPTER])?;
    let ports = Arc::new(Ports::from_catalog(&catalog, &deployment)?);
    let provider = Provider::new(
        NATIVE_PROVIDER_ID,
        "People (this installation)",
        "1.0.0",
        [
            "company_directory",
            "employee_directory",
            "organization_directory",
        ]
        .into_iter()
        .map(|key| Capability::new(key, Direction::Read).unwrap())
        .collect(),
        Credential::None,
    )?;
    let connector = Connector::from_catalog(
        &services,
        &catalog,
        &deployment,
        Registry::new().register(provider)?,
        ports,
    )?;
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    let backup = connector.backups();
    let mut tx = boundary
        .begin(
            &mut client,
            admin.mutation_context("backups.fixture.storage_outage"),
        )
        .await?;
    settings
        .put(
            &mut tx,
            &admin,
            bilibi_base::settings::Target::Global,
            "artifacts.storage_root",
            json!(format!("{root}/missing")),
        )
        .await?;
    tx.commit().await?;
    require(
        backup.create(&manager, 1).await.is_err(),
        "storage outage refuses backup publication",
    )?;
    require(
        backup
            .summary(&manager, 1)
            .await?
            .records
            .first()
            .is_some_and(|r| r.state == "failed"),
        "failed publication leaves a failed receipt",
    )?;
    let mut tx = boundary
        .begin(
            &mut client,
            admin.mutation_context("backups.fixture.storage_restored"),
        )
        .await?;
    settings
        .put(
            &mut tx,
            &admin,
            bilibi_base::settings::Target::Global,
            "artifacts.storage_root",
            json!(root),
        )
        .await?;
    tx.commit().await?;

    require(
        backup.create(&viewer, 1).await.is_err(),
        "viewer cannot back up",
    )?;
    require(
        backup.summary(&manager, 100).await.is_err(),
        "foreign company is hidden",
    )?;
    require(
        backup
            .configure(
                &manager,
                1,
                BackupPolicy {
                    retention_days: 0,
                    preview_minutes: 10,
                }
                .into(),
            )
            .await
            .is_err(),
        "backup policy bounds enforced",
    )?;
    backup
        .configure(
            &manager,
            1,
            BackupPolicy {
                retention_days: 30,
                preview_minutes: 10,
            }
            .into(),
        )
        .await?;
    backup
        .configure(
            &manager,
            1,
            BackupPolicyUpdate {
                preview_minutes: Some(11),
                ..Default::default()
            },
        )
        .await?;
    let policy = backup
        .configure(
            &manager,
            1,
            BackupPolicyUpdate {
                retention_days: Some(31),
                ..Default::default()
            },
        )
        .await?;
    require(
        policy.preview_minutes == 11 && policy.retention_days == 31,
        "inline policy updates preserve unrelated concurrent edits",
    )?;
    backup
        .configure(
            &manager,
            1,
            BackupPolicy {
                retention_days: 30,
                preview_minutes: 10,
            }
            .into(),
        )
        .await?;
    client.execute("INSERT INTO people_connector_sync_runs (tenant_id,connection_id,platform_company_id,provider_id,idempotency_key,pass,state,started_at) SELECT tenant_id,id,platform_company_id,provider_id,'backup-proof-running','bootstrap','running',clock_timestamp() FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1",&[]).await?;
    require(
        matches!(
            backup.create(&manager, 1).await,
            Err(ConnectorError::SyncInProgress)
        ),
        "running sync prevents backup reservation",
    )?;
    client.execute("DELETE FROM people_connector_sync_runs WHERE tenant_id=1 AND idempotency_key='backup-proof-running'",&[]).await?;
    let empty = backup.create(&manager, 1).await?;
    let preview = backup.preview(&manager, 1, &empty.id).await?;
    require(
        backup
            .restore(&manager, 1, &empty.id, &preview.token, false)
            .await
            .is_err(),
        "restore requires explicit confirmation",
    )?;
    let restored = backup
        .restore(&manager, 1, &empty.id, &preview.token, true)
        .await?;
    require(!restored.replayed, "empty checkpoint restores once")?;
    let generation: i64 = client
        .query_one(
            "SELECT version FROM people_connector_sync_checkpoints WHERE tenant_id=1",
            &[],
        )
        .await?
        .get(0);
    require(
        generation > 0,
        "empty checkpoint restore allocates a fencing generation",
    )?;
    require(
        matches!(
            backup.recover(&manager, 1, &empty.id).await,
            Err(ConnectorError::Disconnected)
        ),
        "disabled recovery is refused",
    )?;
    connector.set_enabled(&manager, 1, true).await?;
    let run = backup.recover(&manager, 1, &empty.id).await?;
    require(
        run.state == RunState::Succeeded && run.pass == RunPass::Bootstrap,
        "restored empty checkpoint bootstraps through ordinary sync",
    )?;
    let replay = backup.recover(&manager, 1, &empty.id).await?;
    require(
        replay.id == run.id,
        "recovery replays its recorded sync run",
    )?;
    connector
        .synchronise(&manager, 1, "backup-followup", SyncOptions::default())
        .await?;
    require(
        backup.recover(&manager, 1, &empty.id).await?.id == run.id,
        "recovery replay survives later checkpoint advancement",
    )?;
    let secret = format!("fixture-secret-{}", jiff::Timestamp::now().as_nanosecond());
    let mut tx = boundary
        .begin(
            &mut client,
            admin.mutation_context("backups.fixture.credential"),
        )
        .await?;
    settings
        .put(
            &mut tx,
            &admin,
            bilibi_base::settings::Target::Company(bilibi_party_ids::OrganisationId::new(1)?),
            CREDENTIAL_KEY,
            json!(secret),
        )
        .await?;
    tx.commit().await?;
    let webhook_secret = format!("webhook-{secret}{secret}");
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                enabled: Some(true),
                secret: Some(webhook_secret.clone()),
                max_skew_seconds: Some(600),
            },
        )
        .await?;
    let receipt = backup.create(&manager, 1).await?;
    require(
        receipt.expires_at.parse::<jiff::Timestamp>()?.as_second()
            - jiff::Timestamp::now().as_second()
            <= 86400,
        "backup expiry capped by installation artifact retention",
    )?;
    let artifact = receipt.artifact_id.as_ref().ok_or("artifact missing")?;
    let path = std::path::Path::new(&root).join(artifact);
    let bytes = std::fs::read(&path)?;
    let saved: Value = serde_json::from_slice(&bytes)?;
    require(
        saved["format"] == "people-connection-backup-v1"
            && saved["projections"].as_array().unwrap().len() == 3,
        "snapshot contains every directory kind",
    )?;
    require(
        saved.get("sync_runs").is_none() && saved["settings"].get(CREDENTIAL_KEY).is_none(),
        "snapshot excludes history and credentials",
    )?;
    require(
        !String::from_utf8_lossy(&bytes).contains(&secret),
        "snapshot contains no stored credential bytes",
    )?;
    require(
        !String::from_utf8_lossy(&bytes).contains(&webhook_secret)
            && saved["connection"].get("webhook_secret").is_none()
            && saved["settings"]["people-connector.webhook.max_skew_seconds"] == 600
            && saved["settings"]["people-connector.webhook.enabled"] == true,
        "snapshot contains only non-secret webhook policy",
    )?;
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                max_skew_seconds: Some(900),
                ..Default::default()
            },
        )
        .await?;

    client.execute("UPDATE people_connector_workforce_records SET name='Changed after backup',active=false WHERE tenant_id=1 AND kind='employee'",&[]).await?;
    let preview = backup.preview(&manager, 1, &receipt.id).await?;
    require(
        preview
            .changes
            .iter()
            .any(|c| c.label.contains("Name") && c.before != c.after),
        "preview includes changed directory facts",
    )?;
    require(
        matches!(
            backup
                .restore(&admin, 1, &receipt.id, &preview.token, true)
                .await,
            Err(ConnectorError::InvalidConfirmation)
        ),
        "confirmation is bound to the authenticated actor",
    )?;
    require(
        matches!(
            backup
                .restore(&manager, 1, &receipt.id, "wrong-token", true)
                .await,
            Err(ConnectorError::InvalidConfirmation)
        ),
        "forged confirmation is refused",
    )?;
    connector
        .put_sync_policy(
            &manager,
            1,
            SyncPolicyUpdate {
                page_limit: Some(17),
                ..Default::default()
            },
        )
        .await?;
    require(
        matches!(
            backup
                .restore(&manager, 1, &receipt.id, &preview.token, true)
                .await,
            Err(ConnectorError::PreviewChanged)
        ),
        "changed reviewed state refuses restore",
    )?;
    let preview = backup.preview(&manager, 1, &receipt.id).await?;
    client.execute("UPDATE people_connector_backups SET preview_expires_at=clock_timestamp()-interval '1 minute' WHERE tenant_id=1 AND id=$1::text::uuid",&[&receipt.id]).await?;
    require(
        matches!(
            backup
                .restore(&manager, 1, &receipt.id, &preview.token, true)
                .await,
            Err(ConnectorError::PreviewExpired)
        ),
        "expired preview is refused",
    )?;
    let old = backup.preview(&manager, 1, &receipt.id).await?;
    let preview = backup.preview(&manager, 1, &receipt.id).await?;
    require(
        matches!(
            backup
                .restore(&manager, 1, &receipt.id, &old.token, true)
                .await,
            Err(ConnectorError::InvalidConfirmation)
        ),
        "new preview invalidates the previous token",
    )?;
    // A real audit outage occurs after all restore writes; the entire transaction must roll back.
    client.batch_execute("CREATE FUNCTION backup_proof_refuse_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event='people-connector.backup.restored' THEN RAISE EXCEPTION 'fixture audit outage'; END IF; RETURN NEW; END $$; CREATE TRIGGER backup_proof_audit BEFORE INSERT ON base_audit_actions FOR EACH ROW EXECUTE FUNCTION backup_proof_refuse_audit()").await?;
    require(
        backup
            .restore(&manager, 1, &receipt.id, &preview.token, true)
            .await
            .is_err(),
        "audit failure refuses restore",
    )?;
    let unchanged:bool=client.query_one("SELECT name='Changed after backup' AND NOT active FROM people_connector_workforce_records WHERE tenant_id=1 AND kind='employee'",&[]).await?.get(0);
    require(
        unchanged && connector.sync_summary(&manager, 1).await?.policy.page_limit == 17,
        "failed restore rolls directory and settings back together",
    )?;
    client.batch_execute("DROP TRIGGER backup_proof_audit ON base_audit_actions; DROP FUNCTION backup_proof_refuse_audit()").await?;
    let restored = backup
        .restore(&manager, 1, &receipt.id, &preview.token, true)
        .await?;
    require(
        !restored.replayed && connector.sync_summary(&manager, 1).await?.policy.page_limit == 250,
        "confirmed restore replaces settings",
    )?;
    let restored_fact:bool=client.query_one("SELECT name='Fixture directory fact' AND active FROM people_connector_workforce_records WHERE tenant_id=1 AND kind='employee'",&[]).await?.get(0);
    require(
        restored_fact,
        "confirmed restore replaces projections atomically",
    )?;
    require(
        !connector.status(&manager, 1).await?.credential_stored,
        "restore clears the excluded stored credential",
    )?;
    let webhook = connector.webhook_summary(&manager, 1).await?;
    require(
        !webhook.enabled && !webhook.secret_stored && webhook.max_skew_seconds == 600,
        "restore clears webhook secret, disables intake and restores signing window",
    )?;

    require(
        backup
            .restore(&manager, 1, &receipt.id, &preview.token, true)
            .await?
            .replayed,
        "confirmed restore replays without applying twice",
    )?;
    require(
        matches!(
            backup.preview(&manager, 1, &receipt.id).await,
            Err(ConnectorError::AlreadyRestored)
        ),
        "a restored backup cannot mint another preview",
    )?;
    connector
        .synchronise(
            &manager,
            1,
            "before-backup-recovery",
            SyncOptions::default(),
        )
        .await?;
    require(
        matches!(
            backup.recover(&manager, 1, &receipt.id).await,
            Err(ConnectorError::CheckpointMoved)
        ),
        "intervening sync fences an unused recovery",
    )?;
    let ready = backup.create(&manager, 1).await?;
    let original =
        std::fs::read(std::path::Path::new(&root).join(ready.artifact_id.as_ref().unwrap()))?;
    std::fs::write(
        std::path::Path::new(&root).join(ready.artifact_id.as_ref().unwrap()),
        b"tampered",
    )?;
    require(
        matches!(
            backup.preview(&manager, 1, &ready.id).await,
            Err(ConnectorError::BackupIntegrity)
        ),
        "tampered artifact refuses preview",
    )?;
    std::fs::write(
        std::path::Path::new(&root).join(ready.artifact_id.as_ref().unwrap()),
        original,
    )?;
    client.execute("UPDATE people_connector_backups SET expires_at=clock_timestamp()-interval '1 minute' WHERE tenant_id=1 AND id=$1::text::uuid",&[&ready.id]).await?;
    require(
        backup.preview(&manager, 1, &ready.id).await.is_err(),
        "expired receipt refuses preview",
    )?;
    // Replace one artifact with a directory to exercise actual Artifacts delete failures.
    let path = std::path::Path::new(&root).join(ready.artifact_id.as_ref().unwrap());
    std::fs::remove_file(&path)?;
    std::fs::create_dir(&path)?;
    let mut tx = boundary
        .begin(&mut client, admin.mutation_context("backups.fixture.retry"))
        .await?;
    settings
        .put(
            &mut tx,
            &admin,
            bilibi_base::settings::Target::Global,
            "artifacts.purge_max_attempts",
            json!(1),
        )
        .await?;
    tx.commit().await?;
    let purge = backup.purge_expired(&manager, 1).await?;
    require(
        purge.errors.contains(&ready.id),
        "failed byte deletion is recorded on receipt",
    )?;
    require(
        backup
            .summary(&manager, 1)
            .await?
            .holds
            .iter()
            .any(|r| r.id == ready.id && r.purge_attempts == 1),
        "cleanup failure reaches held worklist at installation limit",
    )?;
    require(
        !backup
            .purge_expired(&manager, 1)
            .await?
            .errors
            .contains(&ready.id),
        "held receipt is excluded from automatic batches",
    )?;
    std::fs::remove_dir(&path)?;
    backup.retry_purge(&manager, 1, &ready.id).await?;
    require(
        backup
            .summary(&manager, 1)
            .await?
            .records
            .iter()
            .any(|r| r.id == ready.id && r.state == "purged"),
        "retry releases cleanup hold and preserves purged receipt",
    )?;
    let before_removal = backup.create(&manager, 1).await?;
    let preview = backup.preview(&manager, 1, &before_removal.id).await?;
    connector.remove_connection(&manager, 1).await?;
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    require(
        matches!(
            backup
                .restore(&manager, 1, &before_removal.id, &preview.token, true)
                .await,
            Err(ConnectorError::ConnectionChanged)
        ),
        "removed/recreated connection cannot restore an old backup",
    )?;
    require(
        backup
            .summary(&manager, 1)
            .await?
            .records
            .iter()
            .any(|r| r.id == before_removal.id),
        "receipts survive connection removal",
    )?;
    connector.set_enabled(&manager, 1, true).await?;
    let browser = backup.create(&manager, 1).await?;
    println!("Browser-ready backup {}", browser.id);
    println!("Backup walkthrough completed");
    Ok(())
}
