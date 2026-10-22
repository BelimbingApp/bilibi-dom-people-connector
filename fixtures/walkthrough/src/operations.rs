//! KC5 running-system proof. Use a fresh disposable database; no product seeds or unit tests.
use bilibi_authz::grants::Grants;
use bilibi_database::MutationBoundary;
use bilibi_module_registry::{
    Catalog, Consumer, Contribution, Layer, Module, Provenance, Services,
};
use bilibi_people_connector_connector::*;
use bilibi_ports::{Port, Ports, PORTS};
use bilibi_tenancy::Authentication;
use serde_json::{json, Value};
use std::sync::Arc;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(format!("FAIL {message}").into());
    }
    println!("PASS {message}");
    Ok(())
}
/// Exercises the actual facade, Settings, Audit and Artifacts on a disposable database.
pub async fn operations_walkthrough() -> Result<()> {
    let db = bilibi_database::pool(&std::env::var("DATABASE_URL")?)?;
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
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
        .authenticate(&**client, 1, 4, "operations-fixture".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "operations-fixture".into())
        .await?;
    let viewer = auth
        .authenticate(&**client, 1, 2, "operations-fixture".into())
        .await?;
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    let fixture = Module {
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
    deployment.push(&fixture);
    let consumers: Vec<&Consumer> = inventory::iter::<Consumer>
        .into_iter()
        .filter(|c| [ADAPTERS, PORTS].contains(&c.id))
        .collect();
    let port = Contribution::new(
        PORTS,
        DIRECTORY_PORT,
        Provenance {
            module: "people/workforce",
            source: file!(),
            line: line!(),
        },
        &Port {
            provider: &crate::FileCompanyPort,
        },
    );
    let catalog = Catalog::build(&deployment, consumers, vec![&port])?;
    let connector = Connector::from_catalog(
        &services,
        &catalog,
        &deployment,
        Providers::installed(),
        Arc::new(Ports::from_catalog(&catalog, &deployment)?),
    )?;
    require(
        matches!(
            connector.run_doctor(&viewer, 1).await,
            Err(ConnectorError::Unauthorized)
        ),
        "doctor requires company management",
    )?;
    require(
        matches!(
            connector.purge(&manager, 100).await,
            Err(ConnectorError::Unauthorized | ConnectorError::NotFound)
        ),
        "foreign tenant cannot be purged",
    )?;
    let absent_consumers: Vec<&Consumer> = inventory::iter::<Consumer>
        .into_iter()
        .filter(|c| [ADAPTERS, PORTS].contains(&c.id))
        .collect();
    let absent_catalog = Catalog::build(&deployment, absent_consumers, vec![])?;
    let absent = Connector::from_catalog(
        &services,
        &absent_catalog,
        &deployment,
        Providers::installed(),
        Arc::new(Ports::from_catalog(&absent_catalog, &deployment)?),
    )?;
    let absent_report = absent.run_doctor(&manager, 1).await?;
    require(
        absent_report
            .checks
            .iter()
            .any(|c| c.code == "configuration" && c.title == "Company identity unavailable"),
        "People absence remains diagnosable",
    )?;
    require(
        absent.purge(&manager, 1).await?.deleted.is_empty(),
        "retention operates with People absent",
    )?;
    let report = connector.run_doctor(&manager, 1).await?;
    require(
        report
            .checks
            .iter()
            .any(|c| c.code == "configuration" && c.title == "No connection configured"),
        "unconfigured company is a finding",
    )?;
    client.batch_execute("CREATE FUNCTION kc5_audit_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture audit hold'; END $$; CREATE TRIGGER kc5_audit_hold BEFORE INSERT ON base_audit_actions FOR EACH ROW WHEN (NEW.event='people-connector.doctor') EXECUTE FUNCTION kc5_audit_hold()").await?;
    require(
        matches!(
            connector.run_doctor(&manager, 1).await,
            Err(ConnectorError::AuditUnavailable)
        ),
        "doctor fails closed when audit cannot record its findings",
    )?;
    client
        .batch_execute(
            "DROP TRIGGER kc5_audit_hold ON base_audit_actions; DROP FUNCTION kc5_audit_hold()",
        )
        .await?;
    let p = connector.retention_policy(&manager, 1).await?;
    require(
        p.sync_days.is_none()
            && p.webhook_days.is_none()
            && p.file_days.is_none()
            && p.batch_size == 100
            && p.retry_minutes == 60,
        "retention defaults keep all records",
    )?;
    for bad in [
        json!({"batch_size":null}),
        json!({"retry_minutes":1441}),
        json!({"sync_days":0}),
        json!({"file_days":1.5}),
        json!({"unknown":1}),
    ] {
        require(
            matches!(
                connector
                    .put_retention_policy(&manager, 1, bad.as_object().unwrap())
                    .await,
                Err(ConnectorError::InvalidRetentionPolicy)
            ),
            "invalid partial policy refuses",
        )?;
    }
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    connector.set_enabled(&manager, 1, true).await?;
    let connection:i64=client.query_one("SELECT id FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1",&[]).await?.get(0);
    // Direct time travel and corruptible data live only in this disposable fixture.
    let mut runs = vec![];
    for (key, state, age) in [
        ("old-failed", "failed", 10),
        ("old-success", "succeeded", 9),
        ("newer-failed", "failed", 8),
        ("recent-failed", "failed", 0),
        ("running", "running", 0),
    ] {
        let id:i64=client.query_one("INSERT INTO people_connector_sync_runs(tenant_id,connection_id,platform_company_id,provider_id,idempotency_key,pass,state,started_at,finished_at) VALUES(1,$1,1,'people.native',$2,'bootstrap',$3,clock_timestamp()-make_interval(days=>$4::int),CASE WHEN $3='running' THEN NULL ELSE clock_timestamp()-make_interval(days=>$4::int) END) RETURNING id",&[&connection,&key,&state,&age]).await?.get(0);
        runs.push(id);
    }
    let config = connector.run_doctor(&manager, 1).await?;
    require(
        config
            .checks
            .iter()
            .any(|c| c.code == "failed_syncs" && c.count == Some(3)),
        "doctor counts unsuccessful runs without employee data",
    )?;
    require(
        config
            .checks
            .iter()
            .any(|c| c.code == "adapter" && c.state == FindingState::Error),
        "doctor identifies missing adapter",
    )?;
    client.execute("UPDATE people_connector_connections SET workforce_company_id=workforce_company_id+1 WHERE tenant_id=1 AND id=$1",&[&connection]).await?;
    let report = connector.run_doctor(&manager, 1).await?;
    require(
        report
            .checks
            .iter()
            .any(|c| c.code == "configuration" && c.title == "Company mapping changed"),
        "mapping drift remains diagnosable",
    )?;
    client.execute("UPDATE people_connector_connections SET workforce_company_id=workforce_company_id-1,webhook_max_skew_seconds=86400 WHERE tenant_id=1 AND id=$1",&[&connection]).await?;
    for hours in [36, 73] {
        let hash = format!("{hours:064x}");
        client.execute("INSERT INTO people_connector_webhook_deliveries(tenant_id,connection_id,delivery_hash,body_hash,received_at) VALUES(1,$1,$2,$2,(clock_timestamp() AT TIME ZONE 'UTC')-make_interval(hours=>$3))",&[&connection,&hash,&hours]).await?;
        client.execute("INSERT INTO people_connector_webhook_nonces(tenant_id,connection_id,nonce_hash,received_at) VALUES(1,$1,$2,(clock_timestamp() AT TIME ZONE 'UTC')-make_interval(hours=>$3))",&[&connection,&hash,&hours]).await?;
    }
    let kept = connector.purge(&manager, 1).await?;
    require(kept.deleted.is_empty(), "unset policy removes nothing")?;
    connector
        .put_retention_policy(
            &manager,
            1,
            json!({"sync_days":1,"webhook_days":1,"file_days":1,"batch_size":1})
                .as_object()
                .unwrap(),
        )
        .await?;
    // One failed candidate must not starve a later batch. The trigger is fixture-only.
    client.batch_execute(&format!("CREATE FUNCTION kc5_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.id={} THEN RAISE EXCEPTION 'fixture deletion hold'; END IF; RETURN OLD; END $$; CREATE TRIGGER kc5_hold BEFORE DELETE ON people_connector_sync_runs FOR EACH ROW EXECUTE FUNCTION kc5_hold();",runs[0])).await?;
    let first = connector.purge(&manager, 1).await?;
    require(
        first
            .errors
            .iter()
            .any(|r| r.kind == "sync" && r.id == runs[0].to_string()),
        "failed row is returned by ID",
    )?;
    require(
        first.deleted.iter().any(|r| r.kind == "webhook")
            && first.deleted.iter().any(|r| r.kind == "nonce"),
        "other record types commit despite a failed sync deletion",
    )?;
    let second = connector.purge(&manager, 1).await?;
    require(
        second
            .deleted
            .iter()
            .any(|r| r.kind == "sync" && r.id == runs[2].to_string()),
        "retry hold lets the next batch advance",
    )?;
    let guards:i64=client.query_one("SELECT count(*) FROM people_connector_webhook_nonces WHERE tenant_id=1 AND connection_id=$1",&[&connection]).await?.get(0);
    require(
        guards == 1,
        "webhook replay guards survive twice the signing skew",
    )?;
    client.batch_execute("DROP TRIGGER kc5_hold ON people_connector_sync_runs; DROP FUNCTION kc5_hold(); UPDATE people_connector_retention_attempts SET attempted_at=clock_timestamp()-interval '61 minutes' WHERE tenant_id=1 AND platform_company_id=1").await?;
    let retry = connector.purge(&manager, 1).await?;
    require(
        retry.deleted.iter().any(|r| r.id == runs[0].to_string()),
        "expired hold retries successfully",
    )?;
    let remaining:Vec<i64>=client.query("SELECT id FROM people_connector_sync_runs WHERE tenant_id=1 AND connection_id=$1 ORDER BY id",&[&connection]).await?.iter().map(|r|r.get(0)).collect();
    require(
        remaining == vec![runs[1], runs[3], runs[4]],
        "latest successful, recent, and running syncs are preserved",
    )?;
    // Add a latest completed failure after ending the running pass; both latest axes stay.
    client.execute("UPDATE people_connector_sync_runs SET state='failed',finished_at=clock_timestamp()-interval '3 days',started_at=clock_timestamp()-interval '3 days' WHERE tenant_id=1 AND id=$1",&[&runs[4]]).await?;
    let latest:i64=client.query_one("INSERT INTO people_connector_sync_runs(tenant_id,connection_id,platform_company_id,provider_id,idempotency_key,pass,state,started_at,finished_at) VALUES(1,$1,1,'people.native','latest-completed','bootstrap','failed',clock_timestamp()-interval '2 days',clock_timestamp()-interval '2 days') RETURNING id",&[&connection]).await?.get(0);
    client.execute("UPDATE people_connector_sync_runs SET started_at=clock_timestamp()-interval '4 days',finished_at=clock_timestamp()-interval '4 days' WHERE tenant_id=1 AND id=$1",&[&runs[3]]).await?;
    connector.purge(&manager, 1).await?;
    connector.purge(&manager, 1).await?;
    let count:i64=client.query_one("SELECT count(*) FROM people_connector_sync_runs WHERE tenant_id=1 AND connection_id=$1 AND id IN ($2,$3)",&[&connection,&runs[1],&latest]).await?.get(0);
    require(
        count == 2,
        "latest completed run and latest successful run survive age cutoff",
    )?;
    let root = std::env::var("BILIBI_OPERATIONS_PROOF_ROOT")?;
    std::fs::create_dir_all(&root)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    }
    client.execute("INSERT INTO platform_operator(singleton,tenant_id) VALUES(true,1) ON CONFLICT DO NOTHING",&[]).await?;
    let settings = services.get::<bilibi_base::settings::Settings>("base/settings.store")?;
    let boundary = services.get::<MutationBoundary>("base/database.mutations")?;
    let mut tx = boundary
        .begin(
            &mut client,
            admin.mutation_context("operations.fixture.storage"),
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
    connector
        .put_file_policy(
            &manager,
            1,
            FilePolicy {
                enabled: true,
                json_enabled: true,
                max_bytes: 1048576,
                max_records: 1000,
                stale_minutes: 15,
            },
        )
        .await?;
    let bytes = serde_json::to_vec(
        &json!({"format":"people-directory-v1","tenant_id":1,"platform_company_id":1,"workforce_source_id":"people/native","workforce_company_id":1001,"records":[]}),
    )?;
    let receipt = connector.import_file(&manager, 1, &bytes).await?.receipt;
    client.execute("UPDATE people_connector_file_exchanges SET inserted_at=clock_timestamp()-interval '5 days' WHERE tenant_id=1 AND id::text=$1",&[&receipt.id]).await?;
    require(
        connector
            .purge(&manager, 1)
            .await?
            .deleted
            .iter()
            .all(|r| r.kind != "file"),
        "unexpired file bytes preserve their receipt",
    )?;
    client.execute("UPDATE people_connector_file_exchanges SET expires_at=clock_timestamp()-interval '2 days' WHERE tenant_id=1 AND id::text=$1",&[&receipt.id]).await?;
    client.execute("INSERT INTO people_connector_file_exchanges(id,tenant_id,platform_company_id,connection_id,workforce_source_id,workforce_company_id,direction,sha256,record_count,state,inserted_at,updated_at) SELECT gen_random_uuid(),tenant_id,platform_company_id,connection_id,workforce_source_id,workforce_company_id,direction,sha256,record_count,'pending',clock_timestamp()-interval '5 days',clock_timestamp()-interval '5 days' FROM people_connector_file_exchanges WHERE tenant_id=1 AND id::text=$1",&[&receipt.id]).await?;
    let report = connector.run_doctor(&manager, 1).await?;
    require(
        report
            .checks
            .iter()
            .any(|c| c.code == "expired_files" && c.count == Some(1))
            && report
                .checks
                .iter()
                .any(|c| c.code == "stalled_files" && c.count == Some(1)),
        "doctor counts expired and abandoned receipts",
    )?;
    client.batch_execute("CREATE FUNCTION kc5_artifact_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture artifact hold'; END $$; CREATE TRIGGER kc5_artifact_hold BEFORE UPDATE ON base_artifacts FOR EACH ROW EXECUTE FUNCTION kc5_artifact_hold()").await?;
    let held = connector.purge(&manager, 1).await?;
    require(
        held.errors.iter().any(|r| {
            r.id == receipt.id
                && r.reason.as_deref()
                    == Some(ConnectorError::ArtifactCleanupFailed.to_string().as_str())
        }),
        "Artifacts cleanup failure holds its receipt with a safe sentence",
    )?;
    let unpurged: bool = client
        .query_one(
            "SELECT purged_at IS NULL FROM base_artifacts WHERE tenant_id=1 AND public_id=$1",
            &[&receipt.artifact_id],
        )
        .await?
        .get(0);
    require(unpurged, "failed Artifacts cleanup cannot delete a receipt")?;
    client.batch_execute("DROP TRIGGER kc5_artifact_hold ON base_artifacts; DROP FUNCTION kc5_artifact_hold(); UPDATE people_connector_retention_attempts SET attempted_at=clock_timestamp()-interval '61 minutes' WHERE tenant_id=1 AND platform_company_id=1").await?;
    client.execute("UPDATE base_artifacts SET expires_at=created_at+interval '1 microsecond' WHERE tenant_id=1 AND public_id=$1",&[&receipt.artifact_id]).await?;
    // Block the receipt deletion AFTER Artifacts commits cleanup: retry must be idempotent.
    client.batch_execute("CREATE FUNCTION kc5_receipt_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture receipt hold'; END $$; CREATE TRIGGER kc5_receipt_hold BEFORE DELETE ON people_connector_file_exchanges FOR EACH ROW EXECUTE FUNCTION kc5_receipt_hold()").await?;
    let result = connector.purge(&manager, 1).await?;
    require(
        result
            .errors
            .iter()
            .any(|r| r.kind == "file" && r.id == receipt.id),
        "failed receipt deletion remains visible after byte cleanup",
    )?;
    client.batch_execute("DROP TRIGGER kc5_receipt_hold ON people_connector_file_exchanges; DROP FUNCTION kc5_receipt_hold(); UPDATE people_connector_retention_attempts SET attempted_at=clock_timestamp()-interval '61 minutes' WHERE tenant_id=1 AND platform_company_id=1").await?;
    require(
        connector
            .purge(&manager, 1)
            .await?
            .deleted
            .iter()
            .any(|r| r.kind == "file" && r.id == receipt.id),
        "Artifacts-confirmed cleanup retries and removes the receipt",
    )?;
    let holds:i64=client.query_one("SELECT count(*) FROM people_connector_retention_attempts WHERE tenant_id=1 AND platform_company_id=1",&[]).await?.get(0);
    require(holds == 0, "successful deletion clears audited retry holds")?;
    let pending:i64=client.query_one("SELECT count(*) FROM people_connector_file_exchanges WHERE tenant_id=1 AND platform_company_id=1 AND state='pending'",&[]).await?.get(0);
    require(
        pending == 1,
        "pending file exchanges remain regardless of age",
    )?;
    let audit:Value=client.query_one("SELECT payload FROM base_audit_actions WHERE tenant_id=1 AND event='people-connector.doctor' AND payload ? 'checks' ORDER BY id DESC LIMIT 1",&[]).await?.get(0);
    require(
        audit["checks"].as_array().is_some_and(|checks| {
            checks.iter().all(|c| {
                c.as_object().is_some_and(|o| {
                    o.len() == 3
                        && o.contains_key("code")
                        && o.contains_key("state")
                        && o.contains_key("count")
                })
            })
        }),
        "doctor audit contains only finding codes, states and counts",
    )?;
    let mut detached_bytes = bytes.clone();
    detached_bytes.push(b' ');
    let detached = connector
        .import_file(&manager, 1, &detached_bytes)
        .await?
        .receipt;
    client.execute("UPDATE people_connector_file_exchanges SET inserted_at=clock_timestamp()-interval '5 days',expires_at=clock_timestamp()-interval '2 days' WHERE tenant_id=1 AND id::text=$1",&[&detached.id]).await?;
    client.execute("UPDATE base_artifacts SET expires_at=created_at+interval '1 microsecond' WHERE tenant_id=1 AND public_id=$1",&[&detached.artifact_id]).await?;
    connector.remove_connection(&manager, 1).await?;
    require(
        absent
            .purge(&manager, 1)
            .await?
            .deleted
            .iter()
            .any(|r| r.kind == "file" && r.id == detached.id),
        "detached file receipt cleans up without People or a connection",
    )?;
    grants.set_direct(&admin, 1, MANAGE, Some(false)).await?;
    require(
        matches!(
            connector.run_doctor(&manager, 1).await,
            Err(ConnectorError::Unauthorized)
        ),
        "revoked grant refuses next check",
    )?;
    require(
        matches!(
            connector.purge(&manager, 1).await,
            Err(ConnectorError::Unauthorized)
        ),
        "revoked grant refuses next purge",
    )?;
    grants.set_direct(&admin, 1, MANAGE, Some(true)).await?;
    println!("KC5 operations walkthrough complete");
    Ok(())
}

/// After the main proof, prepare the real mounted app for a browser purge failure.
/// This explicit fixture command requires People and a disposable walkthrough database.
pub async fn operations_browser_setup() -> Result<()> {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = bilibi_database::pool(&std::env::var("DATABASE_URL")?)?;
    let modules = bilibi_module_registry::discover()?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    let client = db.get().await?;
    let scope = services
        .get::<Authentication>("base/tenancy.authentication")?
        .authenticate(&**client, 1, 1, "operations-browser-fixture".into())
        .await?;
    let connector = services.get::<Connector>(SERVICE)?;
    connector
        .configure_connection(&scope, 1, NATIVE_PROVIDER_ID)
        .await?;
    connector.set_enabled(&scope, 1, true).await?;
    connector
        .put_retention_policy(
            &scope,
            1,
            json!({"sync_days":1,"batch_size":1}).as_object().unwrap(),
        )
        .await?;
    let connection: i64 = client.query_one("SELECT id FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1", &[]).await?.get(0);
    let mut failed_id = 0;
    for (key, state, age) in [
        ("browser-success", "succeeded", 10),
        ("browser-failure", "failed", 9),
        ("browser-latest", "failed", 8),
    ] {
        let id: i64 = client.query_one("INSERT INTO people_connector_sync_runs(tenant_id,connection_id,platform_company_id,provider_id,idempotency_key,pass,state,started_at,finished_at) VALUES(1,$1,1,'people.native',$2,'bootstrap',$3,clock_timestamp()-make_interval(days=>$4::int),clock_timestamp()-make_interval(days=>$4::int)) ON CONFLICT (connection_id,idempotency_key) DO UPDATE SET state=EXCLUDED.state,started_at=EXCLUDED.started_at,finished_at=EXCLUDED.finished_at RETURNING id", &[&connection,&key,&state,&age]).await?.get(0);
        if key == "browser-failure" {
            failed_id = id;
        }
    }
    client.batch_execute(&format!("CREATE OR REPLACE FUNCTION kc5_browser_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.id={failed_id} THEN RAISE EXCEPTION 'fixture browser hold'; END IF; RETURN OLD; END $$; DROP TRIGGER IF EXISTS kc5_browser_hold ON people_connector_sync_runs; CREATE TRIGGER kc5_browser_hold BEFORE DELETE ON people_connector_sync_runs FOR EACH ROW EXECUTE FUNCTION kc5_browser_hold();")).await?;
    println!("Browser fixture ready: eligible sync {failed_id} will be retained for retry.");
    Ok(())
}
