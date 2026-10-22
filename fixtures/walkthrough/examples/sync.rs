//! KC1 database proof. Run on a disposable fixture database only:
//! BILIBI_ENV=development DATABASE_URL=... cargo run -p bilibi-people-connector-walkthrough --example sync
//! Explicit catalogs substitute a company port and scripted adapter. Nothing is
//! contributed to the running application's inventory, even when KA1 is mounted.
use bilibi_authz::grants::Grants;
use bilibi_database::{Error, Result};
use bilibi_module_registry::{Catalog, Consumer, Contribution, Module, Provenance, Services};
use bilibi_people_connector_connector::*;
use bilibi_ports::{Port, PortClient, PortFuture, PortProvider, Ports, PORTS};
use bilibi_tenancy::{Authentication, Scope};
use jiff::Timestamp;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

static MODE: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static ENTERED: Notify = Notify::const_new();
static RELEASE: Notify = Notify::const_new();
struct CompanyPort;
impl PortProvider for CompanyPort {
    fn call<'a>(&'a self, client: PortClient<'a>, scope: &'a Scope, body: Value) -> PortFuture<'a> {
        Box::pin(async move {
            let company = body["company_id"].as_i64().ok_or(Error::Invalid)?;
            // A fixture provider still proves Core company visibility on the caller's client.
            bilibi_ports::on_client!(client, |c| bilibi_company::organisation::authorize_target(
                c, scope, company, VIEW
            )
            .await)?;
            Ok(
                json!({"value":{"reference":{"source_id":"people/native","type":"company","stable_id":company.to_string()},
            "platform_company_id":company,"workforce_company_id":company+1000,"name":"Workforce company","code":"WORKFORCE"},"freshness":"current"}),
            )
        })
    }
}
static COMPANY_PORT: Contribution = Contribution::new(
    PORTS,
    DIRECTORY_PORT,
    Provenance {
        module: "people/workforce",
        source: file!(),
        line: line!(),
    },
    &Port {
        provider: &CompanyPort,
    },
);
struct Scripted;
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
        port: &Scripted,
    },
);

fn record(kind: &str, id: &str, at: Timestamp) -> Value {
    let mut v = json!({"kind":kind,"source_id":"people/native","stable_id":id,"workforce_company_id":1001,"name":id,"code":id,"observed_at":at.to_string(),"active":true});
    if kind == "position" {
        v["vacant"] = json!(true);
        v["assignments_incomplete"] = json!(false);
        v["assignments"] = json!([]);
    }
    v
}
impl ReadPort for Scripted {
    fn read<'a>(
        &'a self,
        _context: PortContext<'a>,
        auth: &'a PortAuthorization<'a>,
        req: &'a PortRequest,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            CALLS.fetch_add(1, Ordering::SeqCst);
            let mode = MODE.load(Ordering::SeqCst);
            if mode == 9 && auth.capability == "employee_directory" {
                ENTERED.notify_one();
                RELEASE.notified().await;
            }
            if mode == 5 {
                return Err(ReadRefusal::InvalidRequest);
            }
            if mode == 15 {
                panic!("fixture adapter panic");
            }
            let now = Timestamp::now();
            let old = Timestamp::from_second(now.as_second() - 7200)
                .map_err(|_| ReadRefusal::UnexpectedResult)?;
            let organisation = auth.capability == "organization_directory";
            let mut page = Page {
                entries: Vec::new(),
                next_cursor: None,
                resume_cursor: Some("resume".into()),
                as_of: now,
                freshness: Freshness::Current,
                snapshot: true,
            };
            if organisation {
                if mode == 17 {
                    page.freshness = Freshness::Stale(old);
                    return Ok(page);
                }
                if mode == 4 {
                    page.freshness = Freshness::Unavailable("private provider detail".into());
                    return Ok(page);
                }
                if mode == 7 {
                    if req.cursor.is_none() {
                        page.entries = vec![record("position", "position-1", now)];
                        page.next_cursor = Some("page-2".into());
                    }
                    return Ok(page);
                }
                if [2, 6, 8, 16].contains(&mode) {
                    return Ok(page);
                }
                page.entries = vec![
                    record("position", "position-1", now),
                    record("position", "position-2", now),
                ];
                return Ok(page);
            }
            page.entries = vec![
                record("company", "company-1", now),
                record("employee", "employee-1", now),
                record("employee", "employee-2", now),
            ];
            match mode {
                1 => {
                    page.snapshot = false;
                    for v in &mut page.entries {
                        v["observed_at"] = json!(old.to_string());
                        v["name"] = json!("older");
                    }
                }
                2 => {
                    page.entries = vec![
                        json!({"kind":"employee","source_id":"foreign","stable_id":"employee-1","workforce_company_id":1001,"name":"refused","code":"R","observed_at":now.to_string()}),
                    ];
                }
                3 => page.freshness = Freshness::Stale(old),
                6 => {
                    page.entries.truncate(2);
                }
                8 => page.entries.clear(),
                10 => {
                    page.next_cursor = Some("loop".into());
                }
                11 => {
                    page.snapshot = false;
                    page.entries = vec![
                        json!({"kind":"employee","source_id":"people/native","stable_id":"employee-1","observed_at":now.to_string(),"deactivated":true}),
                    ];
                }
                12 => {
                    page.as_of = old;
                }
                13 => {
                    page.entries.push(json!({"broken":true}));
                }
                14 => {
                    page.freshness = Freshness::Unavailable("private provider detail".into());
                }
                19 => {
                    page.entries[1]
                        .as_object_mut()
                        .expect("record")
                        .remove("code");
                }
                16 => {
                    page.entries = vec![record("employee", "employee-1", now)];
                    page.entries[0]["workforce_company_id"] = json!(777);
                }
                _ => {}
            }
            Ok(page)
        })
    }
}
fn expect(condition: bool, message: &str) -> Result<()> {
    if condition {
        println!("PASS {message}");
        Ok(())
    } else {
        eprintln!("FAIL {message}");
        Err(Error::Internal)
    }
}
async fn pass(c: &Connector, s: &Scope, key: &str, mode: usize) -> Result<SyncRun> {
    MODE.store(mode, Ordering::SeqCst);
    c.synchronise(s, 1, key, SyncOptions::default())
        .await
        .map_err(|e| {
            eprintln!("{key}: {e:?}");
            Error::Internal
        })
}
#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("DATABASE_URL").map_err(|_| Error::Invalid)?;
    bilibi_people_connector_walkthrough::run(&url).await?;
    let db = bilibi_database::pool(&url)?;
    let modules = bilibi_module_registry::discover()?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    services
        .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
        .set_enforced(false);
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    let auth = services.get::<Authentication>("base/tenancy.authentication")?;
    let client = db.get().await?;
    let admin = auth
        .authenticate(&**client, 1, 4, "sync-proof".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "sync-proof".into())
        .await?;
    let viewer = auth
        .authenticate(&**client, 1, 2, "sync-proof".into())
        .await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    // Explicit deployment includes only connector and port consumers. The fixture
    // workforce module supplies company identity without mounting a People crate.
    let fixture_workforce = Module {
        id: "people/workforce",
        layer: bilibi_module_registry::Layer::Domains,
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
    let catalog = Catalog::build(&deployment, consumers, vec![&COMPANY_PORT, &ADAPTER])?;
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
        .map(|k| Capability::new(k, Direction::Read).expect("known capability"))
        .collect(),
        Credential::None,
    )
    .expect("native declaration");
    let connector = Arc::new(Connector::from_catalog(
        &services,
        &catalog,
        &deployment,
        Registry::new().register(provider).expect("one provider"),
        ports.clone(),
    )?);
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await
        .map_err(|_| Error::Internal)?;
    connector
        .set_enabled(&manager, 1, true)
        .await
        .map_err(|_| Error::Internal)?;
    expect(
        matches!(
            connector
                .synchronise(&viewer, 1, "denied", SyncOptions::default())
                .await,
            Err(ConnectorError::Unauthorized)
        ),
        "viewer cannot synchronise",
    )?;
    expect(
        matches!(
            connector
                .synchronise(&manager, 1, "bad key", SyncOptions::default())
                .await,
            Err(ConnectorError::InvalidIdempotencyKey)
        ),
        "invalid key refused before reservation",
    )?;
    let first = pass(&connector, &manager, "bootstrap", 0).await?;
    expect(
        first.state == RunState::Succeeded
            && first.pass == RunPass::Bootstrap
            && first.applied == 5
            && first.checkpoint_version == Some(1),
        "bootstrap applies company, employees and positions atomically",
    )?;
    let calls = CALLS.load(Ordering::SeqCst);
    let replay = pass(&connector, &manager, "bootstrap", 0).await?;
    expect(
        replay == first && CALLS.load(Ordering::SeqCst) == calls,
        "same key returns identical run without reading",
    )?;
    expect(
        pass(&connector, &manager, "unchanged", 0).await?.unchanged == 5,
        "identical content is unchanged",
    )?;
    expect(
        pass(&connector, &manager, "older", 1).await?.superseded == 3,
        "older observations never overwrite",
    )?;
    let before = connector
        .sync_summary(&manager, 1)
        .await
        .map_err(|_| Error::Internal)?
        .checkpoint_version;
    let refused = pass(&connector, &manager, "all-refused", 2).await?;
    expect(
        refused.state == RunState::Refused
            && refused.refused == 1
            && refused.checkpoint_version.is_none(),
        "every refused record leaves checkpoint untouched",
    )?;
    for (key, mode, state) in [
        ("stale", 3, RunState::Stale),
        ("adapter-error", 5, RunState::Failed),
        ("unavailable", 14, RunState::Unavailable),
        ("cursor-loop", 10, RunState::Failed),
        ("panic", 15, RunState::Failed),
    ] {
        let r = pass(&connector, &manager, key, mode).await?;
        expect(
            r.state == state,
            "non-current or broken adapter applies nothing",
        )?;
    }
    expect(
        connector
            .sync_summary(&manager, 1)
            .await
            .map_err(|_| Error::Internal)?
            .checkpoint_version
            == before,
        "failed reads cannot move checkpoint",
    )?;
    let org_stale = pass(&connector, &manager, "organisation-stale", 17).await?;
    expect(
        org_stale.state == RunState::Stale && org_stale.checkpoint_version.is_none(),
        "a later stale organisation stream discards the earlier directory stream",
    )?;
    pass(&connector, &manager, "known-invalid", 19).await?;
    expect(
        connector
            .workforce(&manager, 1)
            .await
            .map_err(|_| Error::Internal)?
            .require_current()
            .map_err(|_| Error::Internal)?
            .iter()
            .any(|r| r.stable_id == "employee-1"),
        "refused known identity is protected from snapshot deactivation",
    )?;
    expect(
        pass(&connector, &manager, "wrong-company", 16).await?.state == RunState::Refused,
        "foreign workforce company is refused",
    )?;
    expect(
        pass(&connector, &manager, "partial-refusal", 13)
            .await?
            .refused
            == 1,
        "a bad record does not discard accepted records",
    )?;
    expect(
        pass(&connector, &manager, "organisation-unavailable", 4)
            .await?
            .state
            == RunState::Succeeded,
        "unavailable organisation still applies directory",
    )?;
    let summary = connector
        .sync_summary(&manager, 1)
        .await
        .map_err(|_| Error::Internal)?;
    expect(
        summary
            .open_issues
            .iter()
            .any(|i| i.kind == "organisation_unavailable"),
        "organisation absence opens issue",
    )?;
    let directory_only = Provider::new(
        NATIVE_PROVIDER_ID,
        "People (this installation)",
        "1.0.0",
        vec![Capability::new("employee_directory", Direction::Read).expect("known capability")],
        Credential::None,
    )
    .expect("valid provider");
    let directory_connector = Connector::from_catalog(
        &services,
        &catalog,
        &deployment,
        Registry::new()
            .register(directory_only)
            .expect("one provider"),
        ports.clone(),
    )?;
    let undeclared = pass(&directory_connector, &manager, "undeclared-organisation", 0).await?;
    expect(
        undeclared.refused == 1
            && directory_connector
                .sync_summary(&manager, 1)
                .await
                .map_err(|_| Error::Internal)?
                .open_issues
                .iter()
                .any(|i| i.kind == "organisation_unavailable"),
        "undeclared company records are refused and existing positions stay with an issue",
    )?;
    pass(&connector, &manager, "position-multipage", 7).await?;
    let records = connector
        .workforce(&manager, 1)
        .await
        .map_err(|_| Error::Internal)?
        .require_current()
        .map_err(|_| Error::Internal)?;
    expect(
        records
            .iter()
            .filter(|r| r.kind == RecordKind::Position)
            .count()
            == 2,
        "multipage full positions do not deactivate missing positions",
    )?;
    pass(&connector, &manager, "position-single-page", 6).await?;
    let records = connector
        .workforce(&manager, 1)
        .await
        .map_err(|_| Error::Internal)?
        .require_current()
        .map_err(|_| Error::Internal)?;
    expect(
        records.len() == 2 && records.iter().all(|r| r.kind != RecordKind::Position),
        "single-page full read deactivates absent positions and employee",
    )?;
    pass(&connector, &manager, "restore-records", 0).await?;
    expect(
        pass(&connector, &manager, "deactivation", 11)
            .await?
            .deactivated
            == 1,
        "explicit changes deactivation retains inactive record",
    )?;
    let bad_policy = connector
        .put_sync_policy(
            &manager,
            1,
            SyncPolicyUpdate {
                page_limit: Some(4),
                max_age_minutes: Some(1),
                ..Default::default()
            },
        )
        .await;
    expect(
        matches!(
            bad_policy,
            Err(ConnectorError::InvalidPolicy("max_age_minutes"))
        ),
        "invalid policy is rejected as a whole",
    )?;
    expect(
        connector
            .sync_summary(&manager, 1)
            .await
            .map_err(|_| Error::Internal)?
            .policy
            .page_limit
            == 250,
        "invalid policy cannot partially write",
    )?;
    connector
        .put_sync_policy(
            &manager,
            1,
            SyncPolicyUpdate {
                max_age_minutes: Some(5),
                ..Default::default()
            },
        )
        .await
        .map_err(|_| Error::Internal)?;
    pass(&connector, &manager, "old-watermark", 12).await?;
    expect(
        matches!(
            connector
                .workforce(&manager, 1)
                .await
                .map_err(|_| Error::Internal)?
                .freshness,
            Freshness::Stale(_)
        ),
        "projection becomes stale at oldest watermark",
    )?;
    // Reserve a real in-flight pass and use a second facade call while its read waits.
    MODE.store(9, Ordering::SeqCst);
    let c = connector.clone();
    let s = manager.clone();
    let task = tokio::spawn(async move {
        c.synchronise(&s, 1, "in-flight", SyncOptions::default())
            .await
    });
    ENTERED.notified().await;
    expect(
        matches!(
            connector
                .synchronise(&manager, 1, "second-pass", SyncOptions::default())
                .await,
            Err(ConnectorError::SyncInProgress)
        ),
        "a second pass is refused during reads",
    )?;
    expect(
        connector
            .synchronise(&manager, 1, "in-flight", SyncOptions::default())
            .await
            .map_err(|_| Error::Internal)?
            .state
            == RunState::Running,
        "running key replay never calls adapter",
    )?;
    connector
        .set_enabled(&manager, 1, false)
        .await
        .map_err(|_| Error::Internal)?;
    connector
        .set_enabled(&manager, 1, true)
        .await
        .map_err(|_| Error::Internal)?;
    RELEASE.notify_one();
    let late = task
        .await
        .map_err(|_| Error::Internal)?
        .map_err(|_| Error::Internal)?;
    expect(
        late.state == RunState::Refused && late.reason.as_deref() == Some("connection_changed"),
        "connection edit refuses late result",
    )?;
    connector
        .set_enabled(&manager, 1, false)
        .await
        .map_err(|_| Error::Internal)?;
    expect(
        matches!(
            connector
                .workforce(&manager, 1)
                .await
                .map_err(|_| Error::Internal)?
                .freshness,
            Freshness::Unavailable(_)
        ),
        "disabled connection withholds projection",
    )?;
    connector
        .set_enabled(&manager, 1, true)
        .await
        .map_err(|_| Error::Internal)?;
    expect(
        matches!(
            connector
                .request_port(
                    &manager,
                    1,
                    NATIVE_PROVIDER_ID,
                    "employee_directory",
                    Direction::Read
                )
                .await,
            Err(ConnectorError::AdapterUnavailable)
        ),
        "declared request_port never hands out adapter",
    )?;
    expect(
        matches!(
            connector
                .request_port(
                    &manager,
                    1,
                    NATIVE_PROVIDER_ID,
                    "employee_directory",
                    Direction::Write
                )
                .await,
            Err(ConnectorError::Unsupported)
        ),
        "read declaration never permits writes",
    )?;
    let issue = connector
        .sync_summary(&manager, 1)
        .await
        .map_err(|_| Error::Internal)?
        .open_issues
        .into_iter()
        .next()
        .ok_or(Error::Internal)?;
    connector
        .resolve_issue(&manager, 1, issue.id)
        .await
        .map_err(|_| Error::Internal)?;
    expect(
        !connector
            .sync_summary(&manager, 1)
            .await
            .map_err(|_| Error::Internal)?
            .open_issues
            .iter()
            .any(|i| i.id == issue.id),
        "manager resolves an open issue",
    )?;
    // SQL deliberately creates an abandoned reservation to prove recovery of a
    // process that died. This is fixture fault injection, never a product write.
    let row=client.query_one("SELECT id FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1",&[]).await?;
    let id: i64 = row.get(0);
    client.execute("INSERT INTO people_connector_sync_runs (tenant_id,connection_id,platform_company_id,provider_id,idempotency_key,pass,state,started_at) VALUES (1,$1,1,'people.native','abandoned','incremental','running',now()-interval '2 hours')",&[&id]).await?;
    expect(
        connector
            .sync_summary(&manager, 1)
            .await
            .map_err(|_| Error::Internal)?
            .last_run
            .is_some(),
        "summary reads persisted runs",
    )?;
    let unknown = pass(&connector, &manager, "abandoned", 0).await?;
    expect(
        unknown.state == RunState::Unknown
            && unknown.reason.as_deref() == Some("no_outcome_recorded"),
        "abandoned key becomes unknown and is never retried",
    )?;
    expect(
        connector
            .sync_summary(&manager, 1)
            .await
            .map_err(|_| Error::Internal)?
            .open_issues
            .iter()
            .any(|i| i.kind == "unknown_outcome"),
        "unknown outcome has reconciliation issue",
    )?;
    MODE.store(9, Ordering::SeqCst);
    let c = connector.clone();
    let s = manager.clone();
    let delayed = tokio::spawn(async move {
        c.synchronise(&s, 1, "timed-out-worker", SyncOptions::default())
            .await
    });
    ENTERED.notified().await;
    client.execute("UPDATE people_connector_sync_runs SET started_at=now()-interval '2 hours' WHERE tenant_id=1 AND connection_id=$1 AND idempotency_key='timed-out-worker'",&[&id]).await?;
    pass(&connector, &manager, "after-timeout", 0).await?;
    RELEASE.notify_one();
    expect(
        delayed
            .await
            .map_err(|_| Error::Internal)?
            .map_err(|_| Error::Internal)?
            .state
            == RunState::Unknown,
        "timed-out worker cannot apply its late result",
    )?;
    MODE.store(11, Ordering::SeqCst);
    expect(
        connector
            .synchronise(
                &manager,
                1,
                "bootstrap-deactivation",
                SyncOptions {
                    full: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| Error::Internal)?
            .state
            == RunState::Failed,
        "bootstrap refuses explicit deactivation pages",
    )?;
    MODE.store(0, Ordering::SeqCst);
    expect(
        connector
            .synchronise(
                &manager,
                1,
                "forced-full",
                SyncOptions {
                    full: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| Error::Internal)?
            .pass
            == RunPass::Bootstrap,
        "full option bootstraps after an existing checkpoint",
    )?;
    let mismatch = connector
        .synchronise(
            &manager,
            1,
            "fenced",
            SyncOptions {
                expected_checkpoint_version: Some(9999),
                ..Default::default()
            },
        )
        .await;
    expect(
        matches!(mismatch, Err(ConnectorError::CheckpointMoved)),
        "recovery checkpoint fence refuses moved checkpoint",
    )?;
    expect(
        pass(&connector, &manager, "empty", 8).await?.state == RunState::Succeeded,
        "empty full read records warning and deactivates",
    )?;
    let audit: i64 = client
        .query_one(
            "SELECT count(*) FROM base_audit_mutations WHERE auditable_type LIKE 'people_connector_%'",
            &[],
        )
        .await?
        .get(0);
    expect(audit > 20, "sync mutations captured in Base Audit")?;
    connector
        .remove_connection(&manager, 1)
        .await
        .map_err(|_| Error::Internal)?;
    let left:i64=client.query_one("SELECT (SELECT count(*) FROM people_connector_sync_runs WHERE tenant_id=1 AND connection_id=$1)+(SELECT count(*) FROM people_connector_workforce_records WHERE tenant_id=1 AND connection_id=$1)+(SELECT count(*) FROM people_connector_sync_checkpoints WHERE tenant_id=1 AND connection_id=$1)+(SELECT count(*) FROM people_connector_reconciliation_issues WHERE tenant_id=1 AND connection_id=$1)",&[&id]).await?.get(0);
    expect(left == 0, "removal deletes all owned sync rows")?;
    println!("KC1 sync proof complete");
    Ok(())
}
