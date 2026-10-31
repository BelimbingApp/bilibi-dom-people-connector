//! Bilimbi NativePeopleAdapter's contract, driven through Base Ports and the
//! real Connector on a disposable database. Explicit catalogs keep scripted
//! port answers out of application inventory and production data.
use bilibi_authz::grants::Grants;
use bilibi_database::{Error, Result};
use bilibi_module_registry::{
    Catalog, Consumer, Contribution, Layer, Module, Provenance, Services,
};
use bilibi_people_connector_connector::*;
use bilibi_people_connector_native_people_adapter::NativePeopleAdapter;
use bilibi_ports::{Port, PortClient, PortFuture, PortProvider, Ports, PORTS};
use bilibi_tenancy::{Authentication, Scope};
use jiff::Timestamp;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

static MODE: AtomicUsize = AtomicUsize::new(0);
struct Directory;
fn reference(kind: &str, id: &str) -> Value {
    json!({"source_id":"people/native", "type":kind, "stable_id":id})
}
impl PortProvider for Directory {
    fn call<'a>(&'a self, client: PortClient<'a>, scope: &'a Scope, body: Value) -> PortFuture<'a> {
        Box::pin(async move {
            let id = body["company_id"].as_i64().ok_or(Error::Invalid)?;
            bilibi_ports::on_client!(client, |c| bilibi_company::authorize_work_target(
                c, scope, id, VIEW
            )
            .await)?;
            let mode = MODE.load(Ordering::SeqCst);
            let now = Timestamp::now().to_string();
            let identity = json!({"platform_company_id":id, "workforce_company_id":id+1000});
            match body["read"].as_str() {
                Some("company") => {
                    if mode == 3 {
                        return Ok(json!({"value":null,"freshness":{"stale":now}}));
                    }
                    let mut company = identity;
                    company["reference"] = reference("company", "native-company");
                    company["name"] = json!("Fixture workforce");
                    company["code"] = json!("KA1");
                    Ok(json!({"value":company,"freshness":"current"}))
                }
                Some("employees") => {
                    if mode == 4 {
                        return Ok(json!({"freshness":{"unavailable":"private detail"}}));
                    }
                    let next = body["cursor"].is_null();
                    if !next && mode == 8 {
                        return Err(Error::Invalid);
                    }
                    let mut e = identity;
                    e["reference"] = reference("employee", if next { "10" } else { "11" });
                    if mode == 1 {
                        e["reference"]["source_id"] = json!("foreign");
                    }
                    if mode == 2 {
                        e["workforce_company_id"] = json!(9999);
                    }
                    e["company_reference"] = reference("company", "native-company");
                    e["employee_number"] = json!(if next { "E-10" } else { "E-11" });
                    e["display_name"] = json!("Fixture employee");
                    e["email"] = json!("employee@example.test");
                    e["supervisor_reference"] = if next {
                        Value::Null
                    } else {
                        reference("employee", "10")
                    };
                    Ok(
                        json!({"employees":[e],"freshness":"current","snapshot":true,
                        "watermark":now,"next_cursor":if next {Some("employees-2")} else {None}}),
                    )
                }
                Some("positions") => {
                    if mode == 5 {
                        return Ok(json!({"freshness":"unavailable","reason":"private detail"}));
                    }
                    let mut p = identity;
                    p["reference"] = reference(
                        "position",
                        if body["cursor"].is_null() { "10" } else { "11" },
                    );
                    p["company_reference"] = reference("company", "native-company");
                    p["code"] = json!("POSITION");
                    p["title"] = json!("Fixture position");
                    p["parent_reference"] = reference("position", "9");
                    p["version"] = json!(2);
                    p["vacant"] = json!(false);
                    p["assignments_incomplete"] = json!(true);
                    p["assignments"] = json!([{"reference":reference("assignment","90"),
                        "employee_reference":reference("employee","10"),"kind":"substantive"}]);
                    if mode == 6 {
                        p["assignments"][0]["employee_reference"]["type"] = json!("user");
                    }
                    Ok(
                        json!({"positions":[p],"next_cursor":if mode == 9 && body["cursor"].is_null() {Some("positions-2")} else {None},"freshness":"current",
                        "snapshot":true,"watermark":now}),
                    )
                }
                _ => Err(Error::Invalid),
            }
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
        provider: &Directory,
    },
);

fn prove(ok: bool, label: &str) -> Result<()> {
    if !ok {
        eprintln!("KA1 FAILED: {label}");
        return Err(Error::Internal);
    }
    println!("KA1 proved: {label}");
    Ok(())
}
fn saved<T, E: std::fmt::Debug>(result: std::result::Result<T, E>) -> Result<T> {
    result.map_err(|e| {
        eprintln!("KA1 unexpected refusal: {e:?}");
        Error::Internal
    })
}
async fn pages(
    context: PortContext<'_>,
    a: &PortAuthorization<'_>,
) -> Result<Vec<WorkforceRecord>> {
    let mut r = PortRequest {
        pass: Pass::Bootstrap,
        since: None,
        cursor: None,
        limit: 1,
    };
    let mut entries = vec![];
    let mut watermark = None;
    for _ in 0..30 {
        let p = saved(NativePeopleAdapter.read(context, a, &r).await)?;
        prove(
            p.entries.len() <= r.limit && p.snapshot && p.freshness == Freshness::Current,
            "current snapshot stays within page limit",
        )?;
        prove(
            watermark.is_none_or(|at| at == p.as_of)
                && p.resume_cursor == Some(p.as_of.to_string()),
            "watermark stays fixed through continuation",
        )?;
        watermark = Some(p.as_of);
        for e in p.entries {
            entries.push(saved(serde_json::from_value(e))?);
        }
        r.cursor = p.next_cursor;
        if r.cursor.is_none() {
            return Ok(entries);
        }
    }
    Err(Error::Internal)
}

pub(super) async fn run(url: &str, live: bool) -> Result<()> {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = bilibi_database::pool(url)?;
    let modules = bilibi_module_registry::discover()?;
    bilibi_database::migrate(&db, &modules).await?;
    if !live {
        bilibi_walkthrough_fixture::install(&db).await?;
    }
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    services
        .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
        .set_enforced(false);
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    let authentication = services.get::<Authentication>("base/tenancy.authentication")?;
    let client = db.get().await?;
    let admin = authentication
        .authenticate(&**client, 1, 4, "KA1 fixture".into())
        .await?;
    let manager = authentication
        .authenticate(&**client, 1, 1, "KA1 fixture".into())
        .await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    let real_ports = services.get::<Ports>(bilibi_ports::SERVICE)?;
    let mut a = PortAuthorization {
        scope: &manager,
        platform_company_id: 1,
        workforce_source_id: NATIVE_SOURCE_ID,
        workforce_company_id: if live { 1 } else { 1001 },
        provider_id: NATIVE_PROVIDER_ID,
        capability: "employee_directory",
        direction: Direction::Read,
    };
    if live {
        let context = PortContext {
            client: (&client).into(),
            ports: &real_ports,
        };
        let records = pages(context, &a).await?;
        prove(
            records
                .first()
                .is_some_and(|r| r.kind == RecordKind::Company)
                && records
                    .iter()
                    .filter(|r| r.kind == RecordKind::Employee)
                    .count()
                    >= 3,
            "real Workforce supplies company and working employees",
        )?;
        a.capability = "organization_directory";
        pages(context, &a).await?;
        let connector = services.get::<Connector>(SERVICE)?;
        prove(
            connector.adapters().serves(NATIVE_PROVIDER_ID),
            "boot installs native adapter from inventory",
        )?;
        saved(
            connector
                .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
                .await,
        )?;
        saved(connector.set_enabled(&manager, 1, true).await)?;
        saved(
            connector
                .put_sync_policy(
                    &manager,
                    1,
                    SyncPolicyUpdate {
                        page_limit: Some(1),
                        ..Default::default()
                    },
                )
                .await,
        )?;
        let first = saved(
            connector
                .synchronise(&manager, 1, "ka1-live", SyncOptions::default())
                .await,
        )?;
        prove(
            first.state == RunState::Succeeded && first.applied >= 4 && first.refused == 0,
            "real mounted native pass applies the directory atomically",
        )?;
        let changes = saved(
            connector
                .synchronise(&manager, 1, "ka1-live-changes", SyncOptions::default())
                .await,
        )?;
        prove(
            changes.state == RunState::Succeeded && changes.pass == RunPass::Incremental,
            "changes pass reads a new complete native snapshot",
        )?;
        saved(
            connector
                .synchronise(
                    &manager,
                    1,
                    "ka1-live-full",
                    SyncOptions {
                        full: true,
                        ..Default::default()
                    },
                )
                .await,
        )?;
        return Ok(());
    }
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
    let mut contributions = vec![&PORT];
    contributions.extend(
        inventory::iter::<Contribution>.into_iter().filter(|c| {
            c.provenance.module == bilibi_people_connector_native_people_adapter::MODULE
        }),
    );
    let catalog = Catalog::build(&deployment, consumers, contributions)?;
    let ports = Arc::new(Ports::from_catalog(&catalog, &deployment)?);
    let context = PortContext {
        client: (&client).into(),
        ports: &ports,
    };
    let records = pages(context, &a).await?;
    prove(
        records.len() == 3
            && records[2].supervisor_stable_id.as_deref() == Some("10")
            && records[1].workforce_company_id == 1001,
        "separate company axes and supervisor identity survive mapping",
    )?;
    let r = PortRequest {
        pass: Pass::Changes,
        since: Some("observation-only".into()),
        cursor: None,
        limit: 1,
    };
    let p = saved(NativePeopleAdapter.read(context, &a, &r).await)?;
    let mut continued = PortRequest {
        cursor: p.next_cursor.clone(),
        ..r.clone()
    };
    let again = saved(NativePeopleAdapter.read(context, &a, &continued).await)?;
    prove(
        saved(NativePeopleAdapter.read(context, &a, &continued).await)? == again,
        "cursor replay is deterministic",
    )?;
    for bad in [
        PortAuthorization {
            provider_id: "foreign",
            ..a
        },
        PortAuthorization {
            workforce_source_id: "foreign",
            ..a
        },
        PortAuthorization {
            capability: "company_directory",
            ..a
        },
        PortAuthorization {
            direction: Direction::Write,
            ..a
        },
    ] {
        prove(
            matches!(
                NativePeopleAdapter.read(context, &bad, &r).await,
                Err(ReadRefusal::InvalidAuthorization)
            ),
            "foreign authorization is refused with fixed code",
        )?;
    }
    for bad in [
        PortRequest {
            limit: 2,
            ..continued.clone()
        },
        PortRequest {
            cursor: Some("broken".into()),
            ..continued.clone()
        },
    ] {
        prove(
            matches!(
                NativePeopleAdapter.read(context, &a, &bad).await,
                Err(ReadRefusal::InvalidCursor)
            ),
            "mismatched or malformed cursor is refused",
        )?;
    }
    prove(
        matches!(
            NativePeopleAdapter
                .read(
                    context,
                    &a,
                    &PortRequest {
                        limit: 0,
                        ..r.clone()
                    }
                )
                .await,
            Err(ReadRefusal::InvalidRequest)
        ),
        "zero page size is refused",
    )?;
    prove(
        matches!(
            NativePeopleAdapter
                .read(
                    context,
                    &PortAuthorization {
                        workforce_company_id: 2,
                        ..a
                    },
                    &r
                )
                .await,
            Err(ReadRefusal::MappingMismatch)
        ),
        "company mapping is checked without assuming equal axes",
    )?;
    prove(
        matches!(
            NativePeopleAdapter
                .read(
                    context,
                    &PortAuthorization {
                        platform_company_id: 100,
                        ..a
                    },
                    &r
                )
                .await,
            Err(ReadRefusal::NotFound)
        ),
        "foreign tenant company is refused",
    )?;
    for mode in [1, 2] {
        MODE.store(mode, Ordering::SeqCst);
        prove(
            matches!(
                NativePeopleAdapter.read(context, &a, &continued).await,
                Err(ReadRefusal::MappingMismatch)
            ),
            "foreign record source or company stops the page",
        )?;
    }
    MODE.store(3, Ordering::SeqCst);
    prove(
        matches!(
            saved(NativePeopleAdapter.read(context, &a, &r).await)?.freshness,
            Freshness::Stale(_)
        ),
        "stale company has no successful directory",
    )?;
    MODE.store(0, Ordering::SeqCst);
    continued.cursor = again.next_cursor;
    MODE.store(8, Ordering::SeqCst);
    prove(
        matches!(
            NativePeopleAdapter.read(context, &a, &continued).await,
            Err(ReadRefusal::InvalidCursor)
        ),
        "Workforce continuation refusal keeps fixed cursor code",
    )?;
    MODE.store(0, Ordering::SeqCst);
    a.capability = "organization_directory";
    let positions = pages(context, &a).await?;
    prove(
        positions.len() == 1
            && positions[0].parent_stable_id.as_deref() == Some("9")
            && positions[0].assignments[0].employee_stable_id == "10"
            && positions[0].version == Some(2)
            && positions[0].vacant == Some(false)
            && positions[0].assignments_incomplete == Some(true),
        "position, parent, version, vacancy and bounded assignment fields survive",
    )?;
    MODE.store(9, Ordering::SeqCst);
    prove(
        pages(context, &a).await?.len() == 2,
        "position scan continues with a fixed watermark",
    )?;
    prove(
        matches!(
            NativePeopleAdapter.read(context, &a, &continued).await,
            Err(ReadRefusal::InvalidCursor)
        ),
        "employee cursor cannot enter the organisation stream",
    )?;
    MODE.store(6, Ordering::SeqCst);
    prove(
        matches!(
            NativePeopleAdapter.read(context, &a, &r).await,
            Err(ReadRefusal::MappingMismatch)
        ),
        "login identity cannot substitute for employee identity",
    )?;
    MODE.store(0, Ordering::SeqCst);
    let connector = Connector::from_catalog(
        &services,
        &catalog,
        &deployment,
        Providers::installed(),
        ports.clone(),
    )?;
    saved(
        connector
            .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
            .await,
    )?;
    saved(connector.set_enabled(&manager, 1, true).await)?;
    saved(
        connector
            .put_sync_policy(
                &manager,
                1,
                SyncPolicyUpdate {
                    page_limit: Some(1),
                    ..Default::default()
                },
            )
            .await,
    )?;
    let first = saved(
        connector
            .synchronise(&manager, 1, "ka1-bootstrap", SyncOptions::default())
            .await,
    )?;
    prove(
        first.state == RunState::Succeeded && first.applied == 4 && first.refused == 0,
        "native adapter feeds the real sync engine",
    )?;
    let before = saved(connector.workforce(&manager, 1).await)?;
    let checkpoint = saved(connector.sync_summary(&manager, 1).await)?.checkpoint_version;
    for mode in [4, 5] {
        MODE.store(mode, Ordering::SeqCst);
        let run = saved(
            connector
                .synchronise(
                    &manager,
                    1,
                    &format!("ka1-unavailable-{mode}"),
                    SyncOptions::default(),
                )
                .await,
        )?;
        prove(
            run.state == RunState::Unavailable
                && run.applied == 0
                && saved(connector.workforce(&manager, 1).await)? == before
                && saved(connector.sync_summary(&manager, 1).await)?.checkpoint_version
                    == checkpoint,
            "unavailable employee or organisation discards every staged page; checkpoint stays put",
        )?;
    }
    let absent = Ports::default();
    let p = saved(
        NativePeopleAdapter
            .read(
                PortContext {
                    ports: &absent,
                    ..context
                },
                &a,
                &r,
            )
            .await,
    )?;
    prove(
        p.entries.is_empty() && matches!(p.freshness, Freshness::Unavailable(_)),
        "unmounted Workforce is an unavailable page, never an empty snapshot",
    )?;
    Ok(())
}
