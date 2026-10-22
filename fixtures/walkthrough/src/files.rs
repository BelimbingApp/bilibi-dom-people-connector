//! KC4 disposable-database proof of Bilimbi Connector.FileExchange. No product registrations.
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
pub struct FileCompanyPort;
impl PortProvider for FileCompanyPort {
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
        provider: &FileCompanyPort,
    },
);
struct Directory;
impl ReadPort for Directory {
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
            } else if auth.capability == "company_directory" {
                vec!["company"]
            } else {
                vec!["employee"]
            };
            let entries=kinds.into_iter().map(|kind|{let mut value=json!({"kind":kind,"source_id":"people/native","stable_id":kind,"workforce_company_id":1001,"name":"Fixture directory fact","code":"FIXTURE","email":null,"supervisor_stable_id":null,"observed_at":at.to_string(),"active":true});if kind=="position"{value["parent_stable_id"]=Value::Null;value["version"]=json!(1);value["vacant"]=json!(true);value["assignments_incomplete"]=json!(false);value["assignments"]=json!([]);}value}).collect();
            Ok(Page {
                entries,
                next_cursor: None,
                resume_cursor: Some("files-fixture".into()),
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
        port: &Directory,
    },
);
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(format!("FAIL {message}").into());
    }
    println!("PASS {message}");
    Ok(())
}
fn wire(records: Value) -> Value {
    json!({"format":"people-directory-v1","tenant_id":1,"platform_company_id":1,"workforce_source_id":"people/native","workforce_company_id":1001,"records":records})
}
async fn import(c: &Connector, s: &Scope, file: &Value) -> Result<FileExchange> {
    Ok(c.import_file(s, 1, &serde_json::to_vec(file)?).await?)
}
/// Run only against the task's disposable database and private evidence directory.
pub async fn file_walkthrough() -> Result<()> {
    let url = std::env::var("DATABASE_URL")?;
    let root = std::env::var("BILIBI_FILES_PROOF_ROOT")?;
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
        .authenticate(&**client, 1, 4, "files-fixture".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "files-fixture".into())
        .await?;
    let viewer = auth
        .authenticate(&**client, 1, 2, "files-fixture".into())
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
        .begin(&mut client, admin.mutation_context("files.fixture.storage"))
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
    connector.set_enabled(&manager, 1, true).await?;
    let policy = FilePolicy {
        enabled: true,
        json_enabled: true,
        max_bytes: 1_048_576,
        max_records: 1000,
        stale_minutes: 1,
    };
    require(
        matches!(
            connector.put_file_policy(&viewer, 1, policy).await,
            Err(ConnectorError::Unauthorized)
        ),
        "viewer cannot configure file exchange",
    )?;
    connector.put_file_policy(&manager, 1, policy).await?;
    require(
        connector.export_file(&manager, 1).await.is_err(),
        "export requires current directory",
    )?;
    connector
        .synchronise(&manager, 1, "files-bootstrap", SyncOptions::default())
        .await?;
    let exported = connector.export_file(&manager, 1).await?;
    let bytes = connector
        .download_file(&manager, 1, &exported.receipt.id)
        .await?
        .bytes;
    std::fs::write(format!("{root}/../directory-v2.json"), &bytes)?;
    let file: Value = serde_json::from_slice(&bytes)?;
    require(
        file["format"] == "people-directory-v2"
            && file["records"].as_array().is_some_and(|r| r.len() == 3),
        "export chooses v2 with positions and exact envelope",
    )?;
    // Fixture-only projection shape switch proves the immutable v1 export branch.
    client.execute("UPDATE people_connector_workforce_records SET active=false WHERE tenant_id=1 AND kind='position'", &[]).await?;
    let v1_export = connector.export_file(&manager, 1).await?;
    let v1_bytes = connector
        .download_file(&manager, 1, &v1_export.receipt.id)
        .await?
        .bytes;
    let v1_file: Value = serde_json::from_slice(&v1_bytes)?;
    require(
        v1_file["format"] == "people-directory-v1"
            && v1_file["records"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r.as_object().unwrap().len() == 10),
        "export without positions chooses v1 and omits every organisation field",
    )?;
    client.execute("UPDATE people_connector_workforce_records SET active=true WHERE tenant_id=1 AND kind='position'", &[]).await?;
    let replay = connector.export_file(&manager, 1).await?;
    require(
        replay.replayed && replay.receipt.id == exported.receipt.id,
        "identical export replays receipt without another artifact",
    )?;
    let imported = connector.import_file(&manager, 1, &bytes).await?;
    require(
        imported.receipt.id != exported.receipt.id,
        "same bytes in another direction get a separate receipt",
    )?;
    require(
        connector.import_file(&manager, 1, &bytes).await?.replayed,
        "identical import replays",
    )?;
    let cp_before = connector
        .sync_summary(&manager, 1)
        .await?
        .checkpoint_version;
    let empty = wire(json!([]));
    let v1 = import(&connector, &manager, &empty).await?;
    require(
        !v1.replayed
            && connector
                .sync_summary(&manager, 1)
                .await?
                .checkpoint_version
                == cp_before,
        "v1 import retained without advancing checkpoint",
    )?;
    let mut invalid = empty.clone();
    invalid["extra"] = json!(true);
    require(
        import(&connector, &manager, &invalid).await.is_err(),
        "extra envelope field refused",
    )?;
    invalid = empty.clone();
    invalid["workforce_company_id"] = json!(1);
    require(
        import(&connector, &manager, &invalid).await.is_err(),
        "platform and workforce company identities cannot be substituted",
    )?;
    invalid = file.clone();
    invalid["format"] = json!("people-directory-v1");
    require(
        import(&connector, &manager, &invalid).await.is_err(),
        "v1 positions refused",
    )?;
    invalid = file.clone();
    invalid["records"][0]["extra"] = json!(true);
    require(
        import(&connector, &manager, &invalid).await.is_err(),
        "extra record field refused",
    )?;
    invalid = file.clone();
    let duplicate = invalid["records"][0].clone();
    invalid["records"].as_array_mut().unwrap().push(duplicate);
    require(
        import(&connector, &manager, &invalid).await.is_err(),
        "duplicate kind and stable ID refused",
    )?;
    invalid = file.clone();
    invalid["records"][0]["active"] = json!("true");
    require(
        import(&connector, &manager, &invalid).await.is_err(),
        "record field type refused",
    )?;
    let duplicate = String::from_utf8(bytes.clone())?.replacen(
        "\"format\":",
        "\"format\":\"people-directory-v2\",\"format\":",
        1,
    );
    require(
        connector
            .import_file(&manager, 1, duplicate.as_bytes())
            .await
            .is_err(),
        "duplicate JSON object keys refused",
    )?;
    require(
        connector
            .download_file(&viewer, 1, &imported.receipt.id)
            .await
            .is_err(),
        "read rechecks manager capability",
    )?;
    require(
        connector
            .download_file(&manager, 2, &imported.receipt.id)
            .await
            .is_err(),
        "cross-company download refused",
    )?;
    connector
        .put_file_policy(
            &manager,
            1,
            FilePolicy {
                max_bytes: 1,
                ..policy
            },
        )
        .await?;
    require(
        connector.import_file(&manager, 1, &bytes).await.is_err(),
        "byte policy enforced before storage",
    )?;
    connector.put_file_policy(&manager, 1, policy).await?;
    // Disposable fixture clock and lifecycle injection; product writes always use the boundary.
    client.execute("UPDATE people_connector_file_exchanges SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=1 AND id=$1::text::uuid",&[&imported.receipt.id]).await?;
    require(
        connector
            .download_file(&manager, 1, &imported.receipt.id)
            .await
            .is_err(),
        "expired receipt cannot download",
    )?;
    require(
        connector.import_file(&manager, 1, &bytes).await?.receipt.id != imported.receipt.id,
        "expired replay creates a fresh receipt",
    )?;
    let abandoned = wire(json!([]));
    let abandoned_bytes = serde_json::to_vec_pretty(&abandoned)?;
    let abandoned_receipt = connector.import_file(&manager, 1, &abandoned_bytes).await?;
    client.execute("UPDATE people_connector_file_exchanges SET state='pending',artifact_id=NULL,expires_at=NULL,updated_at=clock_timestamp()-interval '2 minutes' WHERE tenant_id=1 AND id=$1::text::uuid",&[&abandoned_receipt.receipt.id]).await?;
    let next = connector.import_file(&manager, 1, &abandoned_bytes).await?;
    require(
        next.receipt.id != abandoned_receipt.receipt.id,
        "abandoned receipt superseded",
    )?;
    client.execute("UPDATE people_connector_file_exchanges SET state='pending',artifact_id=NULL,expires_at=NULL WHERE tenant_id=1 AND id=$1::text::uuid",&[&next.receipt.id]).await?;
    require(
        matches!(
            connector.import_file(&manager, 1, &abandoned_bytes).await,
            Err(ConnectorError::FileExchangeInProgress)
        ),
        "active pending replay refused",
    )?;
    client.execute("UPDATE people_connector_file_exchanges SET state='failed' WHERE tenant_id=1 AND id=$1::text::uuid",&[&next.receipt.id]).await?;
    require(
        connector
            .import_file(&manager, 1, &abandoned_bytes)
            .await?
            .receipt
            .id
            == next.receipt.id,
        "failed storage retries same receipt",
    )?;
    REMAPPED.store(true, Ordering::SeqCst);
    require(
        connector
            .download_file(&manager, 1, &v1.receipt.id)
            .await
            .is_err(),
        "mapping change closes historical downloads",
    )?;
    REMAPPED.store(false, Ordering::SeqCst);
    connector.set_enabled(&manager, 1, false).await?;
    require(
        connector
            .download_file(&manager, 1, &v1.receipt.id)
            .await
            .is_err(),
        "disabled connection cannot download",
    )?;
    connector.set_enabled(&manager, 1, true).await?;
    // Expire Base-owned bytes on the disposable database to exercise its real purge.
    client.execute("UPDATE base_artifacts SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=1 AND company_id=1 AND owner_id=$1",&[&MODULE]).await?;
    connector.remove_connection(&manager, 1).await?;
    let purged = connector.purge_expired_files(&manager, 1).await?;
    require(
        purged.failed.is_empty() && !purged.purged.is_empty(),
        "expired bytes purged after disconnect while receipts remain",
    )?;
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    connector.set_enabled(&manager, 1, true).await?;
    require(
        connector
            .download_file(&manager, 1, &v1.receipt.id)
            .await
            .is_err(),
        "recreated connection cannot reopen prior receipts",
    )?;
    let summary = connector.file_summary(&manager, 1).await?;
    require(
        !summary.records.is_empty(),
        "receipt history survives connection removal and cleanup",
    )?;
    // Leave one ready directory document for browser download/upload interaction.
    connector
        .synchronise(
            &manager,
            1,
            "files-browser-bootstrap",
            SyncOptions::default(),
        )
        .await?;
    connector.export_file(&manager, 1).await?;
    println!("File exchange proof complete; fixture database ready for the Files page.");
    Ok(())
}
