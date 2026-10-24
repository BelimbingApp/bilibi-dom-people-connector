//! KU1: company-target and refusal checks through the installed live page services.
//! Run after People's workforce-settings setup and the Connector's mounted sweep.
use bilibi_authz::grants::Grants;
use bilibi_database::{pool, Error};
use bilibi_module_registry::Services;
use bilibi_people_connector_connector::{Connector, MANAGE, SERVICE};
use bilibi_tenancy::{Authentication, Scope};
use bilibi_ui::page::{LiveBody, LiveInput, LiveRequest, LiveState, PageRequest, PageService};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn prove(condition: bool, label: &str) -> Result<()> {
    if !condition {
        return Err(format!("KU1 FAILED: {label}").into());
    }
    println!("KU1 proved: {label}");
    Ok(())
}
async fn mount(
    services: &Services,
    scope: &Scope,
    operation: &'static str,
    company: &str,
) -> Result<Box<dyn LiveState<Scope, Error>>> {
    let page = services.get::<PageService<Scope, Error>>(&format!(
        "people_connector/connector.{operation}_pages"
    ))?;
    Ok(page
        .0
        .mount(PageRequest {
            operation,
            scope: Some(scope),
            path: &HashMap::new(),
            query: &HashMap::from([("company_id".into(), company.into())]),
            body: &[],
            framed: false,
        })
        .await?
        .ok_or("live page missing")?)
}
async fn event(
    page: &mut dyn LiveState<Scope, Error>,
    scope: &Scope,
    payload: Value,
) -> Result<LiveBody> {
    Ok(page
        .update(LiveRequest {
            scope,
            credential: "KU1 disposable proof",
            input: LiveInput::Event(&payload),
        })
        .await?)
}

pub(super) async fn run(url: &str) -> Result<()> {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = pool(url)?;
    let modules = bilibi_module_registry::discover()?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    services
        .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
        .set_enforced(false);
    let client = db.get().await?;
    let auth = services.get::<Authentication>("base/tenancy.authentication")?;
    let admin = auth
        .authenticate(&**client, 1, 4, "KU1 proof".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "KU1 proof".into())
        .await?;
    let grants = services.get::<Grants>("base/authz.grants")?;
    let connector = services.get::<Connector>(SERVICE)?;

    // Explicit unavailable selections cannot become writes on a default company.
    for operation in ["connections", "files", "operations", "backups"] {
        for company in ["", "invalid", "100", "999999"] {
            let mut page = mount(&services, &manager, operation, company).await?;
            let result = event(page.as_mut(), &manager, json!({"type":"clear-flash"})).await?;
            prove(
                result.query.get("company_id").is_some_and(String::is_empty)
                    && result.html.contains("value=\"\" selected")
                    && !result.html.contains("id=\"people-files-export\"")
                    && !result.html.contains("id=\"people-backups-create\""),
                &format!("{operation} keeps an explicit unavailable company unselected"),
            )?;
            let reloaded = page
                .update(LiveRequest {
                    scope: &manager,
                    credential: "KU1 disposable proof",
                    input: LiveInput::Params(&result.query),
                })
                .await?;
            prove(
                reloaded
                    .query
                    .get("company_id")
                    .is_some_and(String::is_empty),
                &format!("{operation} preserves the unavailable selection on reload"),
            )?;
        }
    }
    let mut files = mount(&services, &manager, "files", "100").await?;
    let before = connector.file_summary(&manager, 1).await?.records.len();
    let result = event(files.as_mut(), &manager, json!({"type":"export"})).await?;
    prove(
        result
            .html
            .contains("No company is available for file exchange.")
            && connector.file_summary(&manager, 1).await?.records.len() == before,
        "unavailable file company cannot export for the default company",
    )?;
    let mut backups = mount(&services, &manager, "backups", "1").await?;
    let initial = event(backups.as_mut(), &manager, json!({"type":"clear-flash"})).await?;
    prove(
        initial.html.contains("data-id=\"1\""),
        "backup editors carry the selected company",
    )?;
    let first = connector.backups().summary(&manager, 1).await?.policy;
    let second = connector.backups().summary(&manager, 2).await?.policy;
    event(
        backups.as_mut(),
        &manager,
        json!({"type":"select_company","company_id":"2"}),
    )
    .await?;
    let delayed = event(
        backups.as_mut(),
        &manager,
        json!({"type":"save_policy","id":"1","field":"preview_minutes","value":"59"}),
    )
    .await?;
    prove(
        delayed.html.contains("Select a company you may manage."),
        "delayed backup draft refuses after company switch",
    )?;
    for (company, before) in [(1, first), (2, second)] {
        let after = connector.backups().summary(&manager, company).await?.policy;
        prove(
            after.preview_minutes == before.preview_minutes
                && after.retention_days == before.retention_days,
            "delayed backup edit preserves both companies",
        )?;
    }
    event(
        backups.as_mut(),
        &manager,
        json!({"type":"save_policy","id":"2","field":"preview_minutes","value":"59"}),
    )
    .await?;
    prove(
        connector
            .backups()
            .summary(&manager, 2)
            .await?
            .policy
            .preview_minutes
            == 59,
        "current-company backup inline edit still saves",
    )?;
    event(
        backups.as_mut(),
        &manager,
        json!({"type":"save_policy","id":"2","field":"preview_minutes","value":"61"}),
    )
    .await?;
    prove(
        connector
            .backups()
            .summary(&manager, 2)
            .await?
            .policy
            .preview_minutes
            == 59,
        "invalid backup policy is refused without mutation",
    )?;
    connector
        .backups()
        .configure(&manager, 2, second.into())
        .await?;

    // Events recheck a grant revoked after mount and stay live with a visible refusal.
    for (operation, payload, refusal) in [
        (
            "connections",
            json!({"type":"set_enabled","enabled":"false"}),
            "You cannot change connections",
        ),
        (
            "files",
            json!({"type":"export"}),
            "You cannot change connections",
        ),
        (
            "operations",
            json!({"type":"run_doctor"}),
            "must have permission",
        ),
        (
            "backups",
            json!({"type":"backup"}),
            "management permission is required",
        ),
    ] {
        let mut page = mount(&services, &manager, operation, "1").await?;
        grants.set_direct(&admin, 1, MANAGE, Some(false)).await?;
        let result = event(page.as_mut(), &manager, payload).await;
        grants.set_direct(&admin, 1, MANAGE, Some(true)).await?;
        prove(
            result?.html.contains(refusal),
            &format!("{operation} refuses a revoked manager without a fatal live error"),
        )?;
    }
    Ok(())
}
