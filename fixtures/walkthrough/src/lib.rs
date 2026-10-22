//! `DATABASE_URL=… cargo run -p bilibi-people-connector-walkthrough`: the
//! People connector walkthrough fixture for slice proofs. Against a freshly
//! created disposable database it migrates the composed app, installs the
//! Platform walkthrough fixture (its tenants, accounts, Persons, affiliations
//! and companies), grants `manager@example.test` both connector capabilities
//! and `viewer@example.test` the view capability, prints the installed
//! providers and the adapters serving them, and drives the Connector facade:
//! the company's status, a provider choice by an account without the grant,
//! an uninstalled provider, the native provider, the credential paths, enable
//! and disable, removal, a sibling company without reach and another tenant's
//! company. While no mounted module answers the `people/workforce.directory`
//! port (People absent, or a People that does not yet provide it), every step
//! that needs workforce identity is refused with the connector's own
//! `people_unavailable` sentence and the walkthrough says so; with the port
//! mounted the same steps store, enable, disable and remove the connection.
//! It ends with PostgreSQL refusing what the facade never attempts: a second
//! connection for a platform company, a second platform company on one
//! workforce company, and a company of another tenant.
//!
//! A `bilibi-web run` against the same database then shows Administration ›
//! System › Integrations › People connections in the sidebar for the manager
//! (the page itself lands with port plan row KC2).
//!
//! Never a product dependency, and never run against a real install: the
//! Platform fixture refuses a database that already holds other accounts.
use bilibi_authz::grants::Grants;
use bilibi_database::{pool, Result};
use bilibi_module_registry::{verify_composition, Services};
use bilibi_people_connector_connector::{
    ConnectionState, Connector, ConnectorError, Status, MANAGE, NATIVE_PROVIDER_ID, SERVICE, VIEW,
};
use bilibi_tenancy::{Authentication, Scope};
use std::sync::Arc;

/// Tenant 1 (the platform operator), its primary company (001), its sibling
/// Company 002, and tenant 2's private company.
const TENANT: i64 = 1;
const COMPANY: i64 = 1;
const OTHER_COMPANY: i64 = 2;
const OTHER_TENANT_COMPANY: i64 = 100;
/// The fixture's accounts: the manager (affiliated with Company 001), the
/// viewer, the grant-all admin and the contractor (affiliated with Company
/// 002).
const MANAGER: i64 = 1;
const VIEWER: i64 = 2;
const ADMIN: i64 = 4;
const CONTRACTOR: i64 = 7;

fn describe_status(status: &Status) -> String {
    format!(
        "{} (platform company {}, workforce company {} of {}, provider {}, credential {})",
        status.state.label(),
        status.platform_company_id,
        status.workforce_company_id,
        status.workforce_source_id,
        status.provider_id.as_deref().unwrap_or("none"),
        if status.credential_stored {
            "stored"
        } else {
            "not stored"
        }
    )
}

fn show<T: std::fmt::Debug>(
    step: &str,
    result: std::result::Result<T, ConnectorError>,
) -> Option<T> {
    match result {
        Ok(value) => {
            println!("{step}: {value:?}");
            Some(value)
        }
        Err(error) => {
            println!("{step}: refused ({error})");
            None
        }
    }
}

fn show_status(step: &str, result: std::result::Result<Status, ConnectorError>) -> Option<Status> {
    match result {
        Ok(status) => {
            println!("{step}: {}", describe_status(&status));
            Some(status)
        }
        Err(error) => {
            println!("{step}: refused ({error})");
            None
        }
    }
}

/// Runs a statement the facade never makes and prints PostgreSQL's answer.
async fn refused(client: &tokio_postgres::Client, step: &str, sql: &str) {
    match client.execute(sql, &[]).await {
        Ok(rows) => println!("{step}: changed {rows} row(s)"),
        Err(error) => println!(
            "{step}: refused ({})",
            error
                .as_db_error()
                .map(|db| db.message().to_owned())
                .unwrap_or_else(|| error.to_string())
        ),
    }
}

/// The full connection lifecycle, reached only while the People directory
/// port answers for the company.
async fn lifecycle(connector: &Connector, manager: &Scope) {
    show(
        "manager stores a credential for the native provider",
        connector
            .put_credential(manager, COMPANY, "not-a-real-secret")
            .await,
    );
    show(
        "manager clears the credential of the native provider",
        connector.clear_credential(manager, COMPANY).await,
    );
    let enabled = show_status(
        "manager enables the connection",
        connector.set_enabled(manager, COMPANY, true).await,
    );
    if enabled.is_some_and(|status| status.state == ConnectionState::Enabled) {
        show_status(
            "manager chooses the native provider again (an unchanged mapping stays enabled)",
            connector
                .configure_connection(manager, COMPANY, NATIVE_PROVIDER_ID)
                .await,
        );
    }
    show_status(
        "manager disables the connection",
        connector.set_enabled(manager, COMPANY, false).await,
    );
    show(
        "manager removes the connection",
        connector.remove_connection(manager, COMPANY).await,
    );
    show_status(
        "manager reads the status after removal",
        connector.status(manager, COMPANY).await,
    );
}

/// Runs the walkthrough against `url`, a freshly created disposable database.
pub async fn run(url: &str) -> Result<()> {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = pool(url)?;
    let modules = bilibi_module_registry::discover()?;
    verify_composition(
        &modules,
        bilibi_domains::MODULES,
        bilibi_extensions::MODULES,
    )?;
    let migrated = bilibi_database::migrate(&db, &modules).await?;
    println!(
        "mounted Domain modules: {:?}; {} migration(s) applied",
        bilibi_domains::MODULES,
        migrated.applied.len()
    );
    bilibi_walkthrough_fixture::install(&db).await?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    // What this program is made of, for Base Software's status and reviews.
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    // Match the web host's documented development mode for this disposable
    // fixture; other environments retain the Platform's step-up enforcement.
    if std::env::var("BILIBI_ENV").as_deref() == Ok("development") {
        services
            .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
            .set_enforced(false);
    }
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    let authentication = services.get::<Authentication>("base/tenancy.authentication")?;
    let connector = services.get::<Connector>(SERVICE)?;

    let client = db.get().await?;
    let admin = authentication
        .authenticate(&**client, TENANT, ADMIN, "walkthrough".into())
        .await?;
    // Bilimbi grants the connector keys to no contributed role: an
    // administrator grants them, here directly.
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, MANAGER, key, Some(true)).await?;
    }
    grants.set_direct(&admin, VIEWER, VIEW, Some(true)).await?;
    println!("manager@example.test holds {VIEW} and {MANAGE}; viewer@example.test holds {VIEW}");
    let manager = authentication
        .authenticate(&**client, TENANT, MANAGER, "walkthrough".into())
        .await?;
    let viewer = authentication
        .authenticate(&**client, TENANT, VIEWER, "walkthrough".into())
        .await?;
    let contractor = authentication
        .authenticate(&**client, TENANT, CONTRACTOR, "walkthrough".into())
        .await?;

    // What this installation offers and serves.
    for provider in connector.registry().providers() {
        let declarations: Vec<String> = provider
            .capabilities
            .iter()
            .map(|capability| format!("{} ({:?})", capability.key, capability.direction))
            .collect();
        println!(
            "installed provider {} \"{}\" contract {} declaring {} with credential {:?}",
            provider.id,
            provider.name,
            provider.contract_version,
            declarations.join(", "),
            provider.credential
        );
    }
    let served: Vec<&str> = connector.adapters().provider_ids().collect();
    if served.is_empty() {
        println!("adapters serving providers: none (the native adapter lands with KA1)");
    } else {
        println!("adapters serving providers: {}", served.join(", "));
    }
    let people_available = connector.people_available();
    println!(
        "People directory port ({}) mounted: {people_available}",
        bilibi_people_connector_connector::DIRECTORY_PORT
    );

    // Reads and writes, in Bilimbi's order of checks.
    show_status(
        &format!("manager reads the status of company {COMPANY}"),
        connector.status(&manager, COMPANY).await,
    );
    show_status(
        "viewer chooses the native provider for company 001 (no manage grant)",
        connector
            .configure_connection(&viewer, COMPANY, NATIVE_PROVIDER_ID)
            .await,
    );
    show_status(
        "manager chooses an uninstalled provider for company 001",
        connector
            .configure_connection(&manager, COMPANY, "people.remote")
            .await,
    );
    let configured = show_status(
        "manager chooses the native provider for company 001",
        connector
            .configure_connection(&manager, COMPANY, NATIVE_PROVIDER_ID)
            .await,
    );
    if configured.is_some() {
        lifecycle(&connector, &manager).await;
    } else {
        println!(
            "(the connection paths above the port refusal cannot be reached until a mounted People provides {}; with it mounted this walkthrough stores, enables, disables and removes the connection)",
            bilibi_people_connector_connector::DIRECTORY_PORT
        );
        show(
            "manager stores a credential for company 001 without a connection",
            connector
                .put_credential(&manager, COMPANY, "not-a-real-secret")
                .await,
        );
        show_status(
            "manager enables company 001 without a connection",
            connector.set_enabled(&manager, COMPANY, true).await,
        );
        show(
            "manager removes the connection of company 001 without one",
            connector.remove_connection(&manager, COMPANY).await,
        );
    }
    show_status(
        &format!("contractor chooses the native provider for company {OTHER_COMPANY} (no grant)"),
        connector
            .configure_connection(&contractor, OTHER_COMPANY, NATIVE_PROVIDER_ID)
            .await,
    );
    show_status(
        &format!(
            "manager chooses the native provider for company {OTHER_COMPANY} (tenant-wide reach)"
        ),
        connector
            .configure_connection(&manager, OTHER_COMPANY, NATIVE_PROVIDER_ID)
            .await,
    );
    show_status(
        &format!(
            "manager chooses the native provider for company {OTHER_TENANT_COMPANY} (another tenant)"
        ),
        connector
            .configure_connection(&manager, OTHER_TENANT_COMPANY, NATIVE_PROVIDER_ID)
            .await,
    );
    show_status(
        &format!("viewer reads the status of company {COMPANY}"),
        connector.status(&viewer, COMPANY).await,
    );

    // What PostgreSQL refuses on its own, through statements the facade never
    // makes; the rows are removed again so the database is left as the facade
    // left it.
    refused(
        &client,
        "SQL inserts a connection for company 001",
        "INSERT INTO people_connector_connections \
         (tenant_id, platform_company_id, provider_id, provider_contract_version, workforce_source_id, workforce_company_id) \
         VALUES (1, 1, 'people.native', '1.0.0', 'people/native', 1)",
    )
    .await;
    refused(
        &client,
        "SQL inserts a second connection for company 001",
        "INSERT INTO people_connector_connections \
         (tenant_id, platform_company_id, provider_id, provider_contract_version, workforce_source_id, workforce_company_id) \
         VALUES (1, 1, 'people.native', '1.0.0', 'people/native', 1)",
    )
    .await;
    refused(
        &client,
        "SQL connects company 002 to workforce company 1 as well",
        "INSERT INTO people_connector_connections \
         (tenant_id, platform_company_id, provider_id, provider_contract_version, workforce_source_id, workforce_company_id) \
         VALUES (1, 2, 'people.native', '1.0.0', 'people/native', 1)",
    )
    .await;
    refused(
        &client,
        "SQL connects another tenant's company 100 in tenant 1",
        "INSERT INTO people_connector_connections \
         (tenant_id, platform_company_id, provider_id, provider_contract_version, workforce_source_id, workforce_company_id) \
         VALUES (1, 100, 'people.native', '1.0.0', 'people/native', 100)",
    )
    .await;
    refused(
        &client,
        "SQL removes the connection it inserted",
        "DELETE FROM people_connector_connections WHERE platform_company_id = 1",
    )
    .await;
    Ok(())
}

// KC4: file exchange proofs and the fixture-only directory port for browser interaction.
mod files;
pub use files::{file_walkthrough, FileCompanyPort};

// KC3: an independent signed-receipt proof on a disposable database.
#[path = "webhooks.rs"]
mod webhook_proof;
pub use webhook_proof::run as run_webhooks;
