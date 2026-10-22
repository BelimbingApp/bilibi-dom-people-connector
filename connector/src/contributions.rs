//! What the Connector adds to the shared catalogs (Bilimbi
//! `Connector.Contributions`): the module descriptor with its migration and
//! facade service, the `people-connector` Authz domain with its two
//! capabilities, the menu leaf under Administration › System › Integrations,
//! and the encrypted company setting that holds a provider credential.
//! Routes, the page writes and the other eighteen settings land with their
//! slices (port plan rows KC1 to KC6).
use crate::adapters::Adapters;
use crate::contracts::Providers;
use crate::{Connector, MODULE, SERVICE};
use bilibi_authz::{CapabilityDefinition as C, VocabularyContribution, VOCABULARY};
use bilibi_database::{Db, MutationBoundary};
use bilibi_menu::{MenuItem, Requirement, ITEMS};
use bilibi_module_registry::{Catalog, Contribution, Layer, Module, Provenance, Service};
use bilibi_ports::Ports;
use bilibi_settings::{Definition, ScopeKind, Setting, ValueType, DEFINITIONS};
use serde_json::Value;
use std::sync::Arc;

/// Opens the connections page and lists the companies the actor can reach.
pub const VIEW: &str = "people-connector.connections.view";
/// Every connection write: provider choice, credential, enable, remove.
pub const MANAGE: &str = "people-connector.connections.manage";
/// Every Connector capability.
pub const CAPABILITIES: [&str; 2] = [VIEW, MANAGE];

/// The encrypted company-scoped setting that holds a provider credential.
/// Written only through the facade, so it has no editor on the generic
/// settings screen and no reveal path; the facade reports only whether one
/// is stored.
pub const CREDENTIAL_KEY: &str = "people-connector.connection.credential";

/// The connections page every slice of the connector hangs on (route in KC2).
pub const CONNECTIONS_PATH: &str = "/integrations/people/connections";

macro_rules! provenance {
    () => {
        Provenance {
            module: MODULE,
            source: file!(),
            line: line!(),
        }
    };
}

inventory::submit! {
    Module {
        id: MODULE,
        layer: Layer::Domains,
        dependencies: &[
            "base/authz", "base/database", "base/menu", "base/module_registry", "base/party_ids",
            "base/ports", "base/settings", "base/tenancy", "core/company",
        ],
        migrations: &[(
            "people-connector-connector-001-connections",
            include_str!("../migrations/001-connections.sql"),
        )],
        services: &[Service {
            id: SERVICE,
            factory: |services| {
                // The adapter catalog is read here, as `base/ports` reads its
                // own: `Services::catalog` is set only after every factory ran.
                let modules = bilibi_module_registry::discover()?;
                let catalog = Catalog::discover(&modules)?;
                Ok(Arc::new(Connector {
                    db: services.get::<Db>("base/database.pool")?.as_ref().clone(),
                    mutations: services.get::<MutationBoundary>("base/database.mutations")?,
                    settings: services.get("base/settings.store")?,
                    ports: services.get::<Ports>(bilibi_ports::SERVICE)?,
                    adapters: Adapters::from_catalog(&catalog, &modules)?,
                    registry: Providers::installed(),
                }))
            },
        }],
    }
}

// The Authz domain is `people-connector` ("People integrations"), as Bilimbi
// declares it; a capability segment allows hyphens. `view` and `manage` are
// verbs Base already declares. Bilimbi grants neither key to a contributed
// role: an administrator grants them.
inventory::submit! {
    Contribution::new(VOCABULARY, "people_connector/connector.vocabulary", provenance!(), &VocabularyContribution {
        domains: &["people-connector"],
        verbs: &[],
        capabilities: &[C::tenant(VIEW), C::tenant(MANAGE)],
        roles: &[],
        system_principals: &[],
    })
}

// Administration › System › Integrations › People connections, with Bilimbi's
// id, label, order and capability; the route enforces the capability again.
inventory::submit! {
    Contribution::new(ITEMS, "admin.system.integrations.people-connections", provenance!(),
        &MenuItem::page("admin.system.integrations.people-connections", "People connections", CONNECTIONS_PATH)
            .under("admin.system.integrations")
            .requires(Requirement::Capability(VIEW))
            .order(50))
}

/// A non-empty text of at most 4,096 bytes, or null.
pub(crate) fn credential_valid(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(secret) => crate::secret_valid(secret),
        _ => false,
    }
}

inventory::submit! { Contribution::new(DEFINITIONS, CREDENTIAL_KEY, provenance!(), &Setting::Definition(Definition {
    value_type: ValueType::String, scopes: &[ScopeKind::Company], default: || Value::Null,
    nullable: true, encrypted: true, minimum: None, maximum: None,
    validator: Some(bilibi_settings::Validator { accepts: credential_valid, message: "Enter a credential of at most 4096 characters." }),
    editor: None,
})) }
