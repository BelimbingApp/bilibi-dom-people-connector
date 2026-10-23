//! What the Connector adds to the shared catalogs (Bilimbi
//! `Connector.Contributions`): the module descriptor with its migration and
//! facade service, the `people-connector` Authz domain with its two
//! capabilities, the menu leaf under Administration › System › Integrations,
//! and the encrypted company setting that holds a provider credential.
//! Routes, the page writes and the other eighteen settings land with their
//! slices (port plan rows KC1 to KC6).
use crate::contracts::Providers;
use crate::{Connector, MODULE, SERVICE};
use bilibi_authz::{CapabilityDefinition as C, VocabularyContribution, VOCABULARY};
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
            "base/artifacts", "base/audit", "base/authz", "base/database", "base/datetime", "base/integration", "base/locale", "base/ui", "base/menu", "base/module_registry", "base/party_ids",
            "base/ports", "base/settings", "base/system", "base/tenancy", "core/company",
        ],
        migrations: &[(
            "people-connector-connector-001-connections",
            include_str!("../migrations/001-connections.sql"),
        ), (
            "people-connector-connector-002-sync",
            include_str!("../migrations/002-sync.sql"),
        ), (
            "people-connector-connector-003-webhooks",
            include_str!("../migrations/003-webhooks.sql"),
        ), (
            "people-connector-connector-004-file-exchanges",
            include_str!("../migrations/004-file-exchanges.sql"),
        ), (
            "people-connector-connector-005-retention-attempts",
            include_str!("../migrations/005-retention-attempts.sql"),
        ), (
            "people-connector-connector-006-backups",
            include_str!("../migrations/006-backups.sql"),
        )],
        services: &[Service {
            id: SERVICE,
            factory: |services| {
                // The adapter catalog is read here, as `base/ports` reads its
                // own: `Services::catalog` is set only after every factory ran.
                let modules = bilibi_module_registry::discover()?;
                let catalog = Catalog::discover(&modules)?;
                Ok(Arc::new(Connector::from_catalog(
                    services, &catalog, &modules, Providers::installed(),
                    services.get::<Ports>(bilibi_ports::SERVICE)?,
                )?))
            },
        }, Service {
            id: crate::WEBHOOK_SERVICE,
            factory: |services| Ok(Arc::new(bilibi_integration::webhooks::WebhookService(
                services.get::<Connector>(SERVICE)?,
            ))),
        }, Service {id:crate::connections_web::PAGE_ID,factory:crate::connections_web::pages_service}, Service {id:crate::operations_web::PAGE_ID,factory:crate::operations_web::pages_service}, Service {id:crate::backups_web::PAGE_ID,factory:crate::backups_web::pages_service}, Service {id:crate::files_web::PAGE_ID,factory:crate::files_web::pages_service}, Service {id:crate::files_web::DOWNLOAD_ID,factory:crate::files_web::download_service}],
    }
}

// Bilimbi Connector's descriptor-owned `people-native` inbound registration.
inventory::submit! {
    Contribution::new(bilibi_integration::webhooks::WEBHOOKS, "people_connector/connector.native", provenance!(),
        &bilibi_integration::webhooks::Webhook { identifier: crate::WEBHOOK_IDENTIFIER, service: crate::WEBHOOK_SERVICE })
}

// The policy is owner-encrypted on the connection row. Keep ciphertext out of
// every audit path too, including future changes that capture a whole row.
inventory::submit! {
    Contribution::new(bilibi_audit::REDACTIONS, "people_connector/connector.webhook-secret", provenance!(),
        &bilibi_audit::Redaction { kind: "people_connector_connection", fields: &["webhook_secret"] })
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

// Bilimbi Connector.Contributions: the three company sync policies.
macro_rules! sync_policy {
    ($key:literal,$default:literal,$min:literal,$max:literal,$label:literal,$help:literal) => {
        inventory::submit! { Contribution::new(DEFINITIONS,$key,provenance!(),&Setting::Definition(Definition {
            value_type: ValueType::Integer, scopes: &[ScopeKind::Company], default: || serde_json::json!($default),
            nullable: false, encrypted: false, minimum: Some($min as f64), maximum: Some($max as f64), validator: None,
            editor: Some(bilibi_settings::Editor { group: "People integrations",label:$label,help:$help,capability:Some(MANAGE),reveal_duration_ms:5000 }),
        })) }
    };
}
sync_policy!(
    "people-connector.sync.page_limit",
    250,
    1,
    1000,
    "Synchronisation page size",
    "Most directory records the provider returns on one page."
);
sync_policy!(
    "people-connector.sync.max_age_minutes",
    1440,
    5,
    43200,
    "Synchronised data maximum age",
    "Minutes after the last completed pass before synchronised records are stale."
);
sync_policy!(
    "people-connector.sync.run_timeout_minutes",
    30,
    1,
    1440,
    "Synchronisation run timeout",
    "Minutes a pass may run before its outcome is recorded as unknown."
);

// KC4: Bilimbi's five company file-exchange settings.
macro_rules! file_setting {
    ($key:literal,$kind:ident,$default:expr,$min:expr,$max:expr,$label:literal,$help:literal) => {
        inventory::submit! { Contribution::new(DEFINITIONS,$key,provenance!(),&Setting::Definition(Definition {
            value_type:ValueType::$kind,scopes:&[ScopeKind::Company],default:||serde_json::json!($default),
            nullable:false,encrypted:false,minimum:$min,maximum:$max,validator:None,
            editor:Some(bilibi_settings::Editor{group:"People integrations",label:$label,help:$help,capability:Some(MANAGE),reveal_duration_ms:5000}),
        })) }
    };
}
file_setting!(
    "people-connector.files.enabled",
    Boolean,
    false,
    None,
    None,
    "File exchange",
    "Allow directory files to be imported and exported."
);
file_setting!(
    "people-connector.files.json_enabled",
    Boolean,
    true,
    None,
    None,
    "Directory JSON format",
    "Allow directory files in JSON format."
);
file_setting!(
    "people-connector.files.max_bytes",
    Integer,
    1048576,
    Some(1.0),
    Some(10485760.0),
    "Maximum file bytes",
    "Largest directory file that can be exchanged."
);
file_setting!(
    "people-connector.files.max_records",
    Integer,
    1000,
    Some(1.0),
    Some(100000.0),
    "Maximum directory records",
    "Most records that one directory file may contain."
);
file_setting!(
    "people-connector.files.stale_minutes",
    Integer,
    15,
    Some(1.0),
    Some(1440.0),
    "Abandon in-progress exchanges after (minutes)",
    "Mark an unfinished file exchange as abandoned after this time."
);

// KC5: Bilimbi Connector.Contributions retention policy, unset periods keep history.
macro_rules! retention_setting {
    ($name:literal,$default:expr,$nullable:literal,$max:literal,$label:literal,$help:literal) => {
        inventory::submit! { Contribution::new(DEFINITIONS,concat!("people-connector.retention.",$name),provenance!(),&Setting::Definition(Definition {
            value_type:ValueType::Integer,scopes:&[ScopeKind::Company],default:||serde_json::json!($default),
            nullable:$nullable,encrypted:false,minimum:Some(1.0),maximum:Some($max as f64),validator:None,
            editor:Some(bilibi_settings::Editor{group:"People integrations",label:$label,help:$help,capability:Some(MANAGE),reveal_duration_ms:5000}),
        })) }
    };
}
retention_setting!(
    "sync_days",
    Value::Null,
    true,
    3650,
    "Sync runs (days)",
    "Keep sync history for this many days; blank keeps records."
);
retention_setting!(
    "webhook_days",
    Value::Null,
    true,
    3650,
    "Webhook deliveries (days)",
    "Keep delivery history for this many days; blank keeps records."
);
retention_setting!(
    "file_days",
    Value::Null,
    true,
    3650,
    "File receipts (days)",
    "Keep expired file receipts for this many days; blank keeps records."
);
retention_setting!(
    "batch_size",
    100,
    false,
    1000,
    "Batch size per record type",
    "Most records of each type removed by one purge."
);
retention_setting!(
    "retry_minutes",
    60,
    false,
    1440,
    "Retry delay (minutes)",
    "Wait this many minutes before retrying a failed removal."
);

// KC6: Bilimbi's backup policy; changes affect newly created receipts/previews.
sync_policy!(
    "people-connector.backup.retention_days",
    30,
    1,
    3650,
    "Backup retention (days)",
    "Days a new connection backup remains available."
);
sync_policy!(
    "people-connector.backup.preview_minutes",
    10,
    1,
    60,
    "Backup preview validity (minutes)",
    "Minutes a restore preview remains valid."
);
