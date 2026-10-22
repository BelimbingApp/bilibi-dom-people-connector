//! Connector (`people_connector/connector`, Bilimbi
//! `Bilimbi.PeopleConnector.Connector`): the provider-neutral boundary of the
//! People connector. It owns the integration contracts, each company's
//! provider connection with both company axes, and the read-port seam the
//! adapter modules serve.
//!
//! This is the module's one facade. Pages, operations and other modules call
//! [`Connector`], resolved from the registry as [`SERVICE`], with the sealed
//! [`Scope`] they were handed and an explicit platform company id (a Core
//! Company in that tenant); the schema, SQL and helpers stay private.
//!
//! People Workforce is reached through the `people/workforce.directory` port
//! ([`people`]), never a Cargo or descriptor edge: it validates the company
//! and answers its separate workforce-company identity. A stored connection
//! records both axes and is refused as `MappingChanged` once they stop
//! matching Workforce. While no mounted module answers the port, every call
//! that needs workforce identity is [`ConnectorError::PeopleUnavailable`]
//! (beside Bilimbi's `adapter_unavailable`), so the connector composes and
//! boots without People and says so.
//!
//! Reads follow Workforce's read policy: `status` first asks the port for the
//! company and is refused when the provider refuses it. Every write also needs
//! the scope's signed-in actor to hold `people-connector.connections.manage`
//! with Core Company reach to the target (the actor's own company, or a
//! same-tenant sibling with tenant-wide company reach), authorized inside the
//! facade with `organisation::authorize_target` and performed through
//! `MutationBoundary` under the company's writable lock. A provider credential
//! is the encrypted company-scoped Base Setting [`CREDENTIAL_KEY`]: the facade
//! stores or clears it and reports only whether one exists.
//!
//! Synchronisation reserves one idempotent pass, reads every declared directory
//! stream outside a transaction and applies all accepted facts atomically.
//! `workforce` carries freshness; `sync_summary` includes the checkpoint,
//! last run, issues and policy. Only `synchronise` invokes adapters.
//!
//! See `README.md`.
mod backup;
mod backup_store;
mod backups_web;
mod connections;
mod file_format;
mod file_store;
mod files;
mod files_web;
pub use backup::{
    Backup, BackupChange, BackupPolicy, BackupPreview, BackupPurge, BackupReceipt, BackupRestore,
    BackupSummary,
};
pub use files::{FileExchange, FilePolicy, FileReceipt, FileSummary};
mod contributions;
mod sync;
mod sync_store;
mod sync_types;
pub use sync::POLICY_FIELDS;
pub use sync_types::*;

pub mod adapters;
pub mod contracts;
mod people;

pub use adapters::{
    Adapter, Adapters, Freshness, Page, Pass, PortAuthorization, PortContext, PortRequest,
    ReadFuture, ReadPort, ReadRefusal, WriteFuture, WritePort, ADAPTERS,
};
pub use contracts::{
    Capability, ContractError, Credential, Direction, Provider, Providers, Registry,
    CAPABILITY_KEYS, NATIVE_PROVIDER_ID,
};
pub use contributions::{CAPABILITIES, CONNECTIONS_PATH, CREDENTIAL_KEY, MANAGE, VIEW};
pub use people::{Read, Reference, WorkforceCompany, DIRECTORY_PORT, NATIVE_SOURCE_ID};

use bilibi_company::{organisation, WriteError};
use bilibi_database::{Db, Error, MutationBoundary, MutationTransaction};
use bilibi_party_ids::OrganisationId;
use bilibi_ports::Ports;
use bilibi_settings::{Settings, Target};
use bilibi_tenancy::Scope;
use connections::{Connection, Mapping};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_postgres::GenericClient;

/// The module ID, Bilimbi's, kept because IDs are persisted data.
pub const MODULE: &str = "people_connector/connector";
/// The registry service ID of the [`Connector`] facade.
pub const SERVICE: &str = "people_connector/connector.connector";
/// The most bytes a provider credential may hold.
pub const CREDENTIAL_MAX_BYTES: usize = 4096;

pub type Result<T> = std::result::Result<T, ConnectorError>;

/// Why a call did not happen: Bilimbi's refusal atoms, each with the sentence
/// its connections page shows for it.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("The backup operation was refused. Check access, connection health and private artifact storage, then preview again.")]
    InvalidBackup,
    #[error("Enter a whole number within the stated range.")]
    InvalidBackupPolicy,
    #[error("Configure a native connection on People connections first.")]
    ConnectionUnavailable,
    #[error(
        "The backup belongs to a different connection or mapping. Use a backup of this connection."
    )]
    ConnectionChanged,
    #[error("Connection state changed after the preview. Preview the backup again.")]
    PreviewChanged,
    #[error("The preview expired. Preview the backup again.")]
    PreviewExpired,
    #[error("The backup operation was refused. Check access, connection health and private artifact storage, then preview again.")]
    InvalidConfirmation,
    #[error("The backup operation was refused. Check access, connection health and private artifact storage, then preview again.")]
    ConfirmationRequired,
    #[error("The backup operation was refused. Check access, connection health and private artifact storage, then preview again.")]
    AlreadyRestored,
    #[error("Restore this backup before requesting recovery.")]
    RestoreRequired,
    #[error("Backup integrity failed. Use another backup and review private storage.")]
    BackupIntegrity,
    #[error(
        "Private storage could not remove the backup. Review private storage settings and retry."
    )]
    BackupCleanupPending,
    /// A denied or unknown capability, or a company outside the actor's reach.
    #[error("You cannot change connections for this company. Ask an administrator for access.")]
    Unauthorized,
    /// A company the scope cannot see (missing, deleted, another tenant's),
    /// or no workforce identity for it.
    #[error("This company has no workforce identity. Choose another company.")]
    NotFound,
    #[error("This company is archived and read-only")]
    CompanyArchived,
    /// Workforce identity that is stale or unavailable is never used as a
    /// mapping.
    #[error("{}", not_current_sentence(.0))]
    NotCurrent(Freshness),
    /// No mounted module answers the People directory port.
    #[error("People is not mounted. Connection information cannot be shown.")]
    PeopleUnavailable,
    /// The stored mapping no longer matches what Workforce reports.
    #[error("The workforce mapping changed. Choose the provider again first.")]
    MappingChanged,
    /// No installed provider with that id.
    #[error("That provider is not installed. Choose another.")]
    Unsupported,
    /// The company has no connection.
    #[error("Choose a provider for this company first.")]
    Disconnected,
    #[error("This provider does not use a credential.")]
    CredentialNotRequired,
    #[error("Save the provider's credential before enabling the connection.")]
    CredentialMissing,
    #[error("Enter a credential of at most 4096 characters.")]
    InvalidCredential,
    #[error("Another company is already connected to this workforce company.")]
    WorkforceCompanyTaken,
    #[error("Someone else changed this connection. Review it and try again.")]
    Conflict,
    #[error("No adapter serves this provider yet, so it cannot be synchronised.")]
    AdapterUnavailable,
    #[error("A synchronisation is already running for this company. Try again when it finishes.")]
    SyncInProgress,
    #[error(
        "Enter a request key of 1–100 letters, digits, dots, underscores, colons or hyphens, starting with a letter or digit."
    )]
    InvalidIdempotencyKey,
    #[error("Enter whole numbers within the ranges shown.")]
    InvalidPolicy(&'static str),
    #[error("The checkpoint changed. Review it and try again.")]
    CheckpointMoved,
    /// A database or capture failure; never a refusal.
    #[error(transparent)]
    Failed(Error),
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    InvalidFile,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    InvalidFilePolicy,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    FileTooLarge,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    TooManyRecords,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    MissingFile,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    PurgeIncomplete,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    FileExchangeDisabled,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    FileExchangeInProgress,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    FileExchangeAbandoned,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    DirectoryUnavailable,
    #[error(
        "File exchange refused. Check the connection, file policy and private storage settings."
    )]
    FileStorage(#[from] bilibi_artifacts::Error),
}
impl From<Error> for ConnectorError {
    fn from(error: Error) -> Self {
        match error {
            Error::Forbidden | Error::Unauthorized => Self::Unauthorized,
            Error::NotFound => Self::NotFound,
            Error::Conflict => Self::Conflict,
            other => Self::Failed(other),
        }
    }
}
impl From<ContractError> for ConnectorError {
    fn from(_: ContractError) -> Self {
        Self::Unsupported
    }
}
impl From<bilibi_party_ids::InvalidId> for ConnectorError {
    fn from(_: bilibi_party_ids::InvalidId) -> Self {
        Self::NotFound
    }
}

/// Bilimbi's `workforce_notice` for a read that is not current.
fn not_current_sentence(freshness: &Freshness) -> &'static str {
    match freshness {
        Freshness::Stale(_) => {
            "Workforce identity is out of date. Connection information is unavailable."
        }
        _ => "Workforce identity is unavailable. Connection information cannot be shown.",
    }
}

/// A company's connection state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionState {
    /// No stored connection.
    Disconnected,
    Disabled,
    Enabled,
}
impl ConnectionState {
    /// The label the connections page shows.
    pub fn label(self) -> &'static str {
        match self {
            Self::Disconnected => "Not connected",
            Self::Disabled => "Disabled",
            Self::Enabled => "Enabled",
        }
    }
}

/// A scoped connection status with both company identity axes (Bilimbi
/// `Connector.Status`). `platform_company_id` is the Core Company in the
/// caller's tenant; `workforce_company_id` the People Workforce company it
/// maps to. `credential_stored` reports only whether a secret exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Status {
    pub state: ConnectionState,
    pub platform_company_id: i64,
    pub workforce_company_id: i64,
    pub workforce_source_id: String,
    pub provider_id: Option<String>,
    pub credential_stored: bool,
}
impl Status {
    /// A disconnected status from current workforce identity; a stale or
    /// unavailable read is refused before its mapping is used.
    fn from_workforce(read: Read<WorkforceCompany>) -> Result<Self> {
        let company = read.require_current()?;
        Ok(Self {
            state: ConnectionState::Disconnected,
            platform_company_id: company.platform_company_id,
            workforce_company_id: company.workforce_company_id,
            workforce_source_id: company.reference.source_id,
            provider_id: None,
            credential_stored: false,
        })
    }
}

/// The facade, resolved from the registry as [`SERVICE`].
pub struct Connector {
    db: Db,
    mutations: Arc<MutationBoundary>,
    settings: Arc<Settings>,
    ports: Arc<Ports>,
    artifacts: Arc<bilibi_artifacts::Artifacts>,
    adapters: Adapters,
    registry: Registry,
}

impl Connector {
    /// Constructs the facade for an explicit deployment catalog. The normal
    /// service factory uses the installed catalog and provider declarations.
    /// Adapters still require validated catalog contributions; every call
    /// retains the same authorization, mapping and mutation boundaries.
    pub fn from_catalog(
        services: &bilibi_module_registry::Services,
        catalog: &bilibi_module_registry::Catalog,
        modules: &[&bilibi_module_registry::Module],
        registry: Registry,
        ports: Arc<Ports>,
    ) -> bilibi_module_registry::Result<Self> {
        Ok(Self {
            db: services.get::<Db>("base/database.pool")?.as_ref().clone(),
            mutations: services.get("base/database.mutations")?,
            settings: services.get("base/settings.store")?,
            ports,
            artifacts: services.get("base/artifacts.store")?,
            adapters: Adapters::from_catalog(catalog, modules)?,
            registry,
        })
    }
    /// The providers an operator may choose ([`Providers::installed`]).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
    /// The read adapters the mounted modules contribute.
    pub fn adapters(&self) -> &Adapters {
        &self.adapters
    }
    /// Whether a mounted module answers the People directory port, for a page
    /// that explains People's absence rather than offering controls that
    /// would only be refused.
    pub fn people_available(&self) -> bool {
        self.ports.available(DIRECTORY_PORT)
    }

    /// The company's connection state (Bilimbi `status/2`). Needs only the
    /// scope. Stale or unavailable workforce identity, and a stored mapping
    /// that no longer matches Workforce, are refused.
    pub async fn status(&self, scope: &Scope, platform_company_id: i64) -> Result<Status> {
        let client = self.db.get().await.map_err(Error::from)?;
        self.status_on(&client, scope, platform_company_id).await
    }

    /// Chooses the company's provider and records the current platform and
    /// workforce company mapping (Bilimbi `configure_connection/4`). A new or
    /// changed provider starts disabled; changing provider discards the
    /// previous provider's credential; a changed provider or mapping also
    /// disables the connection.
    pub async fn configure_connection(
        &self,
        scope: &Scope,
        platform_company_id: i64,
        provider_id: &str,
    ) -> Result<Status> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company_id = authorize(&**client, scope, platform_company_id).await?;
        let provider = self.registry.fetch(provider_id)?.clone();
        let mut tx = self
            .begin(&mut client, scope, "people-connector.configure_connection")
            .await?;
        require_writable(&tx, scope, company_id).await?;
        let current =
            Status::from_workforce(people::company(&self.ports, &tx, scope, company_id).await?)?;
        let tenant = scope.tenant();
        let mapping = Mapping {
            provider_id: provider.id.clone(),
            provider_contract_version: provider.contract_version.clone(),
            workforce_source_id: current.workforce_source_id.clone(),
            workforce_company_id: current.workforce_company_id,
        };
        if connections::workforce_company_taken(
            tx.sql(),
            tenant,
            company_id,
            &mapping.workforce_source_id,
            mapping.workforce_company_id,
        )
        .await?
        {
            return Err(ConnectorError::WorkforceCompanyTaken);
        }
        match connections::find(tx.sql(), tenant, company_id, true).await? {
            None => {
                connections::insert(&mut tx, tenant, company_id, mapping).await?;
            }
            Some(connection) => {
                let provider_changed = connection.provider_id != provider.id;
                let remapped = connection.workforce_source_id != mapping.workforce_source_id
                    || connection.workforce_company_id != mapping.workforce_company_id;
                if provider_changed {
                    self.delete_credential(&mut tx, scope, &connection).await?;
                }
                if provider_changed || remapped {
                    sync_store::reset(&mut tx, &connection).await?;
                }
                let enabled = connection.enabled && !provider_changed && !remapped;
                connections::update(&mut tx, connection, mapping, enabled).await?;
            }
        }
        tx.commit().await?;
        self.status(scope, company_id).await
    }

    /// Stores a secret for a connection whose provider declares one (Bilimbi
    /// `put_credential/4`): non-empty, at most [`CREDENTIAL_MAX_BYTES`],
    /// never shown back.
    pub async fn put_credential(
        &self,
        scope: &Scope,
        platform_company_id: i64,
        secret: &str,
    ) -> Result<()> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company_id = authorize(&**client, scope, platform_company_id).await?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.put_credential")
            .await?;
        require_writable(&tx, scope, company_id).await?;
        let (connection, provider) = self
            .connection_provider(tx.sql(), scope, company_id)
            .await?;
        if provider.credential != Credential::Secret {
            return Err(ConnectorError::CredentialNotRequired);
        }
        if !secret_valid(secret) {
            return Err(ConnectorError::InvalidCredential);
        }
        self.settings
            .put(
                &mut tx,
                scope,
                Target::Company(OrganisationId::new(connection.platform_company_id)?),
                CREDENTIAL_KEY,
                Value::String(secret.to_owned()),
            )
            .await
            .map_err(|error| match error {
                Error::Invalid => ConnectorError::InvalidCredential,
                other => other.into(),
            })?;
        tx.commit().await?;
        Ok(())
    }

    /// Clears the stored secret and disables a connection that needs one
    /// (Bilimbi `clear_credential/3`).
    pub async fn clear_credential(&self, scope: &Scope, platform_company_id: i64) -> Result<()> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company_id = authorize(&**client, scope, platform_company_id).await?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.clear_credential")
            .await?;
        require_writable(&tx, scope, company_id).await?;
        let (connection, provider) = self
            .connection_provider(tx.sql(), scope, company_id)
            .await?;
        if provider.credential != Credential::Secret {
            return Err(ConnectorError::CredentialNotRequired);
        }
        self.delete_credential(&mut tx, scope, &connection).await?;
        if connection.enabled {
            connections::set_enabled(&mut tx, connection, false).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Enables or disables a connection (Bilimbi `set_enabled/4`). Enabling
    /// needs a current company mapping, an installed provider and, when the
    /// provider declares one, a stored credential.
    pub async fn set_enabled(
        &self,
        scope: &Scope,
        platform_company_id: i64,
        enabled: bool,
    ) -> Result<Status> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company_id = authorize(&**client, scope, platform_company_id).await?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.set_enabled")
            .await?;
        require_writable(&tx, scope, company_id).await?;
        let current =
            Status::from_workforce(people::company(&self.ports, &tx, scope, company_id).await?)?;
        let (connection, provider) = self
            .connection_provider(tx.sql(), scope, company_id)
            .await?;
        require_mapping(&connection, &current)?;
        if enabled
            && provider.credential == Credential::Secret
            && !self.credential_stored(tx.sql(), scope, company_id).await?
        {
            return Err(ConnectorError::CredentialMissing);
        }
        if connection.enabled != enabled {
            connections::set_enabled(&mut tx, connection, enabled).await?;
        }
        tx.commit().await?;
        self.status(scope, company_id).await
    }

    /// Removes the company's connection and its stored credential (Bilimbi
    /// `remove_connection/2`); a company without one is `Disconnected`.
    pub async fn remove_connection(&self, scope: &Scope, platform_company_id: i64) -> Result<()> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company_id = authorize(&**client, scope, platform_company_id).await?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.remove_connection")
            .await?;
        require_writable(&tx, scope, company_id).await?;
        let connection = connections::find(tx.sql(), scope.tenant(), company_id, true)
            .await?
            .ok_or(ConnectorError::Disconnected)?;
        self.delete_credential(&mut tx, scope, &connection).await?;
        sync_store::remove(&mut tx, &connection).await?;
        file_store::detach(&mut tx, scope.tenant(), connection.id).await?;
        backup_store::detach(&mut tx, scope.tenant(), connection.id).await?;
        connections::delete(&mut tx, connection).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn status_on(
        &self,
        client: &tokio_postgres::Client,
        scope: &Scope,
        platform_company_id: i64,
    ) -> Result<Status> {
        let current = Status::from_workforce(
            people::company(&self.ports, client, scope, platform_company_id).await?,
        )?;
        let Some(connection) =
            connections::find(client, scope.tenant(), current.platform_company_id, false).await?
        else {
            return Ok(current);
        };
        require_mapping(&connection, &current)?;
        let credential_stored = self
            .credential_stored(client, scope, connection.platform_company_id)
            .await?;
        Ok(Status {
            state: if connection.enabled {
                ConnectionState::Enabled
            } else {
                ConnectionState::Disabled
            },
            provider_id: Some(connection.provider_id),
            credential_stored,
            ..current
        })
    }

    /// Reports only the presence of the encrypted company override.
    async fn credential_stored(
        &self,
        client: &(impl GenericClient + Sync),
        scope: &Scope,
        company_id: i64,
    ) -> Result<bool> {
        Ok(self
            .settings
            .resolve_masked(
                client,
                scope,
                Target::Company(OrganisationId::new(company_id)?),
                &[CREDENTIAL_KEY],
            )
            .await?
            .get(CREDENTIAL_KEY)
            .is_some_and(|resolved| resolved.overridden))
    }

    /// The company's locked connection and its installed provider, read only
    /// after the company lock so concurrent provider changes cannot race it.
    async fn connection_provider(
        &self,
        client: &(impl GenericClient + Sync),
        scope: &Scope,
        company_id: i64,
    ) -> Result<(Connection, &Provider)> {
        let connection = connections::find(client, scope.tenant(), company_id, true)
            .await?
            .ok_or(ConnectorError::Disconnected)?;
        let provider = self.registry.fetch(&connection.provider_id)?;
        Ok((connection, provider))
    }

    async fn begin<'a>(
        &self,
        client: &'a mut tokio_postgres::Client,
        scope: &Scope,
        action: &str,
    ) -> Result<MutationTransaction<'a>> {
        Ok(self
            .mutations
            .begin(client, scope.mutation_context(action))
            .await?)
    }

    /// Deletes the company's credential override; deleting ciphertext needs
    /// no encryption key.
    async fn delete_credential(
        &self,
        tx: &mut MutationTransaction<'_>,
        scope: &Scope,
        connection: &Connection,
    ) -> Result<()> {
        self.settings
            .delete(
                tx,
                scope,
                Target::Company(OrganisationId::new(connection.platform_company_id)?),
                CREDENTIAL_KEY,
            )
            .await?;
        Ok(())
    }
}

/// The scope's signed-in actor holds `people-connector.connections.manage`
/// with Core Company reach to the company (Bilimbi `authorize/2`).
async fn authorize(
    client: &(impl GenericClient + Sync),
    scope: &Scope,
    platform_company_id: i64,
) -> Result<i64> {
    organisation::authorize_target(client, scope, platform_company_id, MANAGE)
        .await
        .map_err(ConnectorError::from)
}

/// The company's writable lock, held for the write: an archived company is
/// read-only for good.
async fn require_writable(
    tx: &MutationTransaction<'_>,
    scope: &Scope,
    company_id: i64,
) -> Result<()> {
    organisation::require_writable(tx.sql(), scope, company_id, true)
        .await
        .map_err(|error| match error {
            WriteError::NotFound => ConnectorError::NotFound,
            WriteError::Archived => ConnectorError::CompanyArchived,
            WriteError::Access(error) => error.into(),
            _ => ConnectorError::Unauthorized,
        })?;
    // A grant may have been revoked while this write waited for the company
    // lock. Re-prove it on the transaction that will perform the mutation.
    authorize(tx.sql(), scope, company_id).await?;
    Ok(())
}

fn require_mapping(connection: &Connection, current: &Status) -> Result<()> {
    if connection.platform_company_id != current.platform_company_id
        || connection.workforce_source_id != current.workforce_source_id
        || connection.workforce_company_id != current.workforce_company_id
    {
        return Err(ConnectorError::MappingChanged);
    }
    Ok(())
}

/// A credential is non-blank text of at most [`CREDENTIAL_MAX_BYTES`].
pub(crate) fn secret_valid(secret: &str) -> bool {
    !secret.trim().is_empty() && secret.len() <= CREDENTIAL_MAX_BYTES
}
