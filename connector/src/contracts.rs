//! The provider-neutral contracts (Bilimbi `Connector.Capability`,
//! `Connector.Provider`, `Connector.Registry` and `Connector.Providers`): what
//! a provider declares, the immutable registry of declarations, and the
//! providers an operator may choose in this installation.
//!
//! Capability keys are protocol vocabulary, not operator settings, and a
//! declaration names its direction so a read can never imply a write.
//! Registration alone never activates a connection or exposes an adapter.
use serde::Serialize;
use std::collections::BTreeMap;

/// The capability keys a provider may declare (Bilimbi `Capability.keys/0`).
pub const CAPABILITY_KEYS: [&str; 12] = [
    "company_directory",
    "employee_directory",
    "organization_directory",
    "manager_hierarchy",
    "user_directory",
    "payroll",
    "attendance",
    "leave",
    "claims",
    "training",
    "documents",
    "single_sign_on",
];

/// The capability every pass synchronises under.
pub const COMPANY_DIRECTORY: &str = "company_directory";
pub const EMPLOYEE_DIRECTORY: &str = "employee_directory";
pub const ORGANIZATION_DIRECTORY: &str = "organization_directory";

/// The co-located native People provider.
pub const NATIVE_PROVIDER_ID: &str = "people.native";

/// Why a declaration or a lookup was refused (Bilimbi's atoms).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ContractError {
    #[error("invalid capability")]
    InvalidCapability,
    #[error("invalid provider")]
    InvalidProvider,
    #[error("duplicate provider")]
    DuplicateProvider,
    /// No installed provider with that id, or one that does not declare the
    /// capability in that direction.
    #[error("That provider is not installed. Choose another.")]
    Unsupported,
}

/// Which way a declared capability moves data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Read,
    Write,
}

/// One provider declaration: a known key and an explicit direction. A `Read`
/// declaration maps to the neutral [`crate::ReadPort`] and a `Write`
/// declaration to [`crate::WritePort`], never to an adapter implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Capability {
    pub key: String,
    pub direction: Direction,
}
impl Capability {
    /// A declaration for one of [`CAPABILITY_KEYS`]; any other key is
    /// `InvalidCapability`.
    pub fn new(key: &str, direction: Direction) -> Result<Self, ContractError> {
        if !CAPABILITY_KEYS.contains(&key) {
            return Err(ContractError::InvalidCapability);
        }
        Ok(Self {
            key: key.to_owned(),
            direction,
        })
    }
}

/// Whether a connection to a provider needs an operator-supplied secret. The
/// secret itself is never part of the descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Credential {
    None,
    Secret,
}

/// A provider descriptor with an immutable declaration set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Provider {
    pub id: String,
    pub name: String,
    pub contract_version: String,
    pub capabilities: Vec<Capability>,
    pub credential: Credential,
}
impl Provider {
    /// A descriptor whose id is lowercase segments joined by `.` or `-` (at
    /// most 100 bytes), whose name is not blank, whose contract version is
    /// `1.x.y` (at most 20 bytes) and whose declarations are known and
    /// distinct; anything else is `InvalidProvider`.
    pub fn new(
        id: &str,
        name: &str,
        contract_version: &str,
        capabilities: Vec<Capability>,
        credential: Credential,
    ) -> Result<Self, ContractError> {
        if !id_valid(id)
            || name.trim().is_empty()
            || !version_valid(contract_version)
            || contract_version.len() > 20
        {
            return Err(ContractError::InvalidProvider);
        }
        for (index, capability) in capabilities.iter().enumerate() {
            if !CAPABILITY_KEYS.contains(&capability.key.as_str())
                || capabilities[..index].contains(capability)
            {
                return Err(ContractError::InvalidProvider);
            }
        }
        Ok(Self {
            id: id.to_owned(),
            name: name.to_owned(),
            contract_version: contract_version.to_owned(),
            capabilities,
            credential,
        })
    }
    /// Whether the provider declares `key` in `direction`.
    pub fn declares(&self, key: &str, direction: Direction) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.key == key && capability.direction == direction)
    }
}

/// `^[a-z0-9]+(?:[.-][a-z0-9]+)*$`, at most 100 bytes.
fn id_valid(id: &str) -> bool {
    if id.is_empty() || id.len() > 100 {
        return false;
    }
    id.split(['.', '-']).all(|segment| {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    })
}

/// `^1\.\d+\.\d+$`.
fn version_valid(version: &str) -> bool {
    let mut parts = version.split('.');
    let major = parts.next() == Some("1");
    let minor = parts
        .next()
        .is_some_and(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()));
    let patch = parts
        .next()
        .is_some_and(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()));
    major && minor && patch && parts.next().is_none()
}

/// Immutable registry of provider capability declarations, keyed by
/// provider id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Registry {
    providers: BTreeMap<String, Provider>,
}
impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
    /// The registry with `provider` added; a provider already registered
    /// under that id is `DuplicateProvider`.
    pub fn register(mut self, provider: Provider) -> Result<Self, ContractError> {
        // Re-validated, so a descriptor built any other way cannot slip in.
        let provider = Provider::new(
            &provider.id,
            &provider.name,
            &provider.contract_version,
            provider.capabilities,
            provider.credential,
        )?;
        if self.providers.contains_key(&provider.id) {
            return Err(ContractError::DuplicateProvider);
        }
        self.providers.insert(provider.id.clone(), provider);
        Ok(self)
    }
    /// The provider registered under `provider_id`, else `Unsupported`.
    pub fn fetch(&self, provider_id: &str) -> Result<&Provider, ContractError> {
        self.providers
            .get(provider_id)
            .ok_or(ContractError::Unsupported)
    }
    /// Every registered provider, by name then id.
    pub fn providers(&self) -> Vec<&Provider> {
        let mut providers: Vec<_> = self.providers.values().collect();
        providers.sort_by(|a, b| (&a.name, &a.id).cmp(&(&b.name, &b.id)));
        providers
    }
    /// `Ok` only when `provider_id` is registered and declares `capability`
    /// in `direction`.
    pub fn permit(
        &self,
        provider_id: &str,
        capability: &str,
        direction: Direction,
    ) -> Result<(), ContractError> {
        match self.providers.get(provider_id) {
            Some(provider) if provider.declares(capability, direction) => Ok(()),
            _ => Err(ContractError::Unsupported),
        }
    }
}

/// The providers an operator may choose in this installation (Bilimbi
/// `Connector.Providers`): only the co-located native People provider, which
/// reads through the mounted People Workforce directory port, so it needs no
/// credential and no transport. Remote and third-party providers are not
/// offered until their transport and authority are separately evidenced.
pub struct Providers;
impl Providers {
    /// [`NATIVE_PROVIDER_ID`].
    pub fn native_id() -> &'static str {
        NATIVE_PROVIDER_ID
    }
    /// The installed registry: `people.native` ("People (this installation)",
    /// contract `1.0.0`) declaring company, employee and organisation directory reads.
    pub fn installed() -> Registry {
        let capabilities = [
            COMPANY_DIRECTORY,
            EMPLOYEE_DIRECTORY,
            ORGANIZATION_DIRECTORY,
        ]
        .into_iter()
        .map(|key| Capability::new(key, Direction::Read))
        .collect::<Result<Vec<_>, _>>()
        .expect("known capability keys");
        let native = Provider::new(
            NATIVE_PROVIDER_ID,
            "People (this installation)",
            "1.0.0",
            capabilities,
            Credential::None,
        )
        .expect("a valid native provider descriptor");
        Registry::new()
            .register(native)
            .expect("one provider in an empty registry")
    }
}
