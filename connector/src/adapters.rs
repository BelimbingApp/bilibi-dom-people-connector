//! The read-port seam between the Connector and the adapter modules that
//! serve installed providers (Bilimbi `Connector.ReadPort`, `WritePort`,
//! `PortAuthorization`, `PortRequest`, `Page` and `Adapters`).
//!
//! An adapter module depends on the Connector, so the Connector cannot name
//! it. In Bilimbi a mounted adapter registers itself at application start
//! (`Adapters.register/2`). Bilibi has no application start and keeps no
//! runtime registry: an adapter module submits one [`Adapter`] under the
//! [`ADAPTERS`] catalog with `inventory`, and [`Adapters`] is built once from
//! the catalog when the Connector service is installed. Unmounting the adapter
//! module leaves the Connector refusing passes with `adapter_unavailable`, as
//! Bilimbi's withdrawn registration does. A contribution never activates a
//! connection or adds a capability: the provider's declaration and the
//! enabled connection still gate every port call.
use crate::contracts::Direction;
use crate::MODULE;
use bilibi_module_registry::{Catalog, Consumer, Entry, Module, RegistryError, Result};
use bilibi_ports::{PortClient, Ports};
use bilibi_tenancy::Scope;
use jiff::Timestamp;
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, pin::Pin};

/// The catalog adapter modules contribute to:
/// `Contribution::new(ADAPTERS, "<provider id>", provenance, &Adapter { provider_id, port })`.
pub const ADAPTERS: &str = "people_connector/connector.adapters";

/// What the Connector hands an adapter after checking the actor, the company
/// mapping, the provider's declaration and the enabled connection. Both
/// company axes are explicit: `platform_company_id` is the Core Company in
/// `scope`'s tenant and `workforce_company_id` the workforce company the
/// connection records. Adapters never build one; only the Connector does.
#[derive(Clone, Copy, Debug)]
pub struct PortAuthorization<'a> {
    pub scope: &'a Scope,
    pub platform_company_id: i64,
    pub workforce_source_id: &'a str,
    pub workforce_company_id: i64,
    pub provider_id: &'a str,
    pub capability: &'a str,
    pub direction: Direction,
}

/// Which pass a page request belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pass {
    /// The provider's whole directory; `since` is `None`.
    Bootstrap,
    /// Changes after the resume cursor the last completed pass returned.
    Changes,
}

/// One page request on a read port. `cursor` is the page cursor within this
/// pass (`None` for the first page) and `limit` the most entries the page may
/// carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortRequest {
    pub pass: Pass,
    pub since: Option<String>,
    pub cursor: Option<String>,
    pub limit: usize,
}

/// How far the Connector may trust a page or a read (the People Workforce
/// `ReadResult` vocabulary, kept here because a Domain never depends on
/// another Domain's crate).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Freshness {
    /// Read from the source just now.
    Current,
    /// A cached copy, last confirmed at this instant.
    Stale(Timestamp),
    /// No value could be read; the reason is for a notice, never for logic.
    Unavailable(String),
}

/// One page an adapter returns for a [`PortRequest`]. `entries` holds the
/// provider's directory records as the sync engine reads them (port plan row
/// KC1 types them); `next_cursor` is `None` on the last page of a pass;
/// `resume_cursor` is what the next `Changes` pass presents as `since`;
/// `as_of` is the provider watermark for the page; a stale or unavailable
/// page stops the pass before anything is applied; `snapshot` is true when
/// the page belongs to a pass that lists every current record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    pub entries: Vec<Value>,
    pub next_cursor: Option<String>,
    pub resume_cursor: Option<String>,
    pub as_of: Timestamp,
    pub freshness: Freshness,
    pub snapshot: bool,
}

/// Why an adapter did not answer a page: a fixed code the Connector records,
/// never adapter or People text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReadRefusal {
    /// Another provider, an undeclared capability or a direction other than
    /// read.
    #[error("invalid_authorization")]
    InvalidAuthorization,
    #[error("invalid_request")]
    InvalidRequest,
    #[error("invalid_cursor")]
    InvalidCursor,
    /// A company the scope's tenant cannot see, or one that is not live.
    #[error("not_found")]
    NotFound,
    /// The source's company axes do not match the authorization.
    #[error("mapping_mismatch")]
    MappingMismatch,
    #[error("unexpected_result")]
    UnexpectedResult,
}
impl ReadRefusal {
    /// The code as the Connector records it.
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidAuthorization => "invalid_authorization",
            Self::InvalidRequest => "invalid_request",
            Self::InvalidCursor => "invalid_cursor",
            Self::NotFound => "not_found",
            Self::MappingMismatch => "mapping_mismatch",
            Self::UnexpectedResult => "unexpected_result",
        }
    }
}

pub type ReadFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<Page, ReadRefusal>> + Send + 'a>>;

/// Services borrowed from the Connector for one adapter call. The native
/// adapter calls the People directory through `ports` on `client`, preserving
/// the caller's transaction without keeping a global service or database pool.
#[derive(Clone, Copy)]
pub struct PortContext<'a> {
    pub client: PortClient<'a>,
    pub ports: &'a Ports,
}

/// The neutral read port for directory synchronisation. The Connector calls it
/// only with a [`PortAuthorization`] it issued; an adapter answers one
/// [`Page`] per [`PortRequest`] or a [`ReadRefusal`], using the caller's
/// [`PortContext`] to reach People.
pub trait ReadPort: Send + Sync {
    fn read<'a>(
        &'a self,
        context: PortContext<'a>,
        authorization: &'a PortAuthorization<'a>,
        request: &'a PortRequest,
    ) -> ReadFuture<'a>;
}

pub type WriteFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<Value, ReadRefusal>> + Send + 'a>>;

/// The provider-neutral write port. No writer is activated.
pub trait WritePort: Send + Sync {
    fn write<'a>(
        &'a self,
        context: PortContext<'a>,
        authorization: &'a PortAuthorization<'a>,
        request: &'a Value,
    ) -> WriteFuture<'a>;
}

/// Contribution payload: the read adapter that serves `provider_id`.
pub struct Adapter {
    pub provider_id: &'static str,
    pub port: &'static dyn ReadPort,
}

/// Each contribution is keyed by the provider it serves, so the catalog itself
/// refuses two adapters for one provider.
fn validate(entries: &[Entry]) -> Result<()> {
    for entry in entries {
        let adapter = entry.get::<Adapter>()?;
        if adapter.provider_id.is_empty() || adapter.provider_id != entry.id {
            return Err(RegistryError::Contribution(format!(
                "adapter {} at {}:{} must be contributed under its provider id",
                entry.id, entry.provenance.source, entry.provenance.line
            )));
        }
    }
    Ok(())
}
inventory::submit! { Consumer::new::<Adapter>(ADAPTERS, MODULE, validate) }

/// The read adapters the mounted modules contribute, keyed by provider id.
#[derive(Default)]
pub struct Adapters {
    installed: BTreeMap<&'static str, &'static dyn ReadPort>,
}
impl Adapters {
    /// Built once from the catalog. A contributing module must depend on the
    /// Connector, so the graph shows which modules serve providers.
    pub fn from_catalog(catalog: &Catalog, modules: &[&Module]) -> Result<Self> {
        let mut installed = BTreeMap::new();
        for entry in catalog.entries::<Adapter>(ADAPTERS)? {
            let depends = modules.iter().any(|module| {
                module.id == entry.provenance.module
                    && (module.id == MODULE || module.dependencies.contains(&MODULE))
            });
            if !depends {
                return Err(RegistryError::Contribution(format!(
                    "adapter {} at {}:{} is contributed by a module that does not depend on {MODULE}",
                    entry.id, entry.provenance.source, entry.provenance.line
                )));
            }
            let adapter = entry.get::<Adapter>()?;
            installed.insert(adapter.provider_id, adapter.port);
        }
        Ok(Self { installed })
    }
    /// The adapter serving `provider_id`, if a mounted module contributes one.
    pub fn fetch(&self, provider_id: &str) -> Option<&'static dyn ReadPort> {
        self.installed.get(provider_id).copied()
    }
    /// Whether any mounted module serves `provider_id`.
    pub fn serves(&self, provider_id: &str) -> bool {
        self.installed.contains_key(provider_id)
    }
    /// Every served provider id, sorted.
    pub fn provider_ids(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.installed.keys().copied()
    }
}
