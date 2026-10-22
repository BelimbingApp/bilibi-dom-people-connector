//! How the Connector reaches People: the `people/workforce.directory` port
//! (Bilimbi `Workforce.company/2`, which the Connector called on a declared
//! edge). The Platform forbids a Cargo dependency, descriptor edge or SQL
//! reference into another container, so the Connector asks People Workforce
//! by port id with its own serde types for the documented shape, and composes
//! without People: while no mounted module answers the port every call that
//! needs workforce identity is `people_unavailable`.
//!
//! The shape below is the Connector's reading of the port; `README.md`
//! ("The People directory port") is where it is documented for the provider.
use crate::adapters::Freshness;
use crate::{ConnectorError, Result};
use bilibi_database::Error;
use bilibi_ports::{PortClient, PortError, Ports};
use bilibi_tenancy::Scope;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

/// The port People Workforce provides for the connector.
pub const DIRECTORY_PORT: &str = "people/workforce.directory";
/// The source identity every native People reference carries.
pub const NATIVE_SOURCE_ID: &str = "people/native";

/// The request for the company's native workforce identity.
#[derive(Serialize)]
struct CompanyRequest {
    read: &'static str,
    company_id: i64,
}

/// A stable reference into the native workforce, as Workforce serializes it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Reference {
    pub source_id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub stable_id: String,
}

/// The native workforce company read model (Workforce's `Company`): both
/// company axes, name and code.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkforceCompany {
    pub reference: Reference,
    pub platform_company_id: i64,
    pub workforce_company_id: i64,
    pub name: String,
    pub code: String,
}

/// Freshness as it crosses the port: `"current"`, `{"stale": "<RFC 3339>"}`
/// or `{"unavailable": "<reason>"}`.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FreshnessShape {
    Current,
    Stale(String),
    Unavailable(String),
}

/// The port's answer: the value, absent only when unavailable, with its
/// freshness.
#[derive(Deserialize)]
struct Answer<T> {
    value: Option<T>,
    freshness: FreshnessShape,
}

/// A workforce read with the freshness a caller may rely on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read<T> {
    pub value: Option<T>,
    pub freshness: Freshness,
}
impl<T> Read<T> {
    /// The value only when it is current (Bilimbi `ReadResult.require_current`);
    /// a stale or unavailable read is the `NotCurrent` refusal.
    pub fn require_current(self) -> Result<T> {
        match (self.freshness, self.value) {
            (Freshness::Current, Some(value)) => Ok(value),
            (Freshness::Current, None) => Err(ConnectorError::NotCurrent(Freshness::Unavailable(
                "empty".into(),
            ))),
            (freshness, _) => Err(ConnectorError::NotCurrent(freshness)),
        }
    }
}

/// The company's native workforce identity through the directory port, on the
/// caller's client and scope. No mounted provider is `PeopleUnavailable`; the
/// provider's own refusals are `NotFound` (a company the scope cannot see) and
/// `Unauthorized`; a shape the two sides disagree on is a failure.
pub(crate) async fn company<'a>(
    ports: &Ports,
    client: impl Into<PortClient<'a>>,
    scope: &'a Scope,
    company_id: i64,
) -> Result<Read<WorkforceCompany>> {
    let request = CompanyRequest {
        read: "company",
        company_id,
    };
    let answer: Answer<WorkforceCompany> = ports
        .call(client, scope, DIRECTORY_PORT, &request)
        .await
        .map_err(|error| match error {
            PortError::Unavailable(_) => ConnectorError::PeopleUnavailable,
            PortError::Refused(Error::NotFound) => ConnectorError::NotFound,
            PortError::Refused(Error::Forbidden | Error::Unauthorized) => {
                ConnectorError::Unauthorized
            }
            PortError::Refused(error) => ConnectorError::Failed(error),
            PortError::Shape { .. } => ConnectorError::Failed(Error::Internal),
        })?;
    let freshness = match answer.freshness {
        FreshnessShape::Current => Freshness::Current,
        FreshnessShape::Stale(at) => Freshness::Stale(
            at.parse::<Timestamp>()
                .map_err(|_| ConnectorError::Failed(Error::Internal))?,
        ),
        FreshnessShape::Unavailable(reason) => Freshness::Unavailable(reason),
    };
    Ok(Read {
        value: answer.value,
        freshness,
    })
}
