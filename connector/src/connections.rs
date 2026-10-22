//! The `people_connector_connections` table: its rows, the SQL that reads
//! them and the writes behind the facade (Bilimbi `Connector.Connection` and
//! the private helpers of `Connector`). Private to the facade: every field is
//! assigned from validated values, nothing is cast from operator input, and
//! every read binds the caller's tenant beside the company.
use crate::Result;
use bilibi_database::{Change, Error, MutationTransaction};
use serde_json::{json, Value};
use tokio_postgres::{GenericClient, Row};

const COLUMNS: &str =
    "id, tenant_id, platform_company_id, provider_id, provider_contract_version, \
    workforce_source_id, workforce_company_id, enabled";
/// The audit kind every connection change is recorded under.
const KIND: &str = "people_connector_connection";

/// One stored connection with both company axes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Connection {
    pub id: i64,
    pub tenant_id: i64,
    pub platform_company_id: i64,
    pub provider_id: String,
    pub provider_contract_version: String,
    pub workforce_source_id: String,
    pub workforce_company_id: i64,
    pub enabled: bool,
}
impl Connection {
    fn read(row: &Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            platform_company_id: row.get("platform_company_id"),
            provider_id: row.get("provider_id"),
            provider_contract_version: row.get("provider_contract_version"),
            workforce_source_id: row.get("workforce_source_id"),
            workforce_company_id: row.get("workforce_company_id"),
            enabled: row.get("enabled"),
        }
    }
    /// The row as audit records it.
    fn audit_values(&self) -> Value {
        json!({
            "tenant_id": self.tenant_id,
            "platform_company_id": self.platform_company_id,
            "provider_id": self.provider_id,
            "provider_contract_version": self.provider_contract_version,
            "workforce_source_id": self.workforce_source_id,
            "workforce_company_id": self.workforce_company_id,
            "enabled": self.enabled,
        })
    }
}

/// The provider and workforce mapping a connection records.
#[derive(Clone, Debug)]
pub(crate) struct Mapping {
    pub provider_id: String,
    pub provider_contract_version: String,
    pub workforce_source_id: String,
    pub workforce_company_id: i64,
}

/// The tenant's connection for a platform company, locked `FOR UPDATE` when
/// the caller is about to change it.
pub(crate) async fn find(
    client: &impl GenericClient,
    tenant: i64,
    platform_company_id: i64,
    lock: bool,
) -> Result<Option<Connection>> {
    let sql = format!(
        "SELECT {COLUMNS} FROM people_connector_connections \
         WHERE tenant_id = $1 AND platform_company_id = $2{}",
        if lock { " FOR UPDATE" } else { "" }
    );
    Ok(client
        .query_opt(&sql, &[&tenant, &platform_company_id])
        .await
        .map_err(Error::from)?
        .as_ref()
        .map(Connection::read))
}

/// Whether another platform company of the tenant already holds this
/// workforce company identity (the refusal decided before an insert or
/// remap, so the unique constraint only catches a race).
pub(crate) async fn workforce_company_taken(
    client: &impl GenericClient,
    tenant: i64,
    platform_company_id: i64,
    workforce_source_id: &str,
    workforce_company_id: i64,
) -> Result<bool> {
    Ok(client
        .query_opt(
            "SELECT 1 FROM people_connector_connections \
             WHERE tenant_id = $1 AND workforce_source_id = $2 AND workforce_company_id = $3 \
               AND platform_company_id <> $4",
            &[
                &tenant,
                &workforce_source_id,
                &workforce_company_id,
                &platform_company_id,
            ],
        )
        .await
        .map_err(Error::from)?
        .is_some())
}

/// Inserts a disabled connection, recorded as a created change.
pub(crate) async fn insert(
    tx: &mut MutationTransaction<'_>,
    tenant: i64,
    platform_company_id: i64,
    mapping: Mapping,
) -> Result<Connection> {
    let connection = tx
        .mutate(move |sql| {
            Box::pin(async move {
                let row = sql
                    .query_one(
                        &format!(
                            "INSERT INTO people_connector_connections \
                             (tenant_id, platform_company_id, provider_id, provider_contract_version, \
                              workforce_source_id, workforce_company_id, enabled) \
                             VALUES ($1, $2, $3, $4, $5, $6, false) RETURNING {COLUMNS}"
                        ),
                        &[
                            &tenant,
                            &platform_company_id,
                            &mapping.provider_id,
                            &mapping.provider_contract_version,
                            &mapping.workforce_source_id,
                            &mapping.workforce_company_id,
                        ],
                    )
                    .await?;
                let connection = Connection::read(&row);
                // An empty object on the absent side is what names the audit
                // event `created`.
                let change = Change {
                    kind: KIND.into(),
                    id: connection.id,
                    old: json!({}),
                    new: connection.audit_values(),
                };
                Ok((connection, vec![change]))
            })
        })
        .await?;
    Ok(connection)
}

/// Rewrites a connection's mapping and enabled flag, recorded as an updated
/// change against the row the caller locked.
pub(crate) async fn update(
    tx: &mut MutationTransaction<'_>,
    before: Connection,
    mapping: Mapping,
    enabled: bool,
) -> Result<Connection> {
    let connection = tx
        .mutate(move |sql| {
            Box::pin(async move {
                let row = sql
                    .query_one(
                        &format!(
                            "UPDATE people_connector_connections \
                             SET provider_id = $3, provider_contract_version = $4, workforce_source_id = $5, \
                                 workforce_company_id = $6, enabled = $7, updated_at = now() AT TIME ZONE 'UTC' \
                             WHERE id = $1 AND tenant_id = $2 RETURNING {COLUMNS}"
                        ),
                        &[
                            &before.id,
                            &before.tenant_id,
                            &mapping.provider_id,
                            &mapping.provider_contract_version,
                            &mapping.workforce_source_id,
                            &mapping.workforce_company_id,
                            &enabled,
                        ],
                    )
                    .await?;
                let after = Connection::read(&row);
                let change = Change {
                    kind: KIND.into(),
                    id: after.id,
                    old: before.audit_values(),
                    new: after.audit_values(),
                };
                Ok((after, vec![change]))
            })
        })
        .await?;
    Ok(connection)
}

/// Enables or disables a connection the caller locked.
pub(crate) async fn set_enabled(
    tx: &mut MutationTransaction<'_>,
    before: Connection,
    enabled: bool,
) -> Result<Connection> {
    let mapping = Mapping {
        provider_id: before.provider_id.clone(),
        provider_contract_version: before.provider_contract_version.clone(),
        workforce_source_id: before.workforce_source_id.clone(),
        workforce_company_id: before.workforce_company_id,
    };
    update(tx, before, mapping, enabled).await
}

/// Deletes a connection the caller locked, recorded as a deleted change.
pub(crate) async fn delete(tx: &mut MutationTransaction<'_>, before: Connection) -> Result<()> {
    tx.mutate(move |sql| {
        Box::pin(async move {
            let deleted = sql
                .execute(
                    "DELETE FROM people_connector_connections WHERE id = $1 AND tenant_id = $2",
                    &[&before.id, &before.tenant_id],
                )
                .await?;
            if deleted != 1 {
                return Err(Error::Conflict);
            }
            let change = Change {
                kind: KIND.into(),
                id: before.id,
                old: before.audit_values(),
                new: json!({}),
            };
            Ok(((), vec![change]))
        })
    })
    .await?;
    Ok(())
}
