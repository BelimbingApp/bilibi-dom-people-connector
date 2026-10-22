//! Private persistence for Bilimbi Connector.Sync. All mutations return every affected row.
use crate::{connections::Connection, ConnectorError, Result};
use bilibi_database::{Change, Error, MutationTransaction};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tokio_postgres::GenericClient;

#[derive(Clone, Copy)]
pub(crate) enum Table {
    Checkpoint,
    Run,
    Record,
    Issue,
}
impl Table {
    fn name(self) -> &'static str {
        match self {
            Self::Checkpoint => "people_connector_sync_checkpoints",
            Self::Run => "people_connector_sync_runs",
            Self::Record => "people_connector_workforce_records",
            Self::Issue => "people_connector_reconciliation_issues",
        }
    }
    fn kind(self) -> &'static str {
        match self {
            Self::Checkpoint => "people_connector_sync_checkpoint",
            Self::Run => "people_connector_sync_run",
            Self::Record => "people_connector_workforce_record",
            Self::Issue => "people_connector_reconciliation_issue",
        }
    }
    fn columns(self) -> &'static str {
        match self {
        Self::Checkpoint => "tenant_id,connection_id,version,resume_cursor,as_of_at,bootstrap_required",
        Self::Run => "tenant_id,connection_id,platform_company_id,provider_id,idempotency_key,pass,state,reason,checkpoint_version,as_of_at,applied,unchanged,superseded,deactivated,refused,started_at,finished_at",
        Self::Record => "tenant_id,connection_id,kind,source_id,stable_id,workforce_company_id,active,name,code,email,supervisor_stable_id,parent_stable_id,version,vacant,assignments_incomplete,assignments,content_hash,observed_at,deactivated_at",
        Self::Issue => "tenant_id,connection_id,issue_key,kind,reason,severity,record_kind,stable_id,status,occurrences,first_seen_at,last_seen_at,resolved_at",
    }
    }
}
pub(crate) fn decode<T: DeserializeOwned>(v: Value) -> Result<T> {
    serde_json::from_value(v).map_err(|_| ConnectorError::Failed(Error::Internal))
}
pub(crate) fn encode<T: Serialize>(v: &T) -> Result<Value> {
    serde_json::to_value(v).map_err(|_| ConnectorError::Failed(Error::Internal))
}
pub(crate) async fn rows(
    client: &impl GenericClient,
    table: Table,
    c: &Connection,
) -> Result<Vec<Value>> {
    Ok(client
        .query(
            &format!(
                "SELECT to_jsonb(t) FROM {} t WHERE tenant_id=$1 AND connection_id=$2 ORDER BY id",
                table.name()
            ),
            &[&c.tenant_id, &c.id],
        )
        .await
        .map_err(Error::from)?
        .into_iter()
        .map(|r| r.get(0))
        .collect())
}
/// Table and columns come only from the closed enum, never input. PostgreSQL's
/// record conversion keeps timestamps and nullable fields typed in one place.
pub(crate) async fn save(
    tx: &mut MutationTransaction<'_>,
    table: Table,
    before: Option<Value>,
    mut values: Value,
) -> Result<Value> {
    if matches!(table, Table::Checkpoint) && values.get("bootstrap_required").is_none() {
        values["bootstrap_required"] = json!(false);
    }
    Ok(tx.mutate(move |sql| Box::pin(async move {
        let name = table.name();
        let cols = table.columns();
        let row = if let Some(ref old) = before {
            let id = old["id"].as_i64().ok_or(Error::Internal)?;
            let tenant = old["tenant_id"].as_i64().ok_or(Error::Internal)?;
            sql.query_one(&format!("UPDATE {name} t SET ({cols})=(SELECT {cols} FROM jsonb_populate_record(NULL::{name},$1)), updated_at=now() WHERE id=$2 AND tenant_id=$3 RETURNING to_jsonb(t)"),&[&values,&id,&tenant]).await?
        } else {
            sql.query_one(&format!("INSERT INTO {name} AS t ({cols}) SELECT {cols} FROM jsonb_populate_record(NULL::{name},$1) RETURNING to_jsonb(t)"),&[&values]).await?
        };
        let after: Value = row.get(0);
        let change = Change { kind: table.kind().into(), id: after["id"].as_i64().ok_or(Error::Internal)?, old: before.unwrap_or_else(||json!({})), new: after.clone() };
        Ok((after,vec![change]))
    })).await?)
}
async fn delete_all(tx: &mut MutationTransaction<'_>, table: Table, c: &Connection) -> Result<()> {
    let tenant = c.tenant_id;
    let id = c.id;
    // The caller holds the connection lock. MutationBoundary refuses an empty
    // Change set, so decide this no-op before entering mutate.
    if tx
        .sql()
        .query_opt(
            &format!(
                "SELECT 1 FROM {} WHERE tenant_id=$1 AND connection_id=$2 LIMIT 1",
                table.name()
            ),
            &[&tenant, &id],
        )
        .await
        .map_err(Error::from)?
        .is_none()
    {
        return Ok(());
    }
    tx.mutate(move |sql| Box::pin(async move {
        let rows = sql.query(&format!("DELETE FROM {} t WHERE tenant_id=$1 AND connection_id=$2 RETURNING to_jsonb(t)",table.name()),&[&tenant,&id]).await?;
        let changes = rows.into_iter().map(|r| {
            let old: Value = r.get(0);
            Ok(Change { kind: table.kind().into(), id: old["id"].as_i64().ok_or(Error::Internal)?, old, new: json!({}) })
        }).collect::<std::result::Result<Vec<_>,Error>>()?;
        Ok(((),changes))
    })).await?;
    Ok(())
}
pub(crate) async fn reset(tx: &mut MutationTransaction<'_>, c: &Connection) -> Result<()> {
    delete_all(tx, Table::Record, c).await?;
    delete_all(tx, Table::Checkpoint, c).await
}
pub(crate) async fn remove(tx: &mut MutationTransaction<'_>, c: &Connection) -> Result<()> {
    reset(tx, c).await?;
    delete_all(tx, Table::Issue, c).await?;
    delete_all(tx, Table::Run, c).await
}
