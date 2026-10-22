//! Private persistence for Bilimbi Connector.Backup.Record. Tokens never enter stored bytes.
use crate::{ConnectorError, Result};
use bilibi_database::{Change, Error, MutationTransaction};
use serde_json::{json, Value};
use tokio_postgres::GenericClient;

pub(crate) async fn fetch(
    client: &(impl GenericClient + Sync),
    tenant: i64,
    company: i64,
    id: &str,
    locked: bool,
) -> Result<Value> {
    uuid::Uuid::parse_str(id).map_err(|_| ConnectorError::NotFound)?;
    client.query_opt(&format!("SELECT to_jsonb(b) FROM people_connector_backups b WHERE tenant_id=$1 AND platform_company_id=$2 AND id=$3::text::uuid{}",if locked {" FOR UPDATE"}else{""}),&[&tenant,&company,&id]).await.map_err(Error::from)?.map(|r|r.get(0)).ok_or(ConnectorError::NotFound)
}
pub(crate) async fn save(
    tx: &mut MutationTransaction<'_>,
    before: Option<Value>,
    values: Value,
) -> Result<Value> {
    Ok(tx.mutate(move |sql| Box::pin(async move {
        let row = if before.is_none() {
            sql.query_one("INSERT INTO people_connector_backups (id,tenant_id,platform_company_id,connection_id,sha256,state,expires_at) SELECT ($1::jsonb->>'id')::uuid,($1::jsonb->>'tenant_id')::bigint,($1::jsonb->>'platform_company_id')::bigint,($1::jsonb->>'connection_id')::bigint,$1::jsonb->>'sha256',$1::jsonb->>'state',($1::jsonb->>'expires_at')::timestamptz RETURNING to_jsonb(people_connector_backups.*)",&[&values]).await?
        }else {
            sql.query_one("UPDATE people_connector_backups b SET (connection_id,state,artifact_id,expires_at,preview_token_hash,preview_state_hash,preview_actor_id,preview_impersonator_id,preview_expires_at,recovery_generation,restored_at,purge_attempts,purge_last_error,purge_attempted_at,purge_held_at)=(SELECT connection_id,state,artifact_id,expires_at,preview_token_hash,preview_state_hash,preview_actor_id,preview_impersonator_id,preview_expires_at,recovery_generation,restored_at,purge_attempts,purge_last_error,purge_attempted_at,purge_held_at FROM jsonb_populate_record(NULL::people_connector_backups,$1)),updated_at=clock_timestamp() WHERE tenant_id=($1::jsonb->>'tenant_id')::bigint AND platform_company_id=($1::jsonb->>'platform_company_id')::bigint AND id=($1::jsonb->>'id')::uuid RETURNING to_jsonb(b)",&[&values]).await?
        };
        let after:Value=row.get(0);
        let change=Change{kind:"people_connector_backup".into(),id:after["audit_id"].as_i64().ok_or(Error::Internal)?,old:before.unwrap_or_else(||json!({})),new:after.clone()};
        Ok((after,vec![change]))
    })).await?)
}
pub(crate) async fn detach(
    tx: &mut MutationTransaction<'_>,
    tenant: i64,
    connection: i64,
) -> Result<()> {
    loop {
        let rows:Vec<Value>=tx.sql().query("SELECT to_jsonb(b) FROM people_connector_backups b WHERE tenant_id=$1 AND connection_id=$2 ORDER BY audit_id LIMIT 100 FOR UPDATE",&[&tenant,&connection]).await.map_err(Error::from)?.into_iter().map(|r|r.get(0)).collect();
        if rows.is_empty() {
            break;
        }
        for before in rows {
            let mut after = before.clone();
            after["connection_id"] = Value::Null;
            save(tx, Some(before), after).await?;
        }
    }
    Ok(())
}
