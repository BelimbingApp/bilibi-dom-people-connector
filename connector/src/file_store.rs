//! Private receipt persistence (Bilimbi FileExchange.Record). Every affected receipt is captured.
use crate::{ConnectorError, Result};
use bilibi_database::{Change, Error, MutationTransaction};
use serde_json::{json, Value};
use tokio_postgres::GenericClient;

pub(crate) async fn fetch(
    client: &(impl GenericClient + Sync),
    tenant: i64,
    company: i64,
    id: &str,
) -> Result<Value> {
    uuid::Uuid::parse_str(id).map_err(|_| ConnectorError::NotFound)?;
    client.query_opt("SELECT to_jsonb(f) FROM people_connector_file_exchanges f WHERE tenant_id=$1 AND platform_company_id=$2 AND id=$3::text::uuid",&[&tenant,&company,&id])
        .await.map_err(Error::from)?.map(|r|r.get(0)).ok_or(ConnectorError::NotFound)
}
pub(crate) async fn matching(
    client: &(impl GenericClient + Sync),
    tenant: i64,
    connection: i64,
    direction: &str,
    hash: &str,
) -> Result<Vec<Value>> {
    Ok(client.query("SELECT to_jsonb(f) FROM people_connector_file_exchanges f WHERE tenant_id=$1 AND connection_id=$2 AND direction=$3 AND sha256=$4 AND (state='pending' OR (state='ready' AND expires_at>clock_timestamp()) OR (state='failed' AND failure_reason IS NULL)) ORDER BY inserted_at DESC,id DESC LIMIT 20 FOR UPDATE",&[&tenant,&connection,&direction,&hash])
        .await.map_err(Error::from)?.into_iter().map(|r|r.get(0)).collect())
}
pub(crate) async fn recent(
    client: &(impl GenericClient + Sync),
    tenant: i64,
    company: i64,
) -> Result<Vec<Value>> {
    Ok(client.query("SELECT to_jsonb(f) FROM people_connector_file_exchanges f WHERE tenant_id=$1 AND platform_company_id=$2 ORDER BY inserted_at DESC,id DESC LIMIT 20",&[&tenant,&company])
        .await.map_err(Error::from)?.into_iter().map(|r|r.get(0)).collect())
}
pub(crate) async fn save(
    tx: &mut MutationTransaction<'_>,
    before: Option<Value>,
    values: Value,
) -> Result<Value> {
    Ok(tx.mutate(move |sql|Box::pin(async move{
        let row = if before.is_none(){
            sql.query_one("INSERT INTO people_connector_file_exchanges (id,tenant_id,platform_company_id,connection_id,workforce_source_id,workforce_company_id,direction,sha256,record_count,state) SELECT ($1::jsonb->>'id')::uuid,($1->>'tenant_id')::bigint,($1->>'platform_company_id')::bigint,($1->>'connection_id')::bigint,$1->>'workforce_source_id',($1->>'workforce_company_id')::bigint,$1->>'direction',$1->>'sha256',($1->>'record_count')::integer,$1->>'state' RETURNING to_jsonb(people_connector_file_exchanges.*)",&[&values]).await?
        }else{
            sql.query_one("UPDATE people_connector_file_exchanges SET state=$1::jsonb->>'state',artifact_id=$1->>'artifact_id',expires_at=($1->>'expires_at')::timestamptz,failure_reason=$1->>'failure_reason',connection_id=($1->>'connection_id')::bigint,updated_at=clock_timestamp() WHERE tenant_id=($1->>'tenant_id')::bigint AND platform_company_id=($1->>'platform_company_id')::bigint AND id=($1->>'id')::uuid RETURNING to_jsonb(people_connector_file_exchanges.*)",&[&values]).await?
        };
        let after:Value=row.get(0);
        let change=Change{kind:"people_connector_file_exchange".into(),id:after["audit_id"].as_i64().ok_or(Error::Internal)?,old:before.unwrap_or_else(||json!({})),new:after.clone()};
        Ok((after,vec![change]))
    })).await?)
}
// A connection removal preserves the receipt and audits its emptied reference.
pub(crate) async fn detach(
    tx: &mut MutationTransaction<'_>,
    tenant: i64,
    connection: i64,
) -> Result<()> {
    loop {
        let rows:Vec<Value>=tx.sql().query("SELECT to_jsonb(f) FROM people_connector_file_exchanges f WHERE tenant_id=$1 AND connection_id=$2 ORDER BY audit_id LIMIT 100 FOR UPDATE",&[&tenant,&connection]).await.map_err(Error::from)?.into_iter().map(|r|r.get(0)).collect();
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
