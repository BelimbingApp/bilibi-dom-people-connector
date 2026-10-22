//! Private persistence mirroring Bilimbi Connector.Webhooks.{Nonce,Delivery}.
//! The caller holds the company and connection locks before every mutation.
use crate::{connections::Connection, ConnectorError, Result};
use bilibi_database::{Change, Error, MutationTransaction};
use jiff::Timestamp;
use serde_json::{json, Value};
use tokio_postgres::GenericClient;

pub(crate) const DEFAULT_SKEW_SECONDS: i32 = 300;

// No Debug: even the encrypted envelope stays out of diagnostics.
#[derive(PartialEq)]
pub(crate) struct Policy {
    pub secret: Option<Value>,
    pub enabled: bool,
    pub max_skew_seconds: i32,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            secret: None,
            enabled: false,
            max_skew_seconds: DEFAULT_SKEW_SECONDS,
        }
    }
}
impl Policy {
    fn audit_values(&self, company: i64) -> Value {
        // The registered Audit redaction owns masking webhook_secret, including
        // any future caller that captures the complete connection row.
        json!({"platform_company_id":company,"webhook_secret":self.secret,
            "webhook_enabled":self.enabled,"webhook_max_skew_seconds":self.max_skew_seconds})
    }
}

pub(crate) async fn policy(client: &impl GenericClient, c: &Connection) -> Result<Policy> {
    let row = client.query_opt(
        "SELECT webhook_secret,webhook_enabled,webhook_max_skew_seconds FROM people_connector_connections WHERE tenant_id=$1 AND id=$2",
        &[&c.tenant_id,&c.id],
    ).await.map_err(Error::from)?.ok_or(ConnectorError::Disconnected)?;
    Ok(Policy {
        secret: row.get(0),
        enabled: row.get(1),
        max_skew_seconds: row.get(2),
    })
}

/// The backup contract excludes even the encrypted secret envelope.
pub(crate) async fn nonsecret_policy(
    client: &(impl GenericClient + Sync),
    c: &Connection,
) -> Result<(bool, i32)> {
    let row=client.query_opt("SELECT webhook_enabled,webhook_max_skew_seconds FROM people_connector_connections WHERE tenant_id=$1 AND id=$2", &[&c.tenant_id,&c.id]).await.map_err(Error::from)?.ok_or(ConnectorError::Disconnected)?;
    Ok((row.get(0), row.get(1)))
}

pub(crate) async fn save_policy(
    tx: &mut MutationTransaction<'_>,
    c: &Connection,
    before: Policy,
    after: Policy,
) -> Result<()> {
    if before == after {
        return Ok(());
    }
    let (tenant, id, company) = (c.tenant_id, c.id, c.platform_company_id);
    tx.mutate(move |sql| Box::pin(async move {
        let changed = sql.execute(
            "UPDATE people_connector_connections SET webhook_secret=$3,webhook_enabled=$4,webhook_max_skew_seconds=$5,updated_at=now() AT TIME ZONE 'UTC' WHERE tenant_id=$1 AND id=$2",
            &[&tenant,&id,&after.secret,&after.enabled,&after.max_skew_seconds],
        ).await?;
        if changed != 1 { return Err(Error::Conflict); }
        Ok(((),vec![Change { kind:"people_connector_connection".into(),id,
            old:before.audit_values(company),new:after.audit_values(company) }]))
    })).await?;
    Ok(())
}

pub(crate) async fn clear(tx: &mut MutationTransaction<'_>, c: &Connection) -> Result<()> {
    let before = policy(tx.sql(), c).await?;
    save_policy(tx, c, before, Policy::default()).await
}

pub(crate) async fn last_received(
    client: &impl GenericClient,
    c: &Connection,
) -> Result<Option<Timestamp>> {
    let row = client.query_opt(
        "SELECT to_char(received_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM people_connector_webhook_deliveries WHERE tenant_id=$1 AND connection_id=$2 ORDER BY received_at DESC,id DESC LIMIT 1",
        &[&c.tenant_id,&c.id],
    ).await.map_err(Error::from)?;
    row.map(|r| {
        r.get::<_, String>(0)
            .parse()
            .map_err(|_| ConnectorError::Failed(Error::Internal))
    })
    .transpose()
}

pub(crate) async fn nonce_used(
    client: &impl GenericClient,
    c: &Connection,
    hash: &str,
) -> Result<bool> {
    Ok(client.query_opt(
        "SELECT 1 FROM people_connector_webhook_nonces WHERE tenant_id=$1 AND connection_id=$2 AND nonce_hash=$3",
        &[&c.tenant_id,&c.id,&hash],
    ).await.map_err(Error::from)?.is_some())
}

pub(crate) async fn delivery_body(
    client: &impl GenericClient,
    c: &Connection,
    hash: &str,
) -> Result<Option<String>> {
    Ok(client.query_opt(
        "SELECT body_hash FROM people_connector_webhook_deliveries WHERE tenant_id=$1 AND connection_id=$2 AND delivery_hash=$3",
        &[&c.tenant_id,&c.id,&hash],
    ).await.map_err(Error::from)?.map(|r| r.get(0)))
}

/// All refusals have already been decided with sql(), under the row lock.
pub(crate) async fn receive(
    tx: &mut MutationTransaction<'_>,
    c: &Connection,
    nonce_hash: String,
    delivery_hash: String,
    body_hash: String,
    first: bool,
) -> Result<()> {
    let (tenant, connection) = (c.tenant_id, c.id);
    tx.mutate(move |sql| Box::pin(async move {
        let row = sql.query_one(
            "INSERT INTO people_connector_webhook_nonces AS n (tenant_id,connection_id,nonce_hash) VALUES($1,$2,$3) RETURNING to_jsonb(n)",
            &[&tenant,&connection,&nonce_hash],
        ).await?;
        let mut changes = vec![created("people_connector_webhook_nonce",row.get(0))?];
        if first {
            let row = sql.query_one(
                "INSERT INTO people_connector_webhook_deliveries AS d (tenant_id,connection_id,delivery_hash,body_hash) VALUES($1,$2,$3,$4) RETURNING to_jsonb(d)",
                &[&tenant,&connection,&delivery_hash,&body_hash],
            ).await?;
            changes.push(created("people_connector_webhook_delivery",row.get(0))?);
        }
        Ok(((),changes))
    })).await?;
    Ok(())
}

fn created(kind: &str, values: Value) -> std::result::Result<Change, Error> {
    Ok(Change {
        kind: kind.into(),
        id: values["id"].as_i64().ok_or(Error::Internal)?,
        old: json!({}),
        new: values,
    })
}

/// Explicit deletion captures every receipt before the connection's cascading
/// FK runs. Table names come only from this closed list, never operator input.
pub(crate) async fn remove(tx: &mut MutationTransaction<'_>, c: &Connection) -> Result<()> {
    for (table, kind) in [
        (
            "people_connector_webhook_nonces",
            "people_connector_webhook_nonce",
        ),
        (
            "people_connector_webhook_deliveries",
            "people_connector_webhook_delivery",
        ),
    ] {
        let (tenant, connection) = (c.tenant_id, c.id);
        if tx
            .sql()
            .query_opt(
                &format!("SELECT 1 FROM {table} WHERE tenant_id=$1 AND connection_id=$2 LIMIT 1"),
                &[&tenant, &connection],
            )
            .await
            .map_err(Error::from)?
            .is_none()
        {
            continue;
        }
        tx.mutate(move |sql| Box::pin(async move {
            let rows = sql.query(&format!("DELETE FROM {table} AS r WHERE tenant_id=$1 AND connection_id=$2 RETURNING to_jsonb(r)"),&[&tenant,&connection]).await?;
            let changes = rows.into_iter().map(|row| {
                let old: Value = row.get(0);
                Ok(Change {kind:kind.into(),id:old["id"].as_i64().ok_or(Error::Internal)?,old,new:json!({})})
            }).collect::<std::result::Result<Vec<_>,Error>>()?;
            Ok(((),changes))
        })).await?;
    }
    Ok(())
}
