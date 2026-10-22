//! Bilimbi Connector.Retention: bounded, independently committed retention with retry holds.
use crate::{authorize, connections, require_writable, Connector, ConnectorError, Result};
use bilibi_database::{Change, Error, MutationTransaction};
use bilibi_party_ids::OrganisationId;
use bilibi_settings::Target;
use bilibi_tenancy::Scope;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::GenericClient;

/// Field, persisted setting key, label, maximum, nullable.
pub const RETENTION_FIELDS: [(&str, &str, &str, i64, bool); 5] = [
    (
        "sync_days",
        "people-connector.retention.sync_days",
        "Sync runs (days)",
        3650,
        true,
    ),
    (
        "webhook_days",
        "people-connector.retention.webhook_days",
        "Webhook deliveries (days)",
        3650,
        true,
    ),
    (
        "file_days",
        "people-connector.retention.file_days",
        "File receipts (days)",
        3650,
        true,
    ),
    (
        "batch_size",
        "people-connector.retention.batch_size",
        "Batch size per record type",
        1000,
        false,
    ),
    (
        "retry_minutes",
        "people-connector.retention.retry_minutes",
        "Retry delay (minutes)",
        1440,
        false,
    ),
];
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub sync_days: Option<i64>,
    pub webhook_days: Option<i64>,
    pub file_days: Option<i64>,
    pub batch_size: i64,
    pub retry_minutes: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct RetentionRecord {
    pub kind: String,
    pub id: String,
    /// A fixed actionable sentence, never a database or storage exception.
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct PurgeResult {
    pub deleted: Vec<RetentionRecord>,
    pub errors: Vec<RetentionRecord>,
}
#[derive(Clone, Copy)]
enum Kind {
    Sync,
    Webhook,
    Nonce,
    File,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Webhook => "webhook",
            Self::Nonce => "nonce",
            Self::File => "file",
        }
    }
    fn table(self) -> &'static str {
        match self {
            Self::Sync => "people_connector_sync_runs",
            Self::Webhook => "people_connector_webhook_deliveries",
            Self::Nonce => "people_connector_webhook_nonces",
            Self::File => "people_connector_file_exchanges",
        }
    }
    fn audit_kind(self) -> &'static str {
        match self {
            Self::Sync => "people_connector_sync_run",
            Self::Webhook => "people_connector_webhook_delivery",
            Self::Nonce => "people_connector_webhook_nonce",
            Self::File => "people_connector_file_exchange",
        }
    }
    fn days(self, p: RetentionPolicy) -> Option<i64> {
        match self {
            Self::Sync => p.sync_days,
            Self::File => p.file_days,
            _ => p.webhook_days,
        }
    }
    // Identifiers and predicates are selected exclusively from this closed enum.
    fn eligibility(self) -> &'static str {
        match self {
        Self::Sync => "r.state <> 'running' AND r.finished_at < clock_timestamp()-make_interval(days=>$3::int) AND r.id <> (SELECT x.id FROM people_connector_sync_runs x WHERE x.tenant_id=r.tenant_id AND x.connection_id=r.connection_id ORDER BY x.started_at DESC,x.id DESC LIMIT 1) AND r.id IS DISTINCT FROM (SELECT x.id FROM people_connector_sync_runs x WHERE x.tenant_id=r.tenant_id AND x.connection_id=r.connection_id AND x.state='succeeded' ORDER BY x.started_at DESC,x.id DESC LIMIT 1)",
        Self::File => "r.state <> 'pending' AND r.inserted_at < clock_timestamp()-make_interval(days=>$3::int) AND (r.expires_at IS NULL OR r.expires_at<=clock_timestamp())",
        _ => "r.received_at < (clock_timestamp() AT TIME ZONE 'UTC')-make_interval(secs=>greatest($3::int*86400,2*(SELECT c.webhook_max_skew_seconds FROM people_connector_connections c WHERE c.tenant_id=r.tenant_id AND c.id=r.connection_id)))",
    }
    }
}
impl Connector {
    async fn retention_values(
        &self,
        client: &(impl GenericClient + Sync),
        scope: &Scope,
        company: i64,
    ) -> Result<RetentionPolicy> {
        let keys = RETENTION_FIELDS.map(|f| f.1);
        let resolved = self
            .settings
            .resolve_many(
                client,
                scope,
                Target::Company(OrganisationId::new(company)?),
                &keys,
            )
            .await?;
        let values = RETENTION_FIELDS
            .into_iter()
            .map(|(field, key, _, _, _)| {
                (
                    field.to_owned(),
                    resolved
                        .get(key)
                        .map(|v| v.value.clone())
                        .unwrap_or(Value::Null),
                )
            })
            .collect();
        serde_json::from_value(Value::Object(values))
            .map_err(|_| ConnectorError::InvalidRetentionPolicy)
    }
    pub async fn retention_policy(&self, scope: &Scope, company: i64) -> Result<RetentionPolicy> {
        let client = self.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        self.retention_values(&**client, scope, company).await
    }
    /// Partial update: unknown fields, fractional numbers and out-of-range values refuse atomically.
    pub async fn put_retention_policy(
        &self,
        scope: &Scope,
        company: i64,
        changes: &serde_json::Map<String, Value>,
    ) -> Result<RetentionPolicy> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        for (field, value) in changes {
            let Some((_, _, _, max, nullable)) = RETENTION_FIELDS.iter().find(|f| f.0 == field)
            else {
                return Err(ConnectorError::InvalidRetentionPolicy);
            };
            if !(value.is_null() && *nullable
                || value.as_i64().is_some_and(|v| (1..=*max).contains(&v)))
            {
                return Err(ConnectorError::InvalidRetentionPolicy);
            }
        }
        let mut tx = self
            .begin(&mut client, scope, "people-connector.retention.policy")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        for (field, key, _, _, _) in RETENTION_FIELDS {
            if let Some(value) = changes.get(field) {
                self.settings
                    .put(
                        &mut tx,
                        scope,
                        Target::Company(OrganisationId::new(company)?),
                        key,
                        value.clone(),
                    )
                    .await?;
            }
        }
        action(
            &tx,
            "people-connector.retention.policy",
            company,
            json!({"fields":changes.keys().collect::<Vec<_>>()}),
            false,
        )
        .await?;
        let policy = self.retention_values(tx.sql(), scope, company).await?;
        tx.commit().await?;
        Ok(policy)
    }
    /// Every candidate commits separately. Failed rows are held so later batches can advance.
    pub async fn purge(&self, scope: &Scope, company: i64) -> Result<PurgeResult> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let tx = self
            .begin(&mut client, scope, "people-connector.retention.started")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let policy = self.retention_values(tx.sql(), scope, company).await?;
        action(
            &tx,
            "people-connector.retention.started",
            company,
            json!({"policy":policy}),
            false,
        )
        .await?;
        tx.commit().await?;
        drop(client);
        let mut result = PurgeResult::default();
        for kind in [Kind::Sync, Kind::Webhook, Kind::Nonce, Kind::File] {
            let rows = {
                let client = self.db.get().await.map_err(Error::from)?;
                candidates(&**client, scope, company, kind, policy, None, false).await?
            };
            for row in rows {
                let id = id(&row)?;
                let outcome = self.purge_retained_row(scope, company, kind, &id).await;
                let record = RetentionRecord {
                    kind: kind.name().into(),
                    id: id.clone(),
                    reason: None,
                };
                match outcome {
                    Ok(()) => result.deleted.push(record),
                    Err(error) => {
                        // Failure bookkeeping is its own transaction after rollback of the row.
                        let code = match &error {
                            ConnectorError::ArtifactCleanupFailed => "artifact_cleanup_failed",
                            ConnectorError::NoLongerEligible => "no_longer_eligible",
                            ConnectorError::Unauthorized => "unauthorized",
                            _ => "record_unavailable",
                        };
                        self.retain_failure(scope, company, kind, &id, code).await?;
                        let reason=match error {
                            ConnectorError::ArtifactCleanupFailed=>ConnectorError::ArtifactCleanupFailed.to_string(),
                            ConnectorError::NoLongerEligible=>ConnectorError::NoLongerEligible.to_string(),
                            ConnectorError::Unauthorized|ConnectorError::CompanyArchived=>error.to_string(),
                            _=>"Record kept because the operation failed. Check database and audit storage, then retry.".into(),
                        };
                        result.errors.push(RetentionRecord {
                            reason: Some(reason),
                            ..record
                        });
                    }
                }
            }
        }
        Ok(result)
    }
    async fn purge_retained_row(
        &self,
        scope: &Scope,
        company: i64,
        kind: Kind,
        id: &str,
    ) -> Result<()> {
        if matches!(kind, Kind::File) {
            let row = {
                let client = self.db.get().await.map_err(Error::from)?;
                authorize(&**client, scope, company).await?;
                let p = self.retention_values(&**client, scope, company).await?;
                candidates(&**client, scope, company, kind, p, Some(id), false)
                    .await?
                    .pop()
                    .ok_or(ConnectorError::NoLongerEligible)?
            };
            // Artifacts commits tombstoning and byte cleanup before we can remove its authorization receipt.
            if let Some(artifact) = row["artifact_id"].as_str() {
                self.cleanup_retained_file(scope, company, artifact).await?;
            }
        }
        let mut client = self.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.retention.purged")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let p = self.retention_values(tx.sql(), scope, company).await?;
        let before = candidates(tx.sql(), scope, company, kind, p, Some(id), true)
            .await?
            .pop()
            .ok_or(ConnectorError::NoLongerEligible)?;
        let tenant = scope.tenant();
        let record_id = id.to_owned();
        let table = kind.table();
        tx.mutate(move|sql|Box::pin(async move {
            let deleted=sql.execute(&format!("DELETE FROM {table} WHERE tenant_id=$1 AND id::text=$2"),&[&tenant,&record_id]).await?;
            if deleted!=1 {return Err(Error::Conflict)}
            let mut changes=vec![Change{kind:kind.audit_kind().into(),id:before[if matches!(kind,Kind::File){"audit_id"}else{"id"}].as_i64().ok_or(Error::Internal)?,old:before,new:json!({})}];
            for row in sql.query("DELETE FROM people_connector_retention_attempts WHERE tenant_id=$1 AND platform_company_id=$2 AND kind=$3 AND record_id=$4 RETURNING to_jsonb(people_connector_retention_attempts.*)",&[&tenant,&company,&kind.name(),&record_id]).await? {
                let old:Value=row.get(0);changes.push(Change{kind:"people_connector_retention_attempt".into(),id:old["id"].as_i64().ok_or(Error::Internal)?,old,new:json!({})});
            }
            Ok(((),changes))
        })).await?;
        action(
            &tx,
            "people-connector.retention.purged",
            company,
            json!({"kind":kind.name(),"record_id":id}),
            false,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn retain_failure(
        &self,
        scope: &Scope,
        company: i64,
        kind: Kind,
        id: &str,
        reason: &str,
    ) -> Result<()> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.retention.failed")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let tenant = scope.tenant();
        let record_id = id.to_owned();
        let before=tx.sql().query_opt("SELECT to_jsonb(a) FROM people_connector_retention_attempts a WHERE tenant_id=$1 AND platform_company_id=$2 AND kind=$3 AND record_id=$4 FOR UPDATE",&[&tenant,&company,&kind.name(),&record_id]).await.map_err(Error::from)?.map(|r|r.get::<_,Value>(0)).unwrap_or(json!({}));
        tx.mutate(move|sql|Box::pin(async move {
            let row=sql.query_one("INSERT INTO people_connector_retention_attempts (tenant_id,platform_company_id,kind,record_id,attempted_at) VALUES ($1,$2,$3,$4,clock_timestamp()) ON CONFLICT (platform_company_id,kind,record_id) DO UPDATE SET attempted_at=EXCLUDED.attempted_at RETURNING to_jsonb(people_connector_retention_attempts.*)",&[&tenant,&company,&kind.name(),&record_id]).await?;
            let after:Value=row.get(0);Ok(((),vec![Change{kind:"people_connector_retention_attempt".into(),id:after["id"].as_i64().ok_or(Error::Internal)?,old:before,new:after}]))
        })).await?;
        action(
            &tx,
            "people-connector.retention.failed",
            company,
            json!({"kind":kind.name(),"record_id":id,"reason":reason}),
            true,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
fn id(row: &Value) -> Result<String> {
    match &row["id"] {
        Value::String(id) => Ok(id.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(ConnectorError::Failed(Error::Internal)),
    }
}
async fn candidates(
    client: &(impl GenericClient + Sync),
    scope: &Scope,
    company: i64,
    kind: Kind,
    p: RetentionPolicy,
    id: Option<&str>,
    lock: bool,
) -> Result<Vec<Value>> {
    let Some(days) = kind.days(p) else {
        return Ok(vec![]);
    };
    let table = kind.table();
    let owner = if matches!(kind, Kind::File) {
        "r.platform_company_id=$2"
    } else {
        "r.connection_id IN (SELECT c.id FROM people_connector_connections c WHERE c.tenant_id=$1 AND c.platform_company_id=$2)"
    };
    let query=format!("SELECT to_jsonb(r) FROM {table} r WHERE r.tenant_id=$1 AND {owner} AND {} AND ($4::text IS NULL OR r.id::text=$4) AND NOT EXISTS (SELECT 1 FROM people_connector_retention_attempts a WHERE a.tenant_id=$1 AND a.platform_company_id=$2 AND a.kind=$5 AND a.record_id=r.id::text AND a.attempted_at>clock_timestamp()-make_interval(mins=>$6::int)) ORDER BY r.id LIMIT $7 {}",kind.eligibility(),if lock{"FOR UPDATE OF r"}else{""});
    Ok(client
        .query(
            &query,
            &[
                &scope.tenant(),
                &company,
                &(days as i32),
                &id,
                &kind.name(),
                &(p.retry_minutes as i32),
                &p.batch_size,
            ],
        )
        .await
        .map_err(Error::from)?
        .into_iter()
        .map(|r| r.get(0))
        .collect())
}
pub(crate) async fn action(
    tx: &MutationTransaction<'_>,
    event: &str,
    company: i64,
    mut details: Value,
    failed: bool,
) -> Result<()> {
    details["company_id"] = json!(company);
    details["result"] = json!(if failed { "failed" } else { "succeeded" });
    bilibi_audit::record_action(
        tx.sql(),
        &bilibi_audit::Attribution::from_context(tx.context())?,
        None,
        event,
        details,
        false,
    )
    .await?;
    Ok(())
}
