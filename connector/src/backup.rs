//! Bilimbi Connector.Backup: private snapshots, actor/state-bound restore and ordinary sync recovery.
use crate::{
    authorize, backup_store as store, connections, people, require_mapping, require_writable,
    sync_store, Connection, Connector, ConnectorError, Result, Status, SyncOptions, SyncRun,
    CREDENTIAL_KEY, NATIVE_PROVIDER_ID,
};
use bilibi_artifacts::{Operation, Owner, Reference};
use bilibi_database::{Error, MutationTransaction};
use bilibi_party_ids::OrganisationId;
use bilibi_settings::Target;
use bilibi_tenancy::{OrganisationDirectory, ProofClient, ProofFuture, Scope};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tokio_postgres::GenericClient;

const POLICY_KEYS: [&str; 2] = [
    "people-connector.backup.retention_days",
    "people-connector.backup.preview_minutes",
];
const SETTING_KEYS: [&str; 10] = [
    "people-connector.sync.page_limit",
    "people-connector.sync.max_age_minutes",
    "people-connector.sync.run_timeout_minutes",
    "people-connector.files.enabled",
    "people-connector.files.json_enabled",
    "people-connector.files.max_bytes",
    "people-connector.files.max_records",
    "people-connector.files.stale_minutes",
    "people-connector.webhook.max_skew_seconds",
    "people-connector.webhook.enabled",
];
const CONNECTION_FIELDS: [&str; 5] = [
    "provider_id",
    "provider_contract_version",
    "workforce_source_id",
    "workforce_company_id",
    "enabled",
];
const CHECKPOINT_FIELDS: [&str; 4] = ["version", "resume_cursor", "as_of_at", "bootstrap_required"];
const RECORD_FIELDS: [&str; 17] = [
    "kind",
    "source_id",
    "stable_id",
    "workforce_company_id",
    "active",
    "name",
    "code",
    "email",
    "supervisor_stable_id",
    "parent_stable_id",
    "version",
    "vacant",
    "assignments_incomplete",
    "assignments",
    "content_hash",
    "observed_at",
    "deactivated_at",
];

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct BackupPolicy {
    pub retention_days: i64,
    pub preview_minutes: i64,
}
/// Partial policy writes keep another operator's unrelated inline edit.
#[derive(Clone, Copy, Debug, Default)]
pub struct BackupPolicyUpdate {
    pub retention_days: Option<i64>,
    pub preview_minutes: Option<i64>,
}
impl From<BackupPolicy> for BackupPolicyUpdate {
    fn from(policy: BackupPolicy) -> Self {
        Self {
            retention_days: Some(policy.retention_days),
            preview_minutes: Some(policy.preview_minutes),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupReceipt {
    pub id: String,
    pub state: String,
    pub artifact_id: Option<String>,
    pub inserted_at: String,
    pub expires_at: String,
    pub restored_at: Option<String>,
    pub purge_attempts: i32,
    pub purge_last_error: Option<String>,
    pub purge_attempted_at: Option<String>,
    pub purge_held_at: Option<String>,
}
#[derive(Clone, Debug)]
pub struct BackupSummary {
    pub policy: BackupPolicy,
    pub records: Vec<BackupReceipt>,
    pub holds: Vec<BackupReceipt>,
}
#[derive(Clone, Debug)]
pub struct BackupChange {
    pub label: String,
    pub before: Value,
    pub after: Value,
    pub instant: bool,
}
/// The confirmation token stays in the server's page state and is never rendered in HTML.
pub struct BackupPreview {
    pub id: String,
    pub token: String,
    pub changes: Vec<BackupChange>,
}
#[derive(Clone, Debug)]
pub struct BackupRestore {
    pub id: String,
    pub restored_at: String,
    pub replayed: bool,
}
#[derive(Clone, Debug, Default)]
pub struct BackupPurge {
    pub deleted: Vec<String>,
    pub errors: Vec<String>,
}
/// Backup workflow reached only through the Connector facade.
pub struct Backup<'a> {
    connector: &'a Connector,
}
impl Connector {
    pub fn backups(&self) -> Backup<'_> {
        Backup { connector: self }
    }
}
fn digest(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn at(row: &Value, key: &str) -> Option<Timestamp> {
    row[key].as_str()?.parse().ok()
}
fn live(row: &Value) -> bool {
    at(row, "expires_at").is_some_and(|t| t > Timestamp::now())
}
fn readable(row: &Value) -> Result<()> {
    if row["state"] == "ready" && live(row) {
        Ok(())
    } else {
        Err(ConnectorError::InvalidBackup)
    }
}
fn present(row: Value) -> Result<BackupReceipt> {
    let expired = !live(&row) && row["state"] != "purged";
    let mut receipt: BackupReceipt =
        serde_json::from_value(row).map_err(|_| ConnectorError::InvalidBackup)?;
    if expired {
        receipt.state = "expired".into();
    }
    Ok(receipt)
}
fn subset(row: &Value, fields: &[&str]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|key| row.get(*key).map(|v| ((*key).into(), v.clone())))
            .collect(),
    )
}
async fn audit(
    tx: &MutationTransaction<'_>,
    event: &str,
    company: i64,
    id: Option<&str>,
) -> Result<()> {
    bilibi_audit::record_action(
        tx.sql(),
        &bilibi_audit::Attribution::from_context(tx.context())?,
        None,
        event,
        json!({"company_id":company,"backup_id":id,"result":"succeeded"}),
        false,
    )
    .await?;
    Ok(())
}
impl Backup<'_> {
    async fn policy(
        &self,
        client: &(impl GenericClient + Sync),
        scope: &Scope,
        company: i64,
    ) -> Result<BackupPolicy> {
        let v = self
            .connector
            .settings
            .resolve_many(
                client,
                scope,
                Target::Company(OrganisationId::new(company)?),
                &POLICY_KEYS,
            )
            .await?;
        Ok(BackupPolicy {
            retention_days: v[POLICY_KEYS[0]]
                .value
                .as_i64()
                .ok_or(ConnectorError::InvalidBackupPolicy)?,
            preview_minutes: v[POLICY_KEYS[1]]
                .value
                .as_i64()
                .ok_or(ConnectorError::InvalidBackupPolicy)?,
        })
    }
    pub async fn summary(&self, scope: &Scope, company: i64) -> Result<BackupSummary> {
        let client = self.connector.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        let policy = self.policy(&**client, scope, company).await?;
        let records=client.query("SELECT to_jsonb(b) FROM people_connector_backups b WHERE tenant_id=$1 AND platform_company_id=$2 ORDER BY inserted_at DESC,id DESC LIMIT 20",&[&scope.tenant(),&company]).await.map_err(Error::from)?.into_iter().map(|r|present(r.get(0))).collect::<Result<_>>()?;
        let holds=client.query("SELECT to_jsonb(b) FROM people_connector_backups b WHERE tenant_id=$1 AND platform_company_id=$2 AND state='ready' AND purge_held_at IS NOT NULL ORDER BY purge_held_at,id LIMIT 1000",&[&scope.tenant(),&company]).await.map_err(Error::from)?.into_iter().map(|r|present(r.get(0))).collect::<Result<_>>()?;
        Ok(BackupSummary {
            policy,
            records,
            holds,
        })
    }
    pub async fn configure(
        &self,
        scope: &Scope,
        company: i64,
        policy: BackupPolicyUpdate,
    ) -> Result<BackupPolicy> {
        if policy
            .retention_days
            .is_some_and(|n| !(1..=3650).contains(&n))
            || policy
                .preview_minutes
                .is_some_and(|n| !(1..=60).contains(&n))
        {
            return Err(ConnectorError::InvalidBackupPolicy);
        }
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.policy")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        for (key, value) in POLICY_KEYS
            .into_iter()
            .zip([policy.retention_days, policy.preview_minutes])
        {
            let Some(value) = value else {
                continue;
            };
            self.connector
                .settings
                .put(
                    &mut tx,
                    scope,
                    Target::Company(OrganisationId::new(company)?),
                    key,
                    json!(value),
                )
                .await?;
        }
        audit(&tx, "people-connector.backup.policy", company, None).await?;
        let resolved = self.policy(tx.sql(), scope, company).await?;
        tx.commit().await?;
        Ok(resolved)
    }
    async fn lock(
        &self,
        tx: &MutationTransaction<'_>,
        scope: &Scope,
        company: i64,
    ) -> Result<Connection> {
        require_writable(tx, scope, company).await?;
        let c = connections::find(tx.sql(), scope.tenant(), company, true)
            .await?
            .ok_or(ConnectorError::ConnectionUnavailable)?;
        if c.provider_id != NATIVE_PROVIDER_ID {
            return Err(ConnectorError::ConnectionUnavailable);
        }
        let current = Status::from_workforce(
            people::company(&self.connector.ports, tx, scope, company).await?,
        )?;
        require_mapping(&c, &current)?;
        Ok(c)
    }
    async fn idle(&self, client: &impl GenericClient, c: &Connection) -> Result<()> {
        if client.query_opt("SELECT 1 FROM people_connector_sync_runs WHERE tenant_id=$1 AND connection_id=$2 AND state='running' LIMIT 1",&[&c.tenant_id,&c.id]).await.map_err(Error::from)?.is_some(){return Err(ConnectorError::SyncInProgress);}
        Ok(())
    }
    async fn snapshot(
        &self,
        client: &(impl GenericClient + Sync),
        scope: &Scope,
        c: &Connection,
    ) -> Result<Value> {
        let connection = json!({"provider_id":c.provider_id,"provider_contract_version":c.provider_contract_version,"workforce_source_id":c.workforce_source_id,"workforce_company_id":c.workforce_company_id,"enabled":c.enabled});
        let checkpoint = sync_store::rows(client, sync_store::Table::Checkpoint, c)
            .await?
            .first()
            .map(|v| subset(v, &CHECKPOINT_FIELDS))
            .unwrap_or(Value::Null);
        let max = self
            .connector
            .settings
            .get_global(client, "artifacts.max_bytes")
            .await?
            .as_u64()
            .ok_or(ConnectorError::InvalidBackup)?;
        let mut projections = Vec::new();
        let mut last = 0i64;
        let mut size = 0u64;
        loop {
            let rows=client.query("SELECT to_jsonb(r) FROM people_connector_workforce_records r WHERE tenant_id=$1 AND connection_id=$2 AND id>$3 ORDER BY id LIMIT 1000",&[&c.tenant_id,&c.id,&last]).await.map_err(Error::from)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let raw: Value = row.get(0);
                last = raw["id"].as_i64().ok_or(ConnectorError::InvalidBackup)?;
                let record = subset(&raw, &RECORD_FIELDS);
                size += record.to_string().len() as u64;
                if size > max {
                    return Err(ConnectorError::InvalidBackup);
                }
                projections.push(record);
            }
        }
        projections.sort_by(|a, b| {
            (
                a["kind"].as_str(),
                a["source_id"].as_str(),
                a["stable_id"].as_str(),
            )
                .cmp(&(
                    b["kind"].as_str(),
                    b["source_id"].as_str(),
                    b["stable_id"].as_str(),
                ))
        });
        // KC3 may land independently. Capture registered non-secret policies only;
        // restore always disables intake once its definition exists.
        let keys: Vec<&str> = SETTING_KEYS
            .into_iter()
            .filter(|key| self.connector.settings.definitions().lookup(key).is_ok())
            .collect();
        let settings = self
            .connector
            .settings
            .resolve_many(
                client,
                scope,
                Target::Company(OrganisationId::new(c.platform_company_id)?),
                &keys,
            )
            .await?
            .into_iter()
            .map(|(k, v)| (k, v.value))
            .collect::<serde_json::Map<_, _>>();
        let snapshot = json!({"format":"people-connection-backup-v1","tenant_id":c.tenant_id,"platform_company_id":c.platform_company_id,"connection_id":c.id,"connection":connection,"checkpoint":checkpoint,"projections":projections,"settings":settings});
        if snapshot.to_string().len() as u64 > max {
            return Err(ConnectorError::InvalidBackup);
        }
        Ok(snapshot)
    }
    pub async fn create(&self, scope: &Scope, company: i64) -> Result<BackupReceipt> {
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.reserved")
            .await?;
        let c = self.lock(&tx, scope, company).await?;
        self.idle(tx.sql(), &c).await?;
        let snapshot = self.snapshot(tx.sql(), scope, &c).await?;
        let bytes = snapshot.to_string().into_bytes();
        let p = self.policy(tx.sql(), scope, company).await?;
        let expiry =
            Timestamp::from_second(Timestamp::now().as_second() + p.retention_days * 86400)
                .map_err(|_| ConnectorError::InvalidBackup)?;
        let id = uuid::Uuid::new_v4().to_string();
        let pending=store::save(&mut tx,None,json!({"id":id,"tenant_id":scope.tenant(),"platform_company_id":company,"connection_id":c.id,"sha256":digest(&bytes),"state":"pending","expires_at":expiry.to_string()})).await?;
        audit(&tx, "people-connector.backup.reserved", company, Some(&id)).await?;
        tx.commit().await?;
        let owner = BackupOwner;
        let documents =
            self.connector
                .artifacts
                .for_owner(scope, OrganisationId::new(company)?, &owner);
        let artifact = match documents
            .put(
                &mut client,
                Reference {
                    subject: id.clone(),
                    kind: "connection-backup".into(),
                },
                &bytes,
                "application/json",
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                self.fail(scope, company, &id).await?;
                return Err(e.into());
            }
        };
        let publication = async {
            let mut tx = self
                .connector
                .begin(&mut client, scope, "people-connector.backup.created")
                .await?;
            let current = self.lock(&tx, scope, company).await?;
            if current.id != c.id {
                return Err(ConnectorError::ConnectionChanged);
            }
            let before = store::fetch(tx.sql(), scope.tenant(), company, &id, true).await?;
            if before["state"] != "pending" || before["connection_id"] != pending["connection_id"] {
                return Err(ConnectorError::ConnectionChanged);
            }
            self.validate(&before, &snapshot, &current)?;
            let mut after = before.clone();
            after["state"] = json!("ready");
            after["artifact_id"] = json!(artifact.id);
            let artifact_expiry = artifact
                .expires_at
                .as_ref()
                .and_then(|v| v.parse::<Timestamp>().ok())
                .ok_or(ConnectorError::InvalidBackup)?;
            after["expires_at"] = json!(expiry.min(artifact_expiry).to_string());
            audit(&tx, "people-connector.backup.created", company, Some(&id)).await?;
            let ready = store::save(&mut tx, Some(before), after).await?;
            tx.commit().await?;
            present(ready)
        }
        .await;
        if publication.is_err() {
            let stored = store::fetch(&**client, scope.tenant(), company, &id, false).await?;
            if stored["artifact_id"] != artifact.id {
                documents.delete(&mut client, &artifact.id).await?;
                self.fail(scope, company, &id).await?;
            }
        }
        publication
    }
    async fn fail(&self, scope: &Scope, company: i64, id: &str) -> Result<()> {
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.failed")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let before = store::fetch(tx.sql(), scope.tenant(), company, id, true).await?;
        if before["state"] == "pending" {
            let mut after = before.clone();
            after["state"] = json!("failed");
            store::save(&mut tx, Some(before), after).await?;
            audit(&tx, "people-connector.backup.failed", company, Some(id)).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn read(&self, scope: &Scope, company: i64, id: &str) -> Result<Value> {
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        let row = store::fetch(&**client, scope.tenant(), company, id, false).await?;
        readable(&row)?;
        let document = self
            .connector
            .artifacts
            .for_owner(scope, OrganisationId::new(company)?, &BackupOwner)
            .read(
                &mut client,
                row["artifact_id"]
                    .as_str()
                    .ok_or(ConnectorError::InvalidBackup)?,
            )
            .await
            .map_err(|e| match e {
                bilibi_artifacts::Error::Integrity => ConnectorError::BackupIntegrity,
                other => other.into(),
            })?;
        if row["sha256"] != digest(&document.bytes) {
            return Err(ConnectorError::BackupIntegrity);
        }
        serde_json::from_slice(&document.bytes).map_err(|_| ConnectorError::InvalidBackup)
    }
    fn validate(&self, row: &Value, saved: &Value, c: &Connection) -> Result<()> {
        let expected = json!({"provider_id":c.provider_id,"provider_contract_version":c.provider_contract_version,"workforce_source_id":c.workforce_source_id,"workforce_company_id":c.workforce_company_id});
        if row["connection_id"] != c.id
            || saved["format"] != "people-connection-backup-v1"
            || saved["tenant_id"] != c.tenant_id
            || saved["platform_company_id"] != c.platform_company_id
            || saved["connection_id"] != c.id
            || CONNECTION_FIELDS[..4]
                .iter()
                .any(|key| saved["connection"][*key] != expected[*key])
        {
            return Err(ConnectorError::ConnectionChanged);
        }
        if saved["connection"]["enabled"].as_bool().is_none()
            || !saved["projections"].is_array()
            || !saved["settings"].is_object()
        {
            return Err(ConnectorError::InvalidBackup);
        }
        let keys = saved["settings"].as_object().unwrap();
        if SETTING_KEYS[..8].iter().any(|k| !keys.contains_key(*k))
            || keys.keys().any(|k| !SETTING_KEYS.contains(&k.as_str()))
        {
            return Err(ConnectorError::InvalidBackup);
        }
        // Validate every record before mutate so a refusal never poisons a transaction.
        let mut identities = BTreeSet::new();
        for raw in saved["projections"].as_array().unwrap() {
            let record: crate::WorkforceRecord =
                serde_json::from_value(raw.clone()).map_err(|_| ConnectorError::InvalidBackup)?;
            if !record.valid()
                || record.source_id != c.workforce_source_id
                || record.workforce_company_id != c.workforce_company_id
                || !identities.insert((record.kind, record.source_id, record.stable_id))
                || raw["content_hash"].as_str().is_none_or(|h| h.len() != 64)
                || (!raw["deactivated_at"].is_null() && at(raw, "deactivated_at").is_none())
            {
                return Err(ConnectorError::InvalidBackup);
            }
        }
        let cp = &saved["checkpoint"];
        if !cp.is_null()
            && (cp["version"].as_i64().is_none_or(|n| n <= 0)
                || at(cp, "as_of_at").is_none()
                || (!cp["resume_cursor"].is_null()
                    && cp["resume_cursor"]
                        .as_str()
                        .is_none_or(|s| s.is_empty() || s.len() > 1000)))
        {
            return Err(ConnectorError::InvalidBackup);
        }
        Ok(())
    }
    pub async fn preview(&self, scope: &Scope, company: i64, id: &str) -> Result<BackupPreview> {
        let saved = self.read(scope, company, id).await?;
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.previewed")
            .await?;
        let c = self.lock(&tx, scope, company).await?;
        self.idle(tx.sql(), &c).await?;
        let before = store::fetch(tx.sql(), scope.tenant(), company, id, true).await?;
        readable(&before)?;
        self.validate(&before, &saved, &c)?;
        if !before["restored_at"].is_null() {
            return Err(ConnectorError::AlreadyRestored);
        }
        let current = self.snapshot(tx.sql(), scope, &c).await?;
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let p = self.policy(tx.sql(), scope, company).await?;
        let mut after = before.clone();
        after["preview_token_hash"] = json!(digest(&token));
        after["preview_state_hash"] = json!(digest(current.to_string()));
        after["preview_actor_id"] = json!(scope.user());
        after["preview_impersonator_id"] = json!(scope.impersonator().map(|i| i.user()));
        after["preview_expires_at"] = json!(Timestamp::from_second(
            Timestamp::now().as_second() + p.preview_minutes * 60
        )
        .map_err(|_| ConnectorError::InvalidBackup)?
        .to_string());
        store::save(&mut tx, Some(before), after).await?;
        audit(&tx, "people-connector.backup.previewed", company, Some(id)).await?;
        tx.commit().await?;
        Ok(BackupPreview {
            id: id.into(),
            token,
            changes: changes(&current, &saved),
        })
    }
    pub async fn restore(
        &self,
        scope: &Scope,
        company: i64,
        id: &str,
        token: &str,
        confirmed: bool,
    ) -> Result<BackupRestore> {
        if !confirmed {
            return Err(ConnectorError::ConfirmationRequired);
        }
        let saved = self.read(scope, company, id).await?;
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.restored")
            .await?;
        let c = self.lock(&tx, scope, company).await?;
        let before = store::fetch(tx.sql(), scope.tenant(), company, id, true).await?;
        readable(&before)?;
        self.validate(&before, &saved, &c)?;
        if before["preview_token_hash"] != digest(token)
            || before["preview_actor_id"] != scope.user()
            || before["preview_impersonator_id"] != json!(scope.impersonator().map(|i| i.user()))
        {
            return Err(ConnectorError::InvalidConfirmation);
        }
        let replayed = !before["restored_at"].is_null();
        let row = if replayed {
            audit(
                &tx,
                "people-connector.backup.restore_replayed",
                company,
                Some(id),
            )
            .await?;
            before
        } else {
            self.idle(tx.sql(), &c).await?;
            if at(&before, "preview_expires_at").is_none_or(|t| t <= Timestamp::now()) {
                return Err(ConnectorError::PreviewExpired);
            }
            let current = self.snapshot(tx.sql(), scope, &c).await?;
            if before["preview_state_hash"] != digest(current.to_string()) {
                return Err(ConnectorError::PreviewChanged);
            }
            let generation = current["checkpoint"]["version"]
                .as_i64()
                .unwrap_or(0)
                .max(saved["checkpoint"]["version"].as_i64().unwrap_or(0))
                .checked_add(1)
                .ok_or(ConnectorError::InvalidBackup)?;
            let target = Target::Company(OrganisationId::new(company)?);
            for (key, value) in saved["settings"].as_object().unwrap() {
                if key != "people-connector.webhook.enabled"
                    && self.connector.settings.definitions().lookup(key).is_ok()
                {
                    self.connector
                        .settings
                        .put(&mut tx, scope, target, key, value.clone())
                        .await?;
                }
            }
            self.connector
                .settings
                .delete(&mut tx, scope, target, CREDENTIAL_KEY)
                .await?;
            if self
                .connector
                .settings
                .definitions()
                .lookup("people-connector.webhook.secret")
                .is_ok()
            {
                self.connector
                    .settings
                    .delete(&mut tx, scope, target, "people-connector.webhook.secret")
                    .await?;
            }
            if self
                .connector
                .settings
                .definitions()
                .lookup("people-connector.webhook.enabled")
                .is_ok()
            {
                self.connector
                    .settings
                    .put(
                        &mut tx,
                        scope,
                        target,
                        "people-connector.webhook.enabled",
                        json!(false),
                    )
                    .await?;
            }
            let enabled = saved["connection"]["enabled"].as_bool().unwrap();
            connections::set_enabled(&mut tx, c.clone(), enabled).await?;
            sync_store::reset(&mut tx, &c).await?;
            let mut cp = saved["checkpoint"].clone();
            let empty = cp.is_null();
            if empty {
                cp = json!({"resume_cursor":null,"as_of_at":Timestamp::now().to_string(),"bootstrap_required":true});
            }
            cp["version"] = json!(generation);
            cp["tenant_id"] = json!(scope.tenant());
            cp["connection_id"] = json!(c.id);
            sync_store::save(&mut tx, sync_store::Table::Checkpoint, None, cp).await?;
            for raw in saved["projections"].as_array().unwrap() {
                let mut values = raw.clone();
                values["tenant_id"] = json!(scope.tenant());
                values["connection_id"] = json!(c.id);
                sync_store::save(&mut tx, sync_store::Table::Record, None, values).await?;
            }
            let mut after = before.clone();
            after["restored_at"] = json!(Timestamp::now().to_string());
            after["recovery_generation"] = json!(generation);
            let row = store::save(&mut tx, Some(before), after).await?;
            audit(&tx, "people-connector.backup.restored", company, Some(id)).await?;
            row
        };
        tx.commit().await?;
        Ok(BackupRestore {
            id: id.into(),
            restored_at: row["restored_at"].as_str().unwrap().into(),
            replayed,
        })
    }
    pub async fn recover(&self, scope: &Scope, company: i64, id: &str) -> Result<SyncRun> {
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        let tx = self
            .connector
            .begin(
                &mut client,
                scope,
                "people-connector.backup.recovery_requested",
            )
            .await?;
        let c = self.lock(&tx, scope, company).await?;
        let row = store::fetch(tx.sql(), scope.tenant(), company, id, false).await?;
        if row["restored_at"].is_null() || row["connection_id"] != c.id {
            return Err(ConnectorError::RestoreRequired);
        }
        let generation = row["recovery_generation"]
            .as_i64()
            .ok_or(ConnectorError::RestoreRequired)?;
        audit(
            &tx,
            "people-connector.backup.recovery_requested",
            company,
            Some(id),
        )
        .await?;
        tx.commit().await?;
        drop(client);
        self.connector
            .synchronise_connection(
                scope,
                company,
                &format!("recovery:{id}"),
                SyncOptions {
                    expected_checkpoint_version: Some(generation),
                    ..Default::default()
                },
                Some(c.id),
            )
            .await
    }
    pub async fn purge_expired(&self, scope: &Scope, company: i64) -> Result<BackupPurge> {
        let client = self.connector.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        let retry = self
            .connector
            .settings
            .get_global(&**client, "artifacts.purge_retry_minutes")
            .await?
            .as_i64()
            .ok_or(ConnectorError::InvalidBackup)?;
        let batch = self
            .connector
            .settings
            .get_global(&**client, "artifacts.purge_batch_size")
            .await?
            .as_i64()
            .ok_or(ConnectorError::InvalidBackup)?;
        let rows=client.query("SELECT id::text FROM people_connector_backups WHERE tenant_id=$1 AND platform_company_id=$2 AND state='ready' AND expires_at<=clock_timestamp() AND purge_held_at IS NULL AND (purge_attempted_at IS NULL OR purge_attempted_at<=clock_timestamp()-($3::bigint*interval '1 minute')) ORDER BY expires_at,id LIMIT $4",&[&scope.tenant(),&company,&retry,&batch]).await.map_err(Error::from)?;
        drop(client);
        let mut report = BackupPurge::default();
        for row in rows {
            let id: String = row.get(0);
            if self.purge(scope, company, &id, false).await.is_ok() {
                report.deleted.push(id);
            } else {
                report.errors.push(id);
            }
        }
        Ok(report)
    }
    pub async fn retry_purge(&self, scope: &Scope, company: i64, id: &str) -> Result<()> {
        self.purge(scope, company, id, true).await
    }
    async fn purge(&self, scope: &Scope, company: i64, id: &str, retry: bool) -> Result<()> {
        let mut client = self.connector.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.purge")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let before = store::fetch(tx.sql(), scope.tenant(), company, id, true).await?;
        if before["state"] != "ready"
            || live(&before)
            || (retry && before["purge_held_at"].is_null())
        {
            return Err(ConnectorError::NotFound);
        }
        if !retry {
            let minutes = self
                .connector
                .settings
                .get_global(tx.sql(), "artifacts.purge_retry_minutes")
                .await?
                .as_i64()
                .ok_or(ConnectorError::InvalidBackup)?;
            if !before["purge_held_at"].is_null()
                || at(&before, "purge_attempted_at")
                    .is_some_and(|t| Timestamp::now().as_second() - t.as_second() < minutes * 60)
            {
                return Err(ConnectorError::BackupCleanupPending);
            }
        }
        let row = if retry {
            let mut after = before.clone();
            after["purge_attempts"] = json!(0);
            for key in ["purge_last_error", "purge_attempted_at", "purge_held_at"] {
                after[key] = Value::Null;
            }
            audit(
                &tx,
                "people-connector.backup.purge_released",
                company,
                Some(id),
            )
            .await?;
            store::save(&mut tx, Some(before), after).await?
        } else {
            before
        };
        // Claim this attempt under the company/receipt locks before doing external IO.
        // Another cleanup pass then skips it for the configured retry interval.
        let before = row.clone();
        let mut claimed = row;
        claimed["purge_attempted_at"] = json!(Timestamp::now().to_string());
        let row = store::save(&mut tx, Some(before), claimed).await?;
        tx.commit().await?;
        let result = self
            .connector
            .artifacts
            .for_owner(scope, OrganisationId::new(company)?, &BackupOwner)
            .delete(
                &mut client,
                row["artifact_id"]
                    .as_str()
                    .ok_or(ConnectorError::InvalidBackup)?,
            )
            .await;
        let mut tx = self
            .connector
            .begin(&mut client, scope, "people-connector.backup.purge_outcome")
            .await?;
        require_writable(&tx, scope, company).await?;
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let before = store::fetch(tx.sql(), scope.tenant(), company, id, true).await?;
        if before["state"] == "purged" {
            tx.commit().await?;
            return Ok(());
        }
        let mut after = before.clone();
        match &result {
            Ok(()) => {
                after["state"] = json!("purged");
                audit(&tx, "people-connector.backup.purged", company, Some(id)).await?;
            }
            Err(error) => {
                let max = self
                    .connector
                    .settings
                    .get_global(tx.sql(), "artifacts.purge_max_attempts")
                    .await?
                    .as_i64()
                    .ok_or(ConnectorError::InvalidBackup)?;
                let attempts = before["purge_attempts"].as_i64().unwrap_or(0) + 1;
                after["purge_attempts"] = json!(attempts);
                after["purge_last_error"] = json!(error.code());
                after["purge_attempted_at"] = json!(Timestamp::now().to_string());
                audit(
                    &tx,
                    "people-connector.backup.purge_failed",
                    company,
                    Some(id),
                )
                .await?;
                if attempts >= max {
                    after["purge_held_at"] = json!(Timestamp::now().to_string());
                    audit(&tx, "people-connector.backup.purge_held", company, Some(id)).await?;
                }
            }
        }
        store::save(&mut tx, Some(before), after).await?;
        tx.commit().await?;
        result.map_err(|_| ConnectorError::BackupCleanupPending)
    }
}

// Stable concrete owner identity: Base reauthenticates and proves Company under its own lock.
struct BackupOwner;
impl Owner for BackupOwner {
    fn owner_id(&self) -> &'static str {
        crate::MODULE
    }
    fn authorize<'a>(
        &'a self,
        proof: &'a dyn ProofClient,
        scope: &'a Scope,
        company: OrganisationId,
        operation: Operation,
        reference: Option<&'a Reference>,
    ) -> ProofFuture<'a, ()> {
        Box::pin(async move {
            let target = bilibi_company::organisation::Directory
                .target(proof, scope, company, crate::MANAGE)
                .await?;
            if target.archived {
                return Err(Error::Forbidden);
            }
            if operation == Operation::Purge && reference.is_none() {
                return Ok(());
            }
            let reference = reference
                .filter(|r| {
                    r.kind == "connection-backup" && uuid::Uuid::parse_str(&r.subject).is_ok()
                })
                .ok_or(Error::Forbidden)?;
            let row=proof.query_opt("SELECT to_jsonb(b) FROM people_connector_backups b WHERE tenant_id=$1 AND platform_company_id=$2 AND id=$3::text::uuid",&[&scope.tenant(),&company.get(),&reference.subject]).await?.ok_or(Error::Forbidden)?;
            let receipt: Value = row.get(0);
            if operation == Operation::Delete {
                return Ok(());
            }
            let expected = match operation {
                Operation::Create => "pending",
                Operation::Read => "ready",
                _ => return Err(Error::Forbidden),
            };
            if receipt["state"] != expected || !live(&receipt) {
                return Err(Error::NotFound);
            }
            Ok(())
        })
    }
}
fn changes(current: &Value, saved: &Value) -> Vec<BackupChange> {
    let mut rows = Vec::new();
    let mut push = |label: &str, before: Value, after: Value, instant| {
        rows.push(BackupChange {
            label: label.into(),
            before,
            after,
            instant,
        })
    };
    push(
        "Connection enabled",
        current["connection"]["enabled"].clone(),
        saved["connection"]["enabled"].clone(),
        false,
    );
    let generation = current["checkpoint"]["version"]
        .as_i64()
        .unwrap_or(0)
        .max(saved["checkpoint"]["version"].as_i64().unwrap_or(0))
        + 1;
    push(
        "Checkpoint generation",
        current["checkpoint"]["version"].clone(),
        json!(generation),
        false,
    );
    push(
        "Checkpoint time",
        current["checkpoint"]["as_of_at"].clone(),
        saved["checkpoint"]["as_of_at"].clone(),
        true,
    );
    push(
        "Directory records",
        json!(current["projections"].as_array().map_or(0, Vec::len)),
        json!(saved["projections"].as_array().map_or(0, Vec::len)),
        false,
    );
    for (key, label) in SETTING_KEYS.into_iter().zip([
        "Sync page size",
        "Sync maximum age (minutes)",
        "Sync timeout (minutes)",
        "File exchange",
        "JSON file exchange",
        "File size limit (bytes)",
        "File record limit",
        "File pending timeout (minutes)",
        "Webhook signing window (seconds)",
        "Saved webhook intake (restore disables it)",
    ]) {
        if current["settings"].get(key).is_some() || saved["settings"].get(key).is_some() {
            push(
                label,
                current["settings"][key].clone(),
                saved["settings"][key].clone(),
                false,
            );
        }
    }
    push(
        "Credentials and webhook secret",
        json!("Excluded"),
        json!("Cleared; re-enter on People connections"),
        false,
    );
    push(
        "Webhook intake",
        current["settings"]["people-connector.webhook.enabled"].clone(),
        json!(false),
        false,
    );
    // Include each changed directory fact without disclosing it to action payloads.
    let identity = |r: &Value| {
        format!(
            "{}\0{}\0{}",
            r["kind"].as_str().unwrap_or(""),
            r["source_id"].as_str().unwrap_or(""),
            r["stable_id"].as_str().unwrap_or("")
        )
    };
    let index = |v: &Value| {
        v["projections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (identity(r), r.clone()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let a = index(current);
    let b = index(saved);
    for key in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
        let before = a.get(key).unwrap_or(&Value::Null);
        let after = b.get(key).unwrap_or(&Value::Null);
        let record = if after.is_null() { before } else { after };
        for (field, title, instant) in [
            ("name", "Name", false),
            ("code", "Code", false),
            ("email", "Email", false),
            ("supervisor_stable_id", "Supervisor reference", false),
            ("parent_stable_id", "Parent reference", false),
            ("version", "Version", false),
            ("vacant", "Vacant", false),
            ("assignments_incomplete", "Assignments incomplete", false),
            ("assignments", "Assignments", false),
            ("active", "Active", false),
            ("observed_at", "Observed", true),
            ("deactivated_at", "Deactivated", true),
        ] {
            if before[field] != after[field] {
                push(
                    &format!(
                        "{} {} · {title}",
                        record["kind"].as_str().unwrap_or(""),
                        record["stable_id"].as_str().unwrap_or("")
                    ),
                    before[field].clone(),
                    after[field].clone(),
                    instant,
                );
            }
        }
    }
    rows
}
