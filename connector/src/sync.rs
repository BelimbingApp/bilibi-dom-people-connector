//! Bilimbi Connector.Sync: reserve a pass, read outside a transaction, then apply atomically.
use crate::{
    sync_store::{self as store, Table},
    *,
};
use bilibi_database::MutationTransaction;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::GenericClient;

/// Policy field name, persisted setting key, inclusive minimum and maximum.
pub const POLICY_FIELDS: [(&str, &str, i64, i64); 3] = [
    ("page_limit", "people-connector.sync.page_limit", 1, 1000),
    (
        "max_age_minutes",
        "people-connector.sync.max_age_minutes",
        5,
        43200,
    ),
    (
        "run_timeout_minutes",
        "people-connector.sync.run_timeout_minutes",
        1,
        1440,
    ),
];
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Checkpoint {
    id: i64,
    tenant_id: i64,
    connection_id: i64,
    version: i64,
    resume_cursor: Option<String>,
    #[serde(with = "sync_types::instant")]
    as_of_at: Timestamp,
}
async fn checkpoint(client: &impl GenericClient, c: &Connection) -> Result<Option<Checkpoint>> {
    store::rows(client, Table::Checkpoint, c)
        .await?
        .into_iter()
        .next()
        .map(store::decode)
        .transpose()
}
fn version(c: &Option<Checkpoint>) -> i64 {
    c.as_ref().map_or(0, |c| c.version)
}
fn key_valid(key: &str) -> bool {
    (1..=100).contains(&key.len())
        && key.as_bytes()[0].is_ascii_alphanumeric()
        && key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
// Bilimbi stores observations as UTC microseconds. Truncate before comparing,
// so PostgreSQL rounding cannot change which observation wins.
fn usec(at: Timestamp) -> Timestamp {
    Timestamp::from_microsecond(at.as_microsecond()).expect("microseconds of a valid timestamp")
}
fn expired(run: &SyncRun, policy: SyncPolicy) -> bool {
    run.state == RunState::Running
        && Timestamp::now().as_second() - run.started_at.as_second()
            > policy.run_timeout_minutes * 60
}
fn fresh(c: Option<&Connection>, checkpoint: &Option<Checkpoint>, policy: SyncPolicy) -> Freshness {
    if !c.is_some_and(|c| c.enabled) {
        return Freshness::Unavailable("disconnected".into());
    }
    match checkpoint {
        None => Freshness::Unavailable("never_synchronised".into()),
        Some(c)
            if Timestamp::now().as_second() - c.as_of_at.as_second()
                > policy.max_age_minutes * 60 =>
        {
            Freshness::Stale(c.as_of_at)
        }
        Some(_) => Freshness::Current,
    }
}
impl Connector {
    async fn sync_policy(
        &self,
        client: &(impl GenericClient + Sync),
        scope: &Scope,
        company: i64,
    ) -> Result<SyncPolicy> {
        let keys = POLICY_FIELDS.map(|(_, key, _, _)| key);
        let values = self
            .settings
            .resolve_many(
                client,
                scope,
                Target::Company(OrganisationId::new(company)?),
                &keys,
            )
            .await?;
        let get = |key: &str| {
            values
                .get(key)
                .and_then(|v| v.value.as_i64())
                .ok_or(ConnectorError::Failed(Error::Internal))
        };
        Ok(SyncPolicy {
            page_limit: get(keys[0])?,
            max_age_minutes: get(keys[1])?,
            run_timeout_minutes: get(keys[2])?,
        })
    }
    /// Runs a complete pass. Replay returns the recorded run without another adapter read.
    pub async fn synchronise(
        &self,
        scope: &Scope,
        platform_company_id: i64,
        idempotency_key: &str,
        options: SyncOptions,
    ) -> Result<SyncRun> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company = authorize(&**client, scope, platform_company_id).await?;
        if !key_valid(idempotency_key) {
            return Err(ConnectorError::InvalidIdempotencyKey);
        }
        let mut tx = self
            .begin(&mut client, scope, "people-connector.synchronise")
            .await?;
        require_writable(&tx, scope, company).await?;
        let current =
            Status::from_workforce(people::company(&self.ports, &tx, scope, company).await?)?;
        let (c, provider) = self.connection_provider(tx.sql(), scope, company).await?;
        require_mapping(&c, &current)?;
        if !c.enabled {
            return Err(ConnectorError::Disconnected);
        }
        self.registry.permit(
            &c.provider_id,
            contracts::EMPLOYEE_DIRECTORY,
            Direction::Read,
        )?;
        let adapter = self
            .adapters
            .fetch(&c.provider_id)
            .ok_or(ConnectorError::AdapterUnavailable)?;
        let policy = self.sync_policy(tx.sql(), scope, company).await?;
        // A timed-out run is persisted as unknown before replay or a new reservation.
        for row in tx.sql().query("SELECT to_jsonb(r) FROM people_connector_sync_runs r WHERE tenant_id=$1 AND connection_id=$2 AND state='running'", &[&c.tenant_id,&c.id]).await.map_err(Error::from)? {
            let raw:Value=row.get(0);
            let old: SyncRun = store::decode(raw.clone())?;
            if expired(&old, policy) {
                close(
                    &mut tx,
                    raw,
                    RunState::Unknown,
                    Some("no_outcome_recorded"),
                    None,
                    None,
                )
                .await?;
                issue(
                    &mut tx,
                    &c,
                    &format!("run:{}", old.idempotency_key),
                    "unknown_outcome",
                    "no_outcome_recorded",
                    "warning",
                    None,
                )
                .await?;
            }
        }
        if let Some(row) = tx.sql().query_opt("SELECT to_jsonb(r) FROM people_connector_sync_runs r WHERE tenant_id=$1 AND connection_id=$2 AND idempotency_key=$3", &[&c.tenant_id,&c.id,&idempotency_key]).await.map_err(Error::from)? {
            let run=store::decode(row.get(0))?;
            tx.commit().await?;
            return Ok(run);
        }
        if tx.sql().query_opt("SELECT 1 FROM people_connector_sync_runs WHERE tenant_id=$1 AND connection_id=$2 AND state='running'", &[&c.tenant_id,&c.id]).await.map_err(Error::from)?.is_some() {
            return Err(ConnectorError::SyncInProgress);
        }
        let cp = checkpoint(tx.sql(), &c).await?;
        if options
            .expected_checkpoint_version
            .is_some_and(|v| v != version(&cp))
        {
            return Err(ConnectorError::CheckpointMoved);
        }
        let pass = if options.full || cp.is_none() {
            RunPass::Bootstrap
        } else {
            RunPass::Incremental
        };
        let raw=store::save(&mut tx,Table::Run,None,json!({
            "tenant_id":c.tenant_id,"connection_id":c.id,"platform_company_id":company,"provider_id":c.provider_id,
            "idempotency_key":idempotency_key,"pass":pass,"state":"running","reason":null,"checkpoint_version":null,
            "as_of_at":null,"applied":0,"unchanged":0,"superseded":0,"deactivated":0,"refused":0,
            "started_at":Timestamp::now().to_string(),"finished_at":null,
        })).await?;
        let stamp = connection_stamp(tx.sql(), &c).await?;
        let run: SyncRun = store::decode(raw)?;
        tx.commit().await?;
        let authorization = PortAuthorization {
            scope,
            platform_company_id: company,
            workforce_source_id: &c.workforce_source_id,
            workforce_company_id: c.workforce_company_id,
            provider_id: &c.provider_id,
            capability: contracts::EMPLOYEE_DIRECTORY,
            direction: Direction::Read,
        };
        let context = PortContext {
            client: (&client).into(),
            ports: &self.ports,
        };
        let outcome = match tokio::time::timeout(
            std::time::Duration::from_secs((policy.run_timeout_minutes * 60) as u64),
            read_streams(
                adapter,
                context,
                &authorization,
                provider,
                pass,
                &cp,
                policy.page_limit as usize,
            ),
        )
        .await
        {
            Ok(v) => v,
            Err(_) => Outcome::Unknown,
        };
        // Release the read connection before acquiring the finish transaction;
        // simultaneous passes across companies must not exhaust the pool.
        drop(client);
        self.finish_sync(scope, c, stamp, run, cp, provider, outcome)
            .await
    }
    /// Inspects gates only. Adapters are never returned to callers, including declared reads.
    pub async fn request_port(
        &self,
        scope: &Scope,
        company: i64,
        provider: &str,
        capability: &str,
        direction: Direction,
    ) -> Result<()> {
        let status = self.status(scope, company).await?;
        self.registry.permit(provider, capability, direction)?;
        if status.state == ConnectionState::Enabled
            && status.provider_id.as_deref() == Some(provider)
        {
            Err(ConnectorError::AdapterUnavailable)
        } else {
            Err(ConnectorError::Disconnected)
        }
    }
    /// Current, stale or unavailable directory projection, under Workforce's read policy.
    pub async fn workforce(
        &self,
        scope: &Scope,
        company: i64,
    ) -> Result<ReadResult<Vec<WorkforceRecord>>> {
        let client = self.db.get().await.map_err(Error::from)?;
        let status = self.status_on(&client, scope, company).await?;
        let c =
            connections::find(&**client, scope.tenant(), status.platform_company_id, false).await?;
        let cp = match &c {
            Some(c) => checkpoint(&**client, c).await?,
            None => None,
        };
        let policy = self
            .sync_policy(&**client, scope, status.platform_company_id)
            .await?;
        let freshness = fresh(c.as_ref(), &cp, policy);
        let value = if matches!(freshness, Freshness::Unavailable(_)) {
            None
        } else {
            let mut records: Vec<WorkforceRecord> = Vec::new();
            if let Some(c) = &c {
                for raw in store::rows(&**client, Table::Record, c).await? {
                    if raw["active"] == true {
                        records.push(store::decode(raw)?);
                    }
                }
            }
            records.sort_by(|a, b| {
                (a.kind.as_str(), &a.name, &a.stable_id).cmp(&(
                    b.kind.as_str(),
                    &b.name,
                    &b.stable_id,
                ))
            });
            Some(records)
        };
        Ok(Read { value, freshness })
    }
    pub async fn sync_summary(&self, scope: &Scope, company: i64) -> Result<SyncSummary> {
        let client = self.db.get().await.map_err(Error::from)?;
        let status = self.status_on(&client, scope, company).await?;
        let policy = self
            .sync_policy(&**client, scope, status.platform_company_id)
            .await?;
        let c =
            connections::find(&**client, scope.tenant(), status.platform_company_id, false).await?;
        let mut cp = None;
        let mut last_run = None;
        let mut open_issues = Vec::new();
        if let Some(c) = &c {
            cp = checkpoint(&**client, c).await?;
            if let Some(row)=client.query_opt("SELECT to_jsonb(r) FROM people_connector_sync_runs r WHERE tenant_id=$1 AND connection_id=$2 ORDER BY started_at DESC,id DESC LIMIT 1",&[&c.tenant_id,&c.id]).await.map_err(Error::from)? {
                let mut run:SyncRun=store::decode(row.get(0))?;
                if expired(&run,policy) {run.state=RunState::Unknown;run.reason=Some("no_outcome_recorded".into());}
                last_run=Some(run);
            }
            for row in client.query("SELECT to_jsonb(i) FROM people_connector_reconciliation_issues i WHERE tenant_id=$1 AND connection_id=$2 AND status='open' ORDER BY severity,last_seen_at DESC,id DESC LIMIT 50",&[&c.tenant_id,&c.id]).await.map_err(Error::from)? {
                open_issues.push(store::decode(row.get(0))?);
            }
        }
        Ok(SyncSummary {
            freshness: fresh(c.as_ref(), &cp, policy),
            checkpoint_version: cp.as_ref().map(|c| c.version),
            as_of_at: cp.map(|c| c.as_of_at),
            last_run,
            open_issues,
            policy,
        })
    }
    pub async fn resolve_issue(&self, scope: &Scope, company: i64, issue_id: i64) -> Result<()> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company = authorize(&**client, scope, company).await?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.resolve_issue")
            .await?;
        require_writable(&tx, scope, company).await?;
        let current =
            Status::from_workforce(people::company(&self.ports, &tx, scope, company).await?)?;
        let (c, _) = self.connection_provider(tx.sql(), scope, company).await?;
        require_mapping(&c, &current)?;
        let raw = tx.sql().query_opt(
            "SELECT to_jsonb(i) FROM people_connector_reconciliation_issues i WHERE tenant_id=$1 AND connection_id=$2 AND id=$3 AND status='open'",
            &[&c.tenant_id,&c.id,&issue_id],
        ).await.map_err(Error::from)?.map(|row|row.get(0)).ok_or(ConnectorError::NotFound)?;
        resolve(&mut tx, raw).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn put_sync_policy(
        &self,
        scope: &Scope,
        company: i64,
        update: SyncPolicyUpdate,
    ) -> Result<SyncPolicy> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company = authorize(&**client, scope, company).await?;
        let values = [
            update.page_limit,
            update.max_age_minutes,
            update.run_timeout_minutes,
        ];
        for ((field, _, min, max), value) in POLICY_FIELDS.into_iter().zip(values) {
            if value.is_some_and(|v| v < min || v > max) {
                return Err(ConnectorError::InvalidPolicy(field));
            }
        }
        let mut tx = self
            .begin(&mut client, scope, "people-connector.put_sync_policy")
            .await?;
        require_writable(&tx, scope, company).await?;
        Status::from_workforce(people::company(&self.ports, &tx, scope, company).await?)?;
        // A policy also exists before connection setup. Lock it when present.
        connections::find(tx.sql(), scope.tenant(), company, true).await?;
        for ((_, key, _, _), value) in POLICY_FIELDS.into_iter().zip(values) {
            if let Some(v) = value {
                self.settings
                    .put(
                        &mut tx,
                        scope,
                        Target::Company(OrganisationId::new(company)?),
                        key,
                        json!(v),
                    )
                    .await?;
            }
        }
        let policy = self.sync_policy(tx.sql(), scope, company).await?;
        tx.commit().await?;
        Ok(policy)
    }
    #[allow(clippy::too_many_arguments)]
    async fn finish_sync(
        &self,
        scope: &Scope,
        original: Connection,
        stamp: String,
        run: SyncRun,
        cp: Option<Checkpoint>,
        provider: &Provider,
        outcome: Outcome,
    ) -> Result<SyncRun> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let mut tx = self
            .begin(&mut client, scope, "people-connector.synchronise")
            .await?;
        require_writable(&tx, scope, original.platform_company_id).await?;
        let c = connections::find(tx.sql(), scope.tenant(), original.platform_company_id, true)
            .await?
            .ok_or(ConnectorError::Conflict)?;
        let raw:Value = tx.sql().query_opt(
            "SELECT to_jsonb(r) FROM people_connector_sync_runs r WHERE tenant_id=$1 AND connection_id=$2 AND id=$3 FOR UPDATE",
            &[&c.tenant_id,&c.id,&run.id],
        ).await.map_err(Error::from)?.map(|row|row.get(0)).ok_or(ConnectorError::Conflict)?;
        if raw["state"] != "running" {
            let result = store::decode(raw)?;
            tx.commit().await?;
            return Ok(result);
        }
        let policy = self
            .sync_policy(tx.sql(), scope, original.platform_company_id)
            .await?;
        let result = if matches!(outcome, Outcome::Unknown) || expired(&run, policy) {
            issue(
                &mut tx,
                &c,
                &format!("run:{}", run.idempotency_key),
                "unknown_outcome",
                "no_outcome_recorded",
                "warning",
                None,
            )
            .await?;
            close(
                &mut tx,
                raw,
                RunState::Unknown,
                Some("no_outcome_recorded"),
                None,
                None,
            )
            .await?
        } else {
            match outcome {
                Outcome::Stale(at) => {
                    close(
                        &mut tx,
                        raw,
                        RunState::Stale,
                        Some("provider_stale"),
                        Some(at),
                        None,
                    )
                    .await?
                }
                Outcome::Unavailable => {
                    close(
                        &mut tx,
                        raw,
                        RunState::Unavailable,
                        Some("provider_unavailable"),
                        None,
                        None,
                    )
                    .await?
                }
                Outcome::Failed(reason) => {
                    close(&mut tx, raw, RunState::Failed, Some(reason), None, None).await?
                }
                Outcome::Complete(feed) => {
                    let current = Status::from_workforce(
                        people::company(&self.ports, &tx, scope, c.platform_company_id).await?,
                    )?;
                    if c != original
                        || connection_stamp(tx.sql(), &c).await? != stamp
                        || require_mapping(&c, &current).is_err()
                    {
                        close(
                            &mut tx,
                            raw,
                            RunState::Refused,
                            Some("connection_changed"),
                            Some(feed.as_of),
                            None,
                        )
                        .await?
                    } else {
                        let present = checkpoint(tx.sql(), &c).await?;
                        if present.as_ref().map(|v| (v.id, v.version))
                            != cp.as_ref().map(|v| (v.id, v.version))
                        {
                            close(
                                &mut tx,
                                raw,
                                RunState::Failed,
                                Some("checkpoint_moved"),
                                Some(feed.as_of),
                                None,
                            )
                            .await?
                        } else {
                            apply(&mut tx, &c, raw, provider, feed, present).await?
                        }
                    }
                }
                Outcome::Unknown => unreachable!(),
            }
        };
        tx.commit().await?;
        Ok(result)
    }
}
// Migration 001's updated_at has whole-second precision. PostgreSQL's row
// version also catches disable/re-enable or remap cycles within that second.
async fn connection_stamp(client: &impl GenericClient, c: &Connection) -> Result<String> {
    Ok(client
        .query_one(
            "SELECT xmin::text FROM people_connector_connections WHERE tenant_id=$1 AND id=$2",
            &[&c.tenant_id, &c.id],
        )
        .await
        .map_err(Error::from)?
        .get(0))
}

struct Stream {
    entries: Vec<Value>,
    as_of: Timestamp,
    resume: Option<String>,
    snapshot: bool,
    pages: usize,
}
struct Feed {
    entries: Vec<Value>,
    as_of: Timestamp,
    resume: Option<String>,
    full: BTreeSet<RecordKind>,
    organisation: Option<bool>,
}
enum Outcome {
    Complete(Feed),
    Stale(Timestamp),
    Unavailable,
    Failed(&'static str),
    Unknown,
}
fn cursor_valid(cursor: &Option<String>) -> bool {
    cursor
        .as_ref()
        .is_none_or(|s| !s.is_empty() && s.len() <= 1000 && !s.contains('\0'))
}
async fn read_pages(
    adapter: &dyn ReadPort,
    context: PortContext<'_>,
    authorization: &PortAuthorization<'_>,
    pass: RunPass,
    cp: &Option<Checkpoint>,
    limit: usize,
) -> std::result::Result<Stream, Outcome> {
    let mut request = PortRequest {
        pass: if pass == RunPass::Bootstrap {
            Pass::Bootstrap
        } else {
            Pass::Changes
        },
        since: if pass == RunPass::Incremental {
            cp.as_ref().and_then(|c| c.resume_cursor.clone())
        } else {
            None
        },
        cursor: None,
        limit,
    };
    let mut entries = Vec::new();
    let mut as_of = None;
    let mut snapshot = true;
    let mut seen = BTreeSet::new();
    let mut pages = 0;
    loop {
        // Poll under catch_unwind as well as constructing the future: a provider panic
        // becomes a fixed adapter_error outcome, never provider text in persistence.
        let mut future = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            adapter.read(context, authorization, &request)
        }))
        .map_err(|_| Outcome::Failed("adapter_error"))?;
        let page = std::future::poll_fn(|cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future.as_mut().poll(cx)
            })) {
                Ok(std::task::Poll::Ready(v)) => {
                    std::task::Poll::Ready(v.map_err(|_| Outcome::Failed("adapter_error")))
                }
                Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                Err(_) => std::task::Poll::Ready(Err(Outcome::Failed("adapter_error"))),
            }
        })
        .await?;
        drop(future);
        if page.entries.len() > limit
            || !cursor_valid(&page.next_cursor)
            || !cursor_valid(&page.resume_cursor)
            || (pass == RunPass::Bootstrap
                && page
                    .entries
                    .iter()
                    .any(|e| e.get("deactivated") == Some(&Value::Bool(true))))
        {
            return Err(Outcome::Failed("invalid_page"));
        }
        match page.freshness {
            Freshness::Current => {}
            Freshness::Stale(at) => return Err(Outcome::Stale(usec(at))),
            Freshness::Unavailable(_) => return Err(Outcome::Unavailable),
        }
        as_of = Some(as_of.map_or(usec(page.as_of), |a: Timestamp| a.min(usec(page.as_of))));
        snapshot &= page.snapshot;
        entries.extend(page.entries);
        pages += 1;
        seen.insert(request.cursor.clone());
        match page.next_cursor {
            None => {
                return Ok(Stream {
                    entries,
                    as_of: as_of.expect("one page"),
                    resume: page.resume_cursor,
                    snapshot,
                    pages,
                })
            }
            Some(cursor) => {
                if seen.contains(&Some(cursor.clone())) {
                    return Err(Outcome::Failed("cursor_repeated"));
                }
                request.cursor = Some(cursor);
            }
        }
    }
}
async fn read_streams(
    adapter: &dyn ReadPort,
    context: PortContext<'_>,
    authorization: &PortAuthorization<'_>,
    provider: &Provider,
    pass: RunPass,
    cp: &Option<Checkpoint>,
    limit: usize,
) -> Outcome {
    let directory = match read_pages(adapter, context, authorization, pass, cp, limit).await {
        Ok(v) => v,
        Err(v) => return v,
    };
    let mut feed = Feed {
        entries: directory.entries,
        as_of: directory.as_of,
        resume: directory.resume,
        full: BTreeSet::new(),
        organisation: None,
    };
    if pass == RunPass::Bootstrap || directory.snapshot {
        feed.full
            .extend([RecordKind::Company, RecordKind::Employee]);
    }
    if provider.declares(contracts::ORGANIZATION_DIRECTORY, Direction::Read) {
        let authorization = PortAuthorization {
            capability: contracts::ORGANIZATION_DIRECTORY,
            ..*authorization
        };
        match read_pages(adapter, context, &authorization, pass, cp, limit).await {
            Ok(positions) => {
                feed.entries.extend(positions.entries);
                feed.as_of = feed.as_of.min(positions.as_of);
                feed.organisation = Some(true);
                if (pass == RunPass::Bootstrap || positions.snapshot) && positions.pages == 1 {
                    feed.full.insert(RecordKind::Position);
                }
            }
            Err(Outcome::Unavailable) => feed.organisation = Some(false),
            Err(stopped) => return stopped,
        }
    }
    Outcome::Complete(feed)
}

async fn close(
    tx: &mut MutationTransaction<'_>,
    raw: Value,
    state: RunState,
    reason: Option<&str>,
    as_of: Option<Timestamp>,
    checkpoint_version: Option<i64>,
) -> Result<SyncRun> {
    let mut after = raw.clone();
    after["state"] = store::encode(&state)?;
    after["reason"] = json!(reason);
    after["finished_at"] = json!(Timestamp::now().to_string());
    if let Some(at) = as_of {
        after["as_of_at"] = json!(at.to_string());
    }
    if let Some(version) = checkpoint_version {
        after["checkpoint_version"] = json!(version);
    }
    store::decode(store::save(tx, Table::Run, Some(raw), after).await?)
}
fn digest(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
type Identity = (RecordKind, String, String);
fn identity(raw: &Value) -> Option<Identity> {
    let kind = serde_json::from_value(raw.get("kind")?.clone()).ok()?;
    let source = raw.get("source_id")?.as_str()?;
    let stable = raw.get("stable_id")?.as_str()?;
    if !sync_types::text(source, 100) || !sync_types::text(stable, 100) {
        return None;
    }
    Some((kind, source.into(), stable.into()))
}
fn issue_key(identity: Option<&Identity>) -> String {
    match identity {
        None => "record:invalid".into(),
        Some((kind, source, stable)) => format!(
            "record:{}:{}",
            kind.as_str(),
            digest(format!("{source}\0{stable}"))
        ),
    }
}
#[allow(clippy::too_many_arguments)]
async fn issue(
    tx: &mut MutationTransaction<'_>,
    c: &Connection,
    key: &str,
    kind: &str,
    reason: &str,
    severity: &str,
    identity: Option<&Identity>,
) -> Result<()> {
    let before = issue_row(tx.sql(), c, key).await?;
    let now = Timestamp::now().to_string();
    let values = json!({"tenant_id":c.tenant_id,"connection_id":c.id,"issue_key":key,"kind":kind,"reason":reason,"severity":severity,
        "record_kind":identity.map(|i|i.0.as_str()),"stable_id":identity.map(|i|&i.2),"status":"open",
        "occurrences":before.as_ref().and_then(|v|v["occurrences"].as_i64()).unwrap_or(0)+1,
        "first_seen_at":before.as_ref().map(|v|v["first_seen_at"].clone()).unwrap_or_else(||json!(now)),"last_seen_at":now,"resolved_at":null});
    store::save(tx, Table::Issue, before, values).await?;
    Ok(())
}
async fn resolve(tx: &mut MutationTransaction<'_>, raw: Value) -> Result<()> {
    let mut after = raw.clone();
    after["status"] = json!("resolved");
    after["resolved_at"] = json!(Timestamp::now().to_string());
    store::save(tx, Table::Issue, Some(raw), after).await?;
    Ok(())
}
async fn issue_row(
    client: &impl GenericClient,
    c: &Connection,
    key: &str,
) -> Result<Option<Value>> {
    Ok(client.query_opt("SELECT to_jsonb(i) FROM people_connector_reconciliation_issues i WHERE tenant_id=$1 AND connection_id=$2 AND issue_key=$3",&[&c.tenant_id,&c.id,&key]).await.map_err(Error::from)?.map(|row|row.get(0)))
}
async fn resolve_key(tx: &mut MutationTransaction<'_>, c: &Connection, key: &str) -> Result<()> {
    if let Some(raw) = issue_row(tx.sql(), c, key)
        .await?
        .filter(|v| v["status"] == "open")
    {
        resolve(tx, raw).await?;
    }
    Ok(())
}

enum Entry {
    Record(Box<WorkforceRecord>),
    Deactivation(Deactivation),
}
fn classify(raw: Value, c: &Connection, p: &Provider) -> std::result::Result<Entry, &'static str> {
    let id = identity(&raw).ok_or("invalid_record")?;
    let (entry, company) = if raw.get("deactivated") == Some(&json!(true)) {
        let mut change: Deactivation = serde_json::from_value(raw).map_err(|_| "invalid_record")?;
        if !change.valid() {
            return Err("invalid_record");
        }
        change.observed_at = usec(change.observed_at);
        (Entry::Deactivation(change), c.workforce_company_id)
    } else {
        let mut record: WorkforceRecord =
            serde_json::from_value(raw).map_err(|_| "invalid_record")?;
        if !record.valid() {
            return Err("invalid_record");
        }
        record.observed_at = usec(record.observed_at);
        let company = record.workforce_company_id;
        (Entry::Record(Box::new(record)), company)
    };
    if id.1 != c.workforce_source_id {
        return Err("foreign_source");
    }
    if !p.declares(id.0.capability(), Direction::Read) {
        return Err("undeclared_capability");
    }
    if company != c.workforce_company_id {
        return Err("other_company");
    }
    Ok(entry)
}
fn content_hash(record: &WorkforceRecord) -> Result<String> {
    let mut record = record.clone();
    record
        .assignments
        .sort_by(|a, b| a.stable_id.cmp(&b.stable_id));
    let mut value = store::encode(&record)?;
    value
        .as_object_mut()
        .ok_or(ConnectorError::Failed(Error::Internal))?
        .remove("observed_at");
    Ok(digest(value.to_string()))
}
async fn switch_off(
    tx: &mut MutationTransaction<'_>,
    raw: Value,
    observed: Timestamp,
    deactivated: Timestamp,
) -> Result<Value> {
    let mut record: WorkforceRecord = store::decode(raw.clone())?;
    record.active = false;
    record.observed_at = observed;
    let mut after = raw.clone();
    after["active"] = json!(false);
    after["observed_at"] = json!(observed.to_string());
    after["deactivated_at"] = json!(deactivated.to_string());
    after["content_hash"] = json!(content_hash(&record)?);
    store::save(tx, Table::Record, Some(raw), after).await
}
async fn apply(
    tx: &mut MutationTransaction<'_>,
    c: &Connection,
    mut run: Value,
    provider: &Provider,
    feed: Feed,
    cp: Option<Checkpoint>,
) -> Result<SyncRun> {
    let before_run = run.clone();
    let mut projections = BTreeMap::new();
    for raw in store::rows(tx.sql(), Table::Record, c).await? {
        let id = identity(&raw).ok_or(ConnectorError::Failed(Error::Internal))?;
        projections.insert(id, raw);
    }
    let had_positions = projections
        .iter()
        .any(|(id, v)| id.0 == RecordKind::Position && v["active"] == true);
    let mut seen = BTreeSet::new();
    let mut counts = BTreeMap::from([
        ("applied", 0i32),
        ("unchanged", 0),
        ("superseded", 0),
        ("deactivated", 0),
        ("refused", 0),
    ]);
    if !feed.full.is_empty() && feed.entries.is_empty() {
        issue(
            tx,
            c,
            "bootstrap:empty",
            "empty_bootstrap",
            "no_records",
            "warning",
            None,
        )
        .await?;
    }
    for raw in feed.entries {
        let id = identity(&raw);
        match classify(raw, c, provider) {
            Err(reason) => {
                issue(
                    tx,
                    c,
                    &issue_key(id.as_ref()),
                    "record_refused",
                    reason,
                    "error",
                    id.as_ref(),
                )
                .await?;
                if let Some(id) = id {
                    seen.insert(id);
                }
                *counts.get_mut("refused").expect("counter") += 1;
            }
            Ok(Entry::Record(record)) => {
                let id = (
                    record.kind,
                    record.source_id.clone(),
                    record.stable_id.clone(),
                );
                seen.insert(id.clone());
                let before = projections.get(&id).cloned();
                let hash = content_hash(&record)?;
                let counter = if let Some(current) = &before {
                    let old: WorkforceRecord = store::decode(current.clone())?;
                    if record.observed_at < old.observed_at {
                        "superseded"
                    } else if current["content_hash"] == hash {
                        "unchanged"
                    } else if old.active && !record.active {
                        "deactivated"
                    } else {
                        "applied"
                    }
                } else {
                    "applied"
                };
                if matches!(counter, "applied" | "deactivated") {
                    let mut after = store::encode(&record)?;
                    after["tenant_id"] = json!(c.tenant_id);
                    after["connection_id"] = json!(c.id);
                    after["content_hash"] = json!(hash);
                    after["deactivated_at"] = if record.active {
                        Value::Null
                    } else {
                        json!(record.observed_at.to_string())
                    };
                    let after = store::save(tx, Table::Record, before, after).await?;
                    projections.insert(id.clone(), after);
                }
                if counter != "superseded" {
                    resolve_key(tx, c, &issue_key(Some(&id))).await?;
                }
                *counts.get_mut(counter).expect("counter") += 1;
            }
            Ok(Entry::Deactivation(change)) => {
                let id = (change.kind, change.source_id, change.stable_id);
                // A superseded tombstone is present too: snapshot absence must
                // not turn it into a newer deactivation.
                seen.insert(id.clone());
                let counter = if let Some(raw) = projections.get(&id).cloned() {
                    let current: WorkforceRecord = store::decode(raw.clone())?;
                    if change.observed_at < current.observed_at {
                        "superseded"
                    } else if !current.active {
                        "unchanged"
                    } else {
                        let after =
                            switch_off(tx, raw, change.observed_at, change.observed_at).await?;
                        projections.insert(id, after);
                        "deactivated"
                    }
                } else {
                    issue(
                        tx,
                        c,
                        &issue_key(Some(&id)),
                        "record_refused",
                        "unknown_reference",
                        "error",
                        Some(&id),
                    )
                    .await?;
                    "refused"
                };
                *counts.get_mut(counter).expect("counter") += 1;
            }
        }
    }
    let refused = counts["refused"] > 0 && counts.iter().all(|(k, v)| *k == "refused" || *v == 0);
    if refused {
        issue(
            tx,
            c,
            "feed:refused",
            "feed_refused",
            "every_record_refused",
            "error",
            None,
        )
        .await?;
    } else {
        for (id, raw) in projections {
            if feed.full.contains(&id.0) && raw["active"] == true && !seen.contains(&id) {
                let current: WorkforceRecord = store::decode(raw.clone())?;
                switch_off(
                    tx,
                    raw,
                    current.observed_at,
                    feed.as_of.max(current.observed_at),
                )
                .await?;
                *counts.get_mut("deactivated").expect("counter") += 1;
            }
        }
        if feed.organisation == Some(false) || (feed.organisation.is_none() && had_positions) {
            issue(
                tx,
                c,
                "organisation:unavailable",
                "organisation_unavailable",
                "provider_unavailable",
                "warning",
                None,
            )
            .await?;
        } else {
            resolve_key(tx, c, "organisation:unavailable").await?;
        }
        let new_version = version(&cp) + 1;
        let before = store::rows(tx.sql(), Table::Checkpoint, c)
            .await?
            .into_iter()
            .next();
        store::save(tx,Table::Checkpoint,before,json!({"tenant_id":c.tenant_id,"connection_id":c.id,"version":new_version,"resume_cursor":feed.resume,"as_of_at":feed.as_of.to_string()})).await?;
        run["checkpoint_version"] = json!(new_version);
        resolve_key(tx, c, "feed:refused").await?;
    }
    for (counter, value) in counts {
        run[counter] = json!(value);
    }
    run["state"] = json!(if refused { "refused" } else { "succeeded" });
    run["reason"] = if refused {
        json!("every_record_refused")
    } else {
        Value::Null
    };
    run["as_of_at"] = json!(feed.as_of.to_string());
    run["finished_at"] = json!(Timestamp::now().to_string());
    store::decode(store::save(tx, Table::Run, Some(before_run), run).await?)
}
