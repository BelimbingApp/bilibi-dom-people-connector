//! Bilimbi Connector.Doctor: fixed findings without repair or transport side effects.
use crate::{
    authorize, connections, people, require_mapping, require_writable, retention::action,
    Connector, ConnectorError, Credential, Result, Status,
};
use bilibi_database::Error;
use bilibi_settings::Target;
use bilibi_tenancy::Scope;
use jiff::Timestamp;
use serde::Serialize;
use serde_json::json;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FindingState {
    Ok,
    Warning,
    Error,
}
impl FindingState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Warning => "Review",
            Self::Error => "Action required",
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct Finding {
    pub code: &'static str,
    pub state: FindingState,
    pub title: &'static str,
    pub action: &'static str,
    pub count: Option<i64>,
    #[serde(with = "crate::sync_types::optional_instant")]
    pub at: Option<Timestamp>,
}
impl Finding {
    fn new(
        code: &'static str,
        state: FindingState,
        title: &'static str,
        action: &'static str,
    ) -> Self {
        Self {
            code,
            state,
            title,
            action,
            count: None,
            at: None,
        }
    }
    fn tally(code: &'static str, count: i64, title: &'static str, action: &'static str) -> Self {
        Self {
            count: Some(count),
            ..Self::new(
                code,
                if count == 0 {
                    FindingState::Ok
                } else {
                    FindingState::Warning
                },
                title,
                action,
            )
        }
    }
    fn at(mut self, at: Option<Timestamp>) -> Self {
        self.at = at;
        self
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct DoctorReport {
    #[serde(with = "crate::sync_types::instant")]
    pub checked_at: Timestamp,
    pub platform_company_id: i64,
    pub checks: Vec<Finding>,
    pub healthy: bool,
}
fn timestamp(value: Option<String>) -> Option<Timestamp> {
    value.and_then(|v| v.parse().ok())
}
impl Connector {
    /// Runs an operator check and records only codes, states and counts in audit.
    pub async fn run_doctor(&self, scope: &Scope, company: i64) -> Result<DoctorReport> {
        use FindingState::{Error as Bad, Ok as Good, Warning};
        let mut client = self.db.get().await.map_err(Error::from)?;
        authorize(&**client, scope, company).await?;
        let tx = self
            .begin(&mut client, scope, "people-connector.doctor")
            .await?;
        require_writable(&tx, scope, company).await?;
        let c = connections::find(tx.sql(), scope.tenant(), company, true).await?;
        let now = Timestamp::now();
        let current = match people::company(&self.ports, &tx, scope, company).await {
            Ok(read) => Status::from_workforce(read).and_then(|s| {
                if let Some(c) = &c {
                    require_mapping(c, &s)?;
                }
                Ok(s)
            }),
            Err(e) => Err(e),
        };
        let configuration = match &current {
            Ok(_) if c.as_ref().is_some_and(|c| c.enabled) => Finding::new(
                "configuration",
                Good,
                "Connection enabled",
                "The company mapping is current.",
            ),
            Ok(_) if c.is_some() => Finding::new(
                "configuration",
                Warning,
                "Connection disabled",
                "Enable the connection when directory synchronisation is required.",
            ),
            Ok(_) => Finding::new(
                "configuration",
                Warning,
                "No connection configured",
                "Choose the native provider on People connections.",
            ),
            Err(ConnectorError::MappingChanged) => Finding::new(
                "configuration",
                Bad,
                "Company mapping changed",
                "Reconfigure the connection to the current workforce company, then synchronise.",
            ),
            Err(_) => Finding::new(
                "configuration",
                Bad,
                "Company identity unavailable",
                "Restore the People workforce company before synchronising.",
            ),
        };
        let mut checks = vec![configuration];
        let provider = c
            .as_ref()
            .and_then(|c| self.registry.fetch(&c.provider_id).ok());
        if let (Some(c), Some(provider)) = (&c, provider) {
            let credential = provider.credential == Credential::None
                || self.credential_stored(tx.sql(), scope, company).await?;
            checks.extend([
                Finding::new("provider",if c.provider_contract_version==provider.contract_version{Good}else{Bad},"Provider contract","Reconfigure if the stored provider contract no longer matches the installed provider."),
                Finding::new("credential",if credential{Good}else{Bad},"Provider credential",if provider.credential==Credential::None{"The co-located native provider requires no credential."}else{"Store the required encrypted credential on People connections."}),
                Finding::new("adapter",if self.adapters.fetch(&provider.id).is_some(){Good}else{Bad},"Native adapter","Install the adapter that registers this provider's directory reads."),
            ]);
        } else {
            checks.push(Finding::new(
                "provider",
                Bad,
                "No supported provider",
                "Choose the co-located native provider; remote and vendor transport are disabled.",
            ));
        }
        if let Some(c) = &c {
            let policy = self.sync_policy(tx.sql(), scope, company).await?;
            let counts=tx.sql().query_one("SELECT count(*) FILTER (WHERE state='running' AND started_at<clock_timestamp()-make_interval(mins=>$3)),count(*) FILTER (WHERE state IN ('failed','unknown','refused','stale','unavailable')) FROM people_connector_sync_runs WHERE tenant_id=$1 AND connection_id=$2",&[&scope.tenant(),&c.id,&(policy.run_timeout_minutes as i32)]).await.map_err(Error::from)?;
            let issues:i64=tx.sql().query_one("SELECT count(*) FROM people_connector_reconciliation_issues WHERE tenant_id=$1 AND connection_id=$2 AND status='open'",&[&scope.tenant(),&c.id]).await.map_err(Error::from)?.get(0);
            let last=tx.sql().query_opt("SELECT state,to_char(coalesce(finished_at,started_at) AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM people_connector_sync_runs WHERE tenant_id=$1 AND connection_id=$2 ORDER BY started_at DESC,id DESC LIMIT 1",&[&scope.tenant(),&c.id]).await.map_err(Error::from)?;
            let cp=tx.sql().query_opt("SELECT to_char(as_of_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM people_connector_sync_checkpoints WHERE tenant_id=$1 AND connection_id=$2",&[&scope.tenant(),&c.id]).await.map_err(Error::from)?;
            let at = timestamp(cp.map(|r| r.get(0)));
            let fresh = if current.is_err() || !c.enabled || at.is_none() {
                Finding::new(
                    "projection",
                    Bad,
                    "Directory unavailable",
                    "Restore the connection mapping and complete a synchronisation pass.",
                )
            } else if at
                .is_some_and(|at| now.as_second() - at.as_second() > policy.max_age_minutes * 60)
            {
                Finding::new(
                    "projection",
                    Warning,
                    "Directory stale",
                    "Run a synchronisation pass on People connections.",
                )
                .at(at)
            } else {
                Finding::new(
                    "projection",
                    Good,
                    "Directory current",
                    "The completed checkpoint is within the company freshness policy.",
                )
                .at(at)
            };
            checks.extend([
                fresh,
                Finding::new("last_sync",if last.as_ref().is_some_and(|r|r.get::<_,String>(0)=="succeeded"){Good}else{Warning},"Last synchronisation","Review the last pass on People connections and retry if it did not complete.").at(timestamp(last.map(|r|r.get(1)))),
                Finding::tally("stalled_syncs",counts.get(0),"Timed-out synchronisations","Run a new pass to record the unknown outcome and retry."),
                Finding::tally("failed_syncs",counts.get(1),"Unsuccessful synchronisations retained","Review pass outcomes and reconciliation issues; resolved history remains until retention applies."),
                Finding::tally("issues",issues,"Open reconciliation issues","Review and resolve the issues on People connections."),
            ]);
        } else {
            checks.push(Finding::new(
                "projection",
                Warning,
                "Directory unavailable",
                "Configure and synchronise the connection.",
            ));
        }
        let webhook = if let Some(c) = &c {
            Some(crate::webhook_store::policy(tx.sql(), c).await?)
        } else {
            None
        };
        let enabled = webhook.as_ref().is_some_and(|p| p.enabled);
        let missing = webhook
            .as_ref()
            .is_some_and(|p| p.enabled && p.secret.is_none());
        let last = if let Some(c) = &c {
            crate::webhook_store::last_received(tx.sql(), c).await?
        } else {
            None
        };
        checks.push(
            Finding::new(
                "webhook",
                if missing { Bad } else { Good },
                if enabled {
                    "Webhook intake enabled"
                } else {
                    "Webhook intake disabled"
                },
                if missing {
                    "Store an encrypted signing secret or disable intake on People connections."
                } else {
                    "Intake is operator controlled; notifications do not run synchronisation."
                },
            )
            .at(last),
        );
        let policy = crate::files::policy(&self.settings, tx.sql(), scope, company).await?;
        let counts=tx.sql().query_one("SELECT count(*) FILTER (WHERE state='failed'),count(*) FILTER (WHERE state='pending' AND updated_at<clock_timestamp()-make_interval(mins=>$3)),count(*) FILTER (WHERE state='ready' AND expires_at<=clock_timestamp()) FROM people_connector_file_exchanges WHERE tenant_id=$1 AND platform_company_id=$2",&[&scope.tenant(),&company,&(policy.stale_minutes as i32)]).await.map_err(Error::from)?;
        let storage = self
            .settings
            .resolve_many(
                tx.sql(),
                scope,
                Target::Global,
                &["artifacts.storage_root", "artifacts.retention_days"],
            )
            .await?;
        let storage_ok = storage
            .get("artifacts.storage_root")
            .and_then(|r| r.value.as_str())
            .is_some_and(|v| !v.is_empty())
            && storage
                .get("artifacts.retention_days")
                .is_some_and(|r| !r.value.is_null());
        checks.extend([
            Finding::new("file_storage",if policy.enabled&&!storage_ok{Bad}else{Good},"Private file storage configuration","An installation operator must configure private document storage and document retention in System settings."),
            Finding::new("files",if policy.enabled&&!policy.json_enabled{Warning}else{Good},if policy.enabled{"File exchange enabled"}else{"File exchange disabled"},"Review format and private storage settings on File exchange."),
            Finding::tally("failed_files",counts.get(0),"Failed file exchanges","Review the exchange history and private storage settings before retrying."),
            Finding::tally("stalled_files",counts.get(1),"Abandoned file exchanges","Retry the file exchange to record abandonment and create a fresh receipt."),
            Finding::tally("expired_files",counts.get(2),"Expired file receipts","Remove expired bytes on File exchange; configure receipt retention here."),
        ]);
        action(&tx,"people-connector.doctor",company,json!({"checks":checks.iter().map(|f|json!({"code":f.code,"state":f.state,"count":f.count})).collect::<Vec<_>>()}),false).await?;
        tx.commit().await?;
        Ok(DoctorReport {
            checked_at: now,
            platform_company_id: company,
            healthy: checks.iter().all(|f| f.state == Good),
            checks,
        })
    }
}
