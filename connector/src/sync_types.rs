//! Bilimbi Connector WorkforceRecord, AssignmentRecord, Deactivation, SyncRun and SyncSummary.
use crate::{Freshness, Read};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Directory identity kind; never a login identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    Company,
    Employee,
    Position,
}
impl RecordKind {
    pub fn capability(self) -> &'static str {
        match self {
            Self::Company => "company_directory",
            Self::Employee => "employee_directory",
            Self::Position => "organization_directory",
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Company => "company",
            Self::Employee => "employee",
            Self::Position => "position",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentRecord {
    pub source_id: String,
    pub stable_id: String,
    pub employee_stable_id: String,
    pub kind: String,
}
/// One provider fact. Non-position records leave all organisation fields empty.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkforceRecord {
    pub kind: RecordKind,
    pub source_id: String,
    pub stable_id: String,
    pub workforce_company_id: i64,
    pub name: String,
    pub code: String,
    pub email: Option<String>,
    pub supervisor_stable_id: Option<String>,
    pub parent_stable_id: Option<String>,
    pub version: Option<i64>,
    pub vacant: Option<bool>,
    pub assignments_incomplete: Option<bool>,
    #[serde(default)]
    pub assignments: Vec<AssignmentRecord>,
    #[serde(with = "instant")]
    pub observed_at: Timestamp,
    #[serde(default = "active")]
    pub active: bool,
}
fn active() -> bool {
    true
}
/// Changes pages may contain this shape with `deactivated: true`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deactivation {
    pub kind: RecordKind,
    pub source_id: String,
    pub stable_id: String,
    #[serde(with = "instant")]
    pub observed_at: Timestamp,
    pub deactivated: bool,
}
pub(crate) fn text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.chars().count() <= max && !value.contains('\0')
}
fn optional(value: &Option<String>, max: usize) -> bool {
    value.as_ref().is_none_or(|v| text(v, max))
}
impl WorkforceRecord {
    pub fn valid(&self) -> bool {
        text(&self.source_id, 100)
            && text(&self.stable_id, 100)
            && self.workforce_company_id > 0
            && text(&self.name, 255)
            && text(&self.code, 100)
            && optional(&self.email, 255)
            && optional(&self.supervisor_stable_id, 100)
            && if self.kind == RecordKind::Position {
                optional(&self.parent_stable_id, 100)
                    && self.version.is_none_or(|v| v > 0)
                    && self.vacant.is_some()
                    && self.assignments_incomplete.is_some()
                    && self.assignments.len() <= 500
                    && self.assignments.iter().all(|a| {
                        a.source_id == self.source_id
                            && text(&a.source_id, 100)
                            && text(&a.stable_id, 100)
                            && text(&a.employee_stable_id, 100)
                            && text(&a.kind, 100)
                    })
                    && self
                        .assignments
                        .iter()
                        .map(|a| &a.stable_id)
                        .collect::<BTreeSet<_>>()
                        .len()
                        == self.assignments.len()
            } else {
                self.parent_stable_id.is_none()
                    && self.version.is_none()
                    && self.vacant.is_none()
                    && self.assignments_incomplete.is_none()
                    && self.assignments.is_empty()
            }
    }
}
impl Deactivation {
    pub fn valid(&self) -> bool {
        self.deactivated && text(&self.source_id, 100) && text(&self.stable_id, 100)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Succeeded,
    Stale,
    Unavailable,
    Refused,
    Failed,
    Unknown,
}
impl RunState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Stale => "provider data was stale; nothing applied",
            Self::Unavailable => "provider unavailable; nothing applied",
            Self::Refused => "refused; the checkpoint did not move",
            Self::Failed => "failed; nothing applied",
            Self::Unknown => "outcome unknown; nothing counts as applied",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPass {
    Bootstrap,
    Incremental,
}
/// A pass outcome. Non-successful provider outcomes are successful facade calls with this recorded state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRun {
    pub id: i64,
    pub tenant_id: i64,
    pub connection_id: i64,
    pub platform_company_id: i64,
    pub provider_id: String,
    pub idempotency_key: String,
    pub pass: RunPass,
    pub state: RunState,
    pub reason: Option<String>,
    pub checkpoint_version: Option<i64>,
    #[serde(with = "optional_instant")]
    pub as_of_at: Option<Timestamp>,
    pub applied: i32,
    pub unchanged: i32,
    pub superseded: i32,
    pub deactivated: i32,
    pub refused: i32,
    #[serde(with = "instant")]
    pub started_at: Timestamp,
    #[serde(with = "optional_instant")]
    pub finished_at: Option<Timestamp>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationIssue {
    pub id: i64,
    pub issue_key: String,
    pub kind: String,
    pub reason: String,
    pub severity: String,
    pub record_kind: Option<String>,
    pub stable_id: Option<String>,
    pub status: String,
    pub occurrences: i32,
    #[serde(with = "instant")]
    pub first_seen_at: Timestamp,
    #[serde(with = "instant")]
    pub last_seen_at: Timestamp,
    #[serde(with = "optional_instant")]
    pub resolved_at: Option<Timestamp>,
}
impl ReconciliationIssue {
    pub fn message(&self) -> &'static str {
        match (self.kind.as_str(), self.reason.as_str()) {
            ("record_refused", "foreign_source") => {
                "A record came from a different source than this connection's."
            }
            ("record_refused", "other_company") => {
                "A record belongs to a different workforce company."
            }
            ("record_refused", "undeclared_capability") => {
                "The provider sent a kind of record it does not declare."
            }
            ("record_refused", "invalid_record") => "A record was incomplete or malformed.",
            ("record_refused", "unknown_reference") => {
                "A deactivation named a record that was never synchronised."
            }
            ("feed_refused", _) => {
                "Every record in a pass was refused, so the checkpoint did not move."
            }
            ("empty_bootstrap", _) => "A full read returned no records.",
            ("organisation_unavailable", _) => {
                "Organisation was unavailable, so positions were not updated in this pass."
            }
            ("unknown_outcome", _) => {
                "A pass stopped without recording an outcome. Nothing from it was applied."
            }
            _ => "The provider sent something that could not be applied.",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPolicy {
    pub page_limit: i64,
    pub max_age_minutes: i64,
    pub run_timeout_minutes: i64,
}
/// Partial policy update; validate every supplied field before writing any.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncPolicyUpdate {
    pub page_limit: Option<i64>,
    pub max_age_minutes: Option<i64>,
    pub run_timeout_minutes: Option<i64>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncOptions {
    pub full: bool,
    pub expected_checkpoint_version: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncSummary {
    pub freshness: Freshness,
    pub checkpoint_version: Option<i64>,
    pub as_of_at: Option<Timestamp>,
    pub last_run: Option<SyncRun>,
    pub open_issues: Vec<ReconciliationIssue>,
    pub policy: SyncPolicy,
}
/// Connector-owned copy of People's read-result vocabulary.
pub type ReadResult<T> = Read<T>;

// Keep jiff's shared dependency features unchanged. The wire contract is RFC3339.
pub(crate) mod instant {
    use super::*;
    pub fn serialize<S: serde::Serializer>(v: &Timestamp, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Timestamp, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
pub(crate) mod optional_instant {
    use super::*;
    pub fn serialize<S: serde::Serializer>(v: &Option<Timestamp>, s: S) -> Result<S::Ok, S::Error> {
        v.map(|v| v.to_string()).serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Timestamp>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|v| v.parse().map_err(serde::de::Error::custom))
            .transpose()
    }
}
