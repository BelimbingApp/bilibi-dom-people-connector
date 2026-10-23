//! Our serde types for Workforce's documented directory port. Bilimbi's
//! native adapter maps only directory identities, never account identities.
use crate::{current, cursor::Cursor};
use bilibi_database::Error;
use bilibi_people_connector_connector::{
    contracts::ORGANIZATION_DIRECTORY, AssignmentRecord, Freshness, Page, PortAuthorization,
    PortContext, PortRequest, ReadRefusal, RecordKind, WorkforceRecord, DIRECTORY_PORT,
};
use bilibi_ports::PortError;
use jiff::Timestamp;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

pub(crate) enum Failure {
    Refused(ReadRefusal),
    NotCurrent(Freshness),
}
impl From<ReadRefusal> for Failure {
    fn from(error: ReadRefusal) -> Self {
        Self::Refused(error)
    }
}
type Result<T> = std::result::Result<T, Failure>;

#[derive(Deserialize)]
struct Reference {
    source_id: String,
    #[serde(rename = "type")]
    kind: String,
    stable_id: String,
}
impl Reference {
    fn matches(&self, a: &PortAuthorization<'_>, kind: &str) -> bool {
        self.source_id == a.workforce_source_id && self.kind == kind
    }
}
#[derive(Deserialize)]
struct Identity {
    reference: Reference,
    platform_company_id: i64,
    workforce_company_id: i64,
}
impl Identity {
    fn check(&self, a: &PortAuthorization<'_>, kind: &str) -> Result<()> {
        if self.platform_company_id != a.platform_company_id
            || self.workforce_company_id != a.workforce_company_id
            || !self.reference.matches(a, kind)
        {
            return Err(ReadRefusal::MappingMismatch.into());
        }
        Ok(())
    }
    fn record(&self, kind: RecordKind, name: String, code: String, at: Timestamp) -> WorkforceRecord {
        WorkforceRecord {
            kind,
            source_id: self.reference.source_id.clone(),
            stable_id: self.reference.stable_id.clone(),
            workforce_company_id: self.workforce_company_id,
            name,
            code,
            email: None,
            supervisor_stable_id: None,
            parent_stable_id: None,
            version: None,
            vacant: None,
            assignments_incomplete: None,
            assignments: vec![],
            observed_at: at,
            active: true,
        }
    }
}
#[derive(Deserialize)]
struct Company {
    #[serde(flatten)]
    identity: Identity,
    name: String,
    code: String,
}
#[derive(Deserialize)]
struct CompanyAnswer {
    value: Company,
}
#[derive(Deserialize)]
struct Employee {
    #[serde(flatten)]
    identity: Identity,
    company_reference: Reference,
    employee_number: String,
    display_name: String,
    email: Option<String>,
    supervisor_reference: Option<Reference>,
}
#[derive(Deserialize)]
struct Assignment {
    reference: Reference,
    employee_reference: Reference,
    kind: String,
}
#[derive(Deserialize)]
struct Position {
    #[serde(flatten)]
    identity: Identity,
    company_reference: Reference,
    code: String,
    title: Option<String>,
    parent_reference: Option<Reference>,
    version: Option<i64>,
    vacant: bool,
    assignments_incomplete: bool,
    assignments: Vec<Assignment>,
}
#[derive(Deserialize)]
struct EmployeePage {
    employees: Vec<Employee>,
    next_cursor: Option<String>,
    snapshot: bool,
    watermark: String,
}
#[derive(Deserialize)]
struct PositionPage {
    positions: Vec<Position>,
    next_cursor: Option<String>,
    snapshot: bool,
    watermark: String,
}

// Read freshness before decoding the value: an unavailable result is never a
// malformed successful page or an empty directory. Never forward provider text.
async fn call<T: DeserializeOwned>(
    context: PortContext<'_>,
    a: &PortAuthorization<'_>,
    request: Value,
    continuing: bool,
) -> Result<T> {
    let answer: Value = context
        .ports
        .call(context.client, a.scope, DIRECTORY_PORT, &request)
        .await
        .map_err(|e| match e {
            PortError::Unavailable(_) => {
                Failure::NotCurrent(Freshness::Unavailable("people_unavailable".into()))
            }
            PortError::Refused(Error::NotFound | Error::Forbidden | Error::Unauthorized) => {
                ReadRefusal::NotFound.into()
            }
            PortError::Refused(Error::Invalid) if continuing => ReadRefusal::InvalidCursor.into(),
            PortError::Refused(Error::Invalid) => ReadRefusal::InvalidRequest.into(),
            _ => ReadRefusal::UnexpectedResult.into(),
        })?;
    match &answer["freshness"] {
        Value::String(s) if s == "current" => {}
        Value::String(s) if s == "unavailable" => {
            return Err(Failure::NotCurrent(Freshness::Unavailable(
                "organisation_unavailable".into(),
            )));
        }
        Value::Object(f) if f.contains_key("unavailable") => {
            return Err(Failure::NotCurrent(Freshness::Unavailable(
                "people_unavailable".into(),
            )));
        }
        Value::Object(f) if f.contains_key("stale") => {
            let at = f["stale"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or(ReadRefusal::UnexpectedResult)?;
            return Err(Failure::NotCurrent(Freshness::Stale(at)));
        }
        _ => return Err(ReadRefusal::UnexpectedResult.into()),
    }
    serde_json::from_value(answer).map_err(|_| ReadRefusal::UnexpectedResult.into())
}

fn company_reference(reference: &Reference, company: &Company, a: &PortAuthorization<'_>) -> Result<()> {
    if !reference.matches(a, "company")
        || reference.stable_id != company.identity.reference.stable_id
    {
        return Err(ReadRefusal::MappingMismatch.into());
    }
    Ok(())
}
fn optional_reference(reference: &Option<Reference>, a: &PortAuthorization<'_>, kind: &str) -> Result<()> {
    if reference.as_ref().is_some_and(|r| !r.matches(a, kind)) {
        return Err(ReadRefusal::MappingMismatch.into());
    }
    Ok(())
}
fn encode(record: WorkforceRecord) -> Result<Value> {
    // Content validation belongs to the sync engine: one malformed record is
    // a per-record refusal. Mapping and reference-kind checks belong here.
    serde_json::to_value(record).map_err(|_| ReadRefusal::UnexpectedResult.into())
}
fn page_shape(length: usize, size: usize, snapshot: bool, watermark: &str) -> Result<()> {
    if length > size || !snapshot || watermark.parse::<Timestamp>().is_err() {
        return Err(ReadRefusal::UnexpectedResult.into());
    }
    Ok(())
}

pub(crate) async fn read(
    context: PortContext<'_>,
    a: &PortAuthorization<'_>,
    request: &PortRequest,
    cursor: Cursor,
) -> Result<Page> {
    let at = cursor.as_of()?;
    let company: CompanyAnswer = call(
        context, a, json!({"read":"company", "company_id":a.platform_company_id}), false,
    ).await?;
    let company = company.value;
    company.identity.check(a, "company")?;
    if a.capability == ORGANIZATION_DIRECTORY {
        let size = request.limit.min(100);
        let day = at.to_string()[..10].to_owned();
        let page: PositionPage = call(context, a, json!({
            "read":"positions", "company_id":a.platform_company_id,
            "as_of":day, "page_size":size, "cursor":cursor.inner,
        }), request.cursor.is_some()).await?;
        page_shape(page.positions.len(), size, page.snapshot, &page.watermark)?;
        let mut entries = Vec::with_capacity(page.positions.len());
        for p in page.positions {
            p.identity.check(a, "position")?;
            company_reference(&p.company_reference, &company, a)?;
            optional_reference(&p.parent_reference, a, "position")?;
            let mut record = p.identity.record(
                RecordKind::Position, p.title.unwrap_or_else(|| p.code.clone()), p.code, at,
            );
            record.parent_stable_id = p.parent_reference.map(|r| r.stable_id);
            record.version = p.version;
            record.vacant = Some(p.vacant);
            record.assignments_incomplete = Some(p.assignments_incomplete);
            for assignment in p.assignments {
                if !assignment.reference.matches(a, "assignment")
                    || !assignment.employee_reference.matches(a, "employee")
                {
                    return Err(ReadRefusal::MappingMismatch.into());
                }
                record.assignments.push(AssignmentRecord {
                    source_id: assignment.reference.source_id,
                    stable_id: assignment.reference.stable_id,
                    employee_stable_id: assignment.employee_reference.stable_id,
                    kind: assignment.kind,
                });
            }
            entries.push(encode(record)?);
        }
        let next = page.next_cursor.map(|inner| cursor.continued(Some(inner))).transpose()?;
        return Ok(current(entries, at, next));
    }
    // A company-only first page keeps every page within even limit=1, and
    // leaves the Workforce scan's page size unchanged throughout continuation.
    if request.cursor.is_none() {
        let record = company.identity.record(RecordKind::Company, company.name, company.code, at);
        return Ok(current(vec![encode(record)?], at, Some(cursor.continued(None)?)));
    }
    let size = request.limit.min(1000);
    let page: EmployeePage = call(context, a, json!({
        "read":"employees", "company_id":a.platform_company_id,
        "page_size":size, "cursor":cursor.inner,
    }), cursor.inner.is_some()).await?;
    page_shape(page.employees.len(), size, page.snapshot, &page.watermark)?;
    let mut entries = Vec::with_capacity(page.employees.len());
    for e in page.employees {
        e.identity.check(a, "employee")?;
        company_reference(&e.company_reference, &company, a)?;
        optional_reference(&e.supervisor_reference, a, "employee")?;
        let mut record = e.identity.record(RecordKind::Employee, e.display_name, e.employee_number, at);
        record.email = e.email;
        record.supervisor_stable_id = e.supervisor_reference.map(|r| r.stable_id);
        entries.push(encode(record)?);
    }
    let next = page.next_cursor.map(|inner| cursor.continued(Some(inner))).transpose()?;
    Ok(current(entries, at, next))
}
