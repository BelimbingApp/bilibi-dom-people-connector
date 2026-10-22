//! Mirrors Bilimbi Connector.Web.OperationsLive, using shared live and table controls.
use crate::{Connector, DoctorReport, PurgeResult, RetentionPolicy, MANAGE, RETENTION_FIELDS};
use askama::Template;
use bilibi_database::{Db, Error, Result};
use bilibi_datetime::{DisplayContext, DisplayPolicy, Format, Precision};
use bilibi_module_registry::{
    contributions::{Access, Method, PageWrites, Pending, Route, PAGE_WRITES, ROUTES},
    Contribution, Provenance, Services,
};
use bilibi_tenancy::{NamedOrganisation, OrganisationDirectory, Scope};
use bilibi_ui::{
    page::{
        LiveBody, LiveFuture, LiveInput, LiveRequest, LiveState, MountFuture, Page, PageBody,
        PageFuture, PageRequest, PageService,
    },
    params, BuiltinColumn, Button, Cell, Choice, CommitStates, CommitStatus, ConfirmDialog,
    EmptyState, Fact, Facts, Field, FlashMap, FlexRow, FlexTable, Form, Html, InlineEdit,
    InlineField, InputKind, Instant, ListColumns, ListSpec, ListState, StatusKind,
};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
pub(crate) const PAGE_ID: &str = "people_connector/connector.operations_pages";
pub(crate) fn pages_service(
    services: &Services,
) -> bilibi_module_registry::Result<Arc<dyn std::any::Any + Send + Sync>> {
    Ok(Arc::new(PageService(Arc::new(Pages {
        connector: services.get(crate::SERVICE)?,
        db: services.get::<Db>("base/database.pool")?.as_ref().clone(),
        display: services.get("base/datetime.display")?,
        locales: services.get("base/locale.locales")?,
    }))))
}
macro_rules! provenance {
    () => {
        Provenance {
            module: crate::MODULE,
            source: file!(),
            line: line!(),
        }
    };
}
inventory::submit! {Contribution::new(ROUTES,"people_connector/connector.operations",provenance!(),&Route{method:Method::Get,path:"/integrations/people/operations",service:PAGE_ID,operation:"operations",access:Access::Capability(MANAGE),framed:true})}
inventory::submit! {Contribution::new(PAGE_WRITES,PAGE_ID,provenance!(),&PageWrites{service:PAGE_ID,contracts:&[],pending:Pending::Events{events:&["run_doctor","save_policy","purge"],reason:"Bilimbi doctor and retention facade writes; operation contracts are a separate port slice."}})}
#[derive(Clone)]
struct Pages {
    connector: Arc<Connector>,
    db: Db,
    display: Arc<DisplayPolicy>,
    locales: Arc<bilibi_locale::Locales>,
}
struct State {
    pages: Pages,
    company: Option<i64>,
    companies: Vec<NamedOrganisation>,
    policy: Option<RetentionPolicy>,
    report: Option<DoctorReport>,
    purge_result: Option<PurgeResult>,
    pending_purge: bool,
    columns: ListColumns,
    errors_columns: ListColumns,
    list: ListState,
    commits: CommitStates,
    flash: Option<(String, StatusKind)>,
    display: DisplayContext,
}
impl State {
    async fn new(pages: Pages, scope: &Scope, query: &HashMap<String, String>) -> Result<Self> {
        let mut state = Self {
            pages,
            company: None,
            companies: vec![],
            policy: None,
            report: None,
            purge_result: None,
            pending_purge: false,
            columns: ListColumns::mount(
                "people-doctor-checks",
                vec![
                    BuiltinColumn::new("title", "Check"),
                    BuiltinColumn::new("state", "State"),
                    BuiltinColumn::new("count", "Records"),
                    BuiltinColumn::new("at", "Last activity"),
                    BuiltinColumn::new("action", "Next step"),
                ],
            )
            .map_err(|_| Error::Internal)?,
            errors_columns: ListColumns::mount(
                "people-retention-errors",
                vec![
                    BuiltinColumn::new("kind", "Record type"),
                    BuiltinColumn::new("id", "Record ID"),
                    BuiltinColumn::new("reason", "Next step"),
                ],
            )
            .map_err(|_| Error::Internal)?,
            list: ListState::parse(
                query,
                ListSpec::default().shared().map_err(|_| Error::Internal)?,
            )
            .map_err(|_| Error::Invalid)?,
            commits: CommitStates::default(),
            flash: None,
            display: DisplayContext::anonymous(),
        };
        state.select(scope, query).await?;
        Ok(state)
    }
    async fn select(&mut self, scope: &Scope, query: &HashMap<String, String>) -> Result<()> {
        let client = self.pages.db.get().await.map_err(Error::from)?;
        self.companies = bilibi_company::organisation::Directory
            .selectable(&**client, scope, MANAGE)
            .await?;
        self.company = if query.contains_key("company_id") {
            params::positive_id(query.get("company_id").map(String::as_str))
                .filter(|id| self.companies.iter().any(|c| c.id.get() == *id))
        } else {
            self.companies.first().map(|c| c.id.get())
        };
        let locale = self
            .pages
            .locales
            .resolve(&**client, scope, None)
            .await?
            .locale;
        self.display = self
            .pages
            .display
            .display(
                &**client,
                scope,
                self.company
                    .and_then(|id| bilibi_party_ids::OrganisationId::new(id).ok()),
                locale,
            )
            .await?;
        drop(client);
        self.report = None;
        self.purge_result = None;
        self.pending_purge = false;
        self.commits = CommitStates::default();
        self.refresh(scope).await;
        Ok(())
    }
    async fn refresh(&mut self, scope: &Scope) {
        self.policy = match self.company {
            Some(company) => self
                .pages
                .connector
                .retention_policy(scope, company)
                .await
                .ok(),
            None => None,
        };
    }
    async fn event(&mut self, scope: &Scope, payload: &Value) -> Result<()> {
        let event = payload["type"].as_str().unwrap_or("");
        let values = params::string_values(payload);
        match event {
            "clear-flash" => {
                self.flash = None;
                return Ok(());
            }
            "grid" => {
                self.columns.handle(&values);
                return Ok(());
            }
            "errors_grid" => {
                self.errors_columns.handle(&values);
                return Ok(());
            }
            "select_company" => {
                self.select(scope, &values).await?;
                return Ok(());
            }
            "cancel_purge" => {
                self.pending_purge = false;
                return Ok(());
            }
            _ => {}
        }
        let Some(company) = self.company else {
            self.flash = Some(("Select a company you may manage.".into(), StatusKind::Error));
            return Ok(());
        };
        let allowed = {
            let client = self.pages.db.get().await.map_err(Error::from)?;
            crate::authorize(&**client, scope, company).await.is_ok()
        };
        if !allowed {
            self.pending_purge = false;
            self.policy = None;
            self.flash = Some((
                "A connection manager must have permission to manage this company.".into(),
                StatusKind::Error,
            ));
            return Ok(());
        }
        match event {
            "run_doctor" => match self.pages.connector.run_doctor(scope, company).await {
                Ok(report) => {
                    self.report = Some(report);
                    self.flash = Some(("Health check recorded.".into(), StatusKind::Success))
                }
                Err(e) => {
                    self.report = None;
                    self.flash = Some((message(&e), StatusKind::Error))
                }
            },
            "save_policy" => {
                let field = values.get("field").map(String::as_str).unwrap_or("");
                let value = values.get("value").map(String::as_str).unwrap_or("");
                let parsed = if value.is_empty() {
                    Ok(Value::Null)
                } else {
                    value
                        .parse::<i64>()
                        .map(|v| json!(v))
                        .map_err(|_| crate::ConnectorError::InvalidRetentionPolicy)
                };
                let result = match parsed {
                    Ok(value) => {
                        self.pages
                            .connector
                            .put_retention_policy(
                                scope,
                                company,
                                &serde_json::Map::from_iter([(field.to_owned(), value)]),
                            )
                            .await
                    }
                    Err(e) => Err(e),
                };
                match result {
                    Ok(policy) => {
                        self.policy = Some(policy);
                        self.commits.put(field, CommitStatus::Saved)
                    }
                    Err(e) => self.commits.put(field, CommitStatus::Error(message(&e))),
                }
            }
            "request_purge" => self.pending_purge = true,
            "purge" if self.pending_purge => {
                self.pending_purge = false;
                match self.pages.connector.purge(scope, company).await {
                    Ok(result) => {
                        self.flash = Some(if result.errors.is_empty() {
                            ("Retention batch completed.".into(), StatusKind::Success)
                        } else {
                            ("Some records could not be removed. Review the results and retry after the configured delay.".into(),StatusKind::Error)
                        });
                        self.purge_result = Some(result);
                        self.report = None
                    }
                    Err(e) => {
                        self.purge_result = None;
                        self.flash = Some((message(&e), StatusKind::Error))
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn instant(&self, at: jiff::Timestamp) -> Instant {
        let date = self.display.render(at, Format::DateTime, Precision::Minute);
        Instant::new(
            &date.iso,
            &date.text,
            &date.company,
            &date.utc,
            date.mode.as_str(),
        )
    }
    fn render(&self) -> Result<String> {
        let selector = if self.companies.is_empty() {
            Html::EMPTY
        } else {
            render(Form {
                id: "people-operations-company-form",
                event: "select_company",
                change: true,
                body: render(
                    Field::named("company_id")
                        .id("people-operations-company")
                        .label("Company")
                        .kind(InputKind::Select)
                        .value(self.company.map(|id| id.to_string()).unwrap_or_default())
                        .options(
                            self.companies
                                .iter()
                                .map(|c| Choice::new(c.name.clone(), c.id.get().to_string()))
                                .collect(),
                        ),
                )?,
                ..Default::default()
            })?
        };
        let content = if let Some(policy) = &self.policy {
            let mut findings = Html::EMPTY;
            let mut facts = Html::EMPTY;
            if let Some(report) = &self.report {
                let rows = report
                    .checks
                    .iter()
                    .map(|f| {
                        FlexRow::new(f.code)
                            .cell("title", Cell::text(f.title))
                            .cell("state", Cell::text(f.state.label()))
                            .cell(
                                "count",
                                Cell::text(f.count.map(|v| v.to_string()).unwrap_or("—".into())),
                            )
                            .cell(
                                "at",
                                f.at.map(|t| Cell::instant(self.instant(t)))
                                    .unwrap_or_else(|| Cell::text("—")),
                            )
                            .cell("action", Cell::text(f.action))
                    })
                    .collect();
                let mut table =
                    FlexTable::new("people-doctor-checks", self.columns.flex_columns(), rows);
                table.caption = "Health check findings";
                table.zoom = self.columns.zoom;
                table.suggestions = self.columns.flex_suggestions();
                table.add_query = &self.columns.add_query;
                findings = render(table)?;
                facts = render(Facts {
                    id: "people-doctor-facts",
                    items: vec![
                        Fact {
                            title: "Checked",
                            value: render(CheckedTime {
                                at: self.instant(report.checked_at),
                            })?,
                            ..Default::default()
                        },
                        Fact {
                            title: "Result",
                            value: Html::text(if report.healthy {
                                "Healthy"
                            } else {
                                "Review findings"
                            }),
                            ..Default::default()
                        },
                    ],
                })?;
            } else {
                findings.push(&render(EmptyState::missing("No health check run","Run a check to review this company's connection, synchronisation, webhook and file exchange state.").id("people-doctor-empty"))?)
            }
            let values = serde_json::to_value(policy).map_err(|_| Error::Internal)?;
            let mut items = Vec::new();
            for (name, _, label, max, nullable) in RETENTION_FIELDS {
                let mut field = InlineField::new(
                    format!("people-retention-{name}"),
                    name,
                    label,
                    values[name]
                        .as_i64()
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                )
                .status(self.commits.get(name));
                field.id_value = self.company.unwrap().to_string();
                field.save_event = "save_policy".into();
                field.allow_empty = nullable;
                field.placeholder = if nullable { "Keep records" } else { "Required" }.into();
                field.hint = format!(
                    "1–{max}{}",
                    if nullable {
                        " days; blank keeps records"
                    } else {
                        ""
                    }
                );
                items.push(Fact {
                    title: label,
                    value: render(InlineEdit { field })?,
                    ..Default::default()
                });
            }
            let policy = render(Facts {
                id: "people-retention-policy",
                items,
            })?;
            let mut purge = Html::EMPTY;
            let mut errors = Html::EMPTY;
            if let Some(result) = &self.purge_result {
                purge = render(Facts {
                    id: "people-retention-result",
                    items: vec![
                        Fact {
                            title: "Removed",
                            value: Html::text(&result.deleted.len().to_string()),
                            ..Default::default()
                        },
                        Fact {
                            title: "Failed",
                            value: Html::text(&result.errors.len().to_string()),
                            ..Default::default()
                        },
                    ],
                })?;
                if !result.errors.is_empty() {
                    let rows = result
                        .errors
                        .iter()
                        .map(|r| {
                            FlexRow::new(format!("{}-{}", r.kind, r.id))
                                .cell(
                                    "kind",
                                    Cell::text(match r.kind.as_str() {
                                        "sync" => "Sync run",
                                        "webhook" => "Webhook delivery",
                                        "nonce" => "Webhook replay guard",
                                        _ => "File receipt",
                                    }),
                                )
                                .cell("id", Cell::text(&r.id))
                                .cell("reason", Cell::text(r.reason.as_deref().unwrap_or("")))
                        })
                        .collect();
                    let mut table = FlexTable::new(
                        "people-retention-errors",
                        self.errors_columns.flex_columns(),
                        rows,
                    );
                    table.caption = "Records retained for retry";
                    table.event = "errors_grid";
                    table.zoom = self.errors_columns.zoom;
                    table.suggestions = self.errors_columns.flex_suggestions();
                    table.add_query = &self.errors_columns.add_query;
                    errors = render(table)?;
                }
            }
            render(Content {
                run: render(Button {
                    id: "people-doctor-run",
                    event: "run_doctor",
                    working: "Checking…",
                    body: Html::text("Run health check"),
                    ..Default::default()
                })?,
                request: render(Button {
                    id: "people-retention-request",
                    event: "request_purge",
                    body: Html::text("Purge eligible records"),
                    ..Default::default()
                })?,
                company: self.company.unwrap(),
                facts,
                findings,
                policy,
                purge,
                errors,
            })?
        } else {
            render(EmptyState::missing("Connection operations unavailable","Select an accessible company. A connection manager must have permission to manage it.").id("people-operations-unavailable"))?
        };
        let dialog = if self.pending_purge {
            render(ConfirmDialog{id:"people-retention-confirm",consequence:&format!("Eligible connector records for {} will be deleted.",self.companies.iter().find(|c|Some(c.id.get())==self.company).map(|c|c.name.as_str()).unwrap_or("this company")),detail:"This cannot be undone. Each record is audited. Checkpoints, directory projections, reconciliation issues and audit history remain.",confirm:"Purge",working:"Purging…",confirm_event:"purge",cancel_event:"cancel_purge"})?
        } else {
            Html::EMPTY
        };
        let mut flash = FlashMap::default();
        if let Some((text, kind)) = &self.flash {
            match kind {
                StatusKind::Success => flash.success = text,
                StatusKind::Info => flash.info = text,
                _ => flash.error = text,
            }
        }
        View {
            selector,
            content,
            dialog,
            flash,
        }
        .render()
        .map_err(|_| Error::Internal)
    }
    fn query(&self) -> HashMap<String, String> {
        let mut query = self.list.to_params();
        if let Some(company) = self.company {
            query.insert("company_id".into(), company.to_string());
        }
        query
    }
}
fn message(error: &crate::ConnectorError) -> String {
    match error {
        crate::ConnectorError::Unauthorized => "A connection manager must have permission to manage this company.".into(),
        crate::ConnectorError::NotFound => "Select an accessible active company.".into(),
        crate::ConnectorError::Failed(_) => "Record kept because the operation failed. Check database and audit storage, then retry.".into(),
        _ => error.to_string(),
    }
}
fn render(template: impl Template) -> Result<Html> {
    Html::render(&template).map_err(|_| Error::Internal)
}
#[derive(Template)]
#[template(path = "operations.html")]
struct View<'a> {
    selector: Html,
    content: Html,
    dialog: Html,
    flash: FlashMap<'a>,
}
#[derive(Template)]
#[template(path = "operations_content.html")]
struct Content {
    run: Html,
    request: Html,
    company: i64,
    facts: Html,
    findings: Html,
    policy: Html,
    purge: Html,
    errors: Html,
}
impl Page<Scope, Error> for Pages {
    fn title(&self, _: &str) -> Option<&'static str> {
        Some("People connection health")
    }
    fn render<'a>(&'a self, request: PageRequest<'a, Scope>) -> PageFuture<'a, Error> {
        Box::pin(async move {
            let state = State::new(
                self.clone(),
                request.scope.ok_or(Error::Unauthorized)?,
                request.query,
            )
            .await?;
            Ok(PageBody {
                html: state.render()?,
                live: true,
                title: None,
            })
        })
    }
    fn mount<'a>(&'a self, request: PageRequest<'a, Scope>) -> MountFuture<'a, Scope, Error> {
        Box::pin(async move {
            Ok(Some(Box::new(
                State::new(
                    self.clone(),
                    request.scope.ok_or(Error::Unauthorized)?,
                    request.query,
                )
                .await?,
            ) as Box<dyn LiveState<Scope, Error>>))
        })
    }
}
impl LiveState<Scope, Error> for State {
    fn update<'a>(&'a mut self, request: LiveRequest<'a, Scope>) -> LiveFuture<'a, Error> {
        Box::pin(async move {
            match request.input {
                LiveInput::Event(payload) => self.event(request.scope, payload).await?,
                LiveInput::Params(query) => self.select(request.scope, query).await?,
                LiveInput::Refresh => self.refresh(request.scope).await,
            }
            Ok(LiveBody {
                html: self.render()?,
                query: self.query(),
                ..Default::default()
            })
        })
    }
}

#[derive(Template)]
#[template(
    source = "{% import \"flex_table.html\" as flex %}{% call flex::instant(id=\"people-doctor-time\",i=at) %}{% endcall %}",
    ext = "html"
)]
struct CheckedTime {
    at: Instant,
}
