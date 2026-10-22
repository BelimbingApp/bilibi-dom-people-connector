//! Bilimbi Connector.Web.BackupsLive. Preview and confirmation remain on the server.
use crate::{BackupPolicyUpdate, BackupPreview, BackupSummary, Connector, ConnectorError, MANAGE};
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
    params, ActionLink, BuiltinColumn, Button, Card, Cell, Choice, CommitStates, CommitStatus,
    ConfirmDialog, EmptyState, Fact, Facts, Field, FlashMap, FlexRow, FlexTable, Form, Html,
    InlineEdit, InlineField, InputKind, Instant, ListColumns, ListSpec, ListState, SectionHeading,
    StatusKind, TableEmpty,
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
pub(crate) const PAGE_ID: &str = "people_connector/connector.backups_pages";
const PATH: &str = "/integrations/people/backups";
macro_rules! provenance {
    () => {
        Provenance {
            module: crate::MODULE,
            source: file!(),
            line: line!(),
        }
    };
}
inventory::submit! {Contribution::new(ROUTES,"people_connector/connector.backups",provenance!(),&Route{method:Method::Get,path:PATH,service:PAGE_ID,operation:"backups",access:Access::Capability(MANAGE),framed:true})}
inventory::submit! {Contribution::new(PAGE_WRITES,PAGE_ID,provenance!(),&PageWrites{service:PAGE_ID,contracts:&[],pending:Pending::Events{events:&["save_policy","backup","preview","request_restore","restore","recover","purge","retry_purge"],reason:"Bilimbi backup facade writes; operation contracts are a separate port slice."}})}
pub(crate) fn pages_service(
    services: &Services,
) -> bilibi_module_registry::Result<Arc<dyn std::any::Any + Send + Sync>> {
    Ok(Arc::new(PageService(Arc::new(Pages::new(services)?))))
}
#[derive(Clone)]
struct Pages {
    connector: Arc<Connector>,
    db: Db,
    display: Arc<DisplayPolicy>,
    locales: Arc<bilibi_locale::Locales>,
    settings: Arc<bilibi_settings::Settings>,
}
impl Pages {
    fn new(services: &Services) -> bilibi_module_registry::Result<Self> {
        Ok(Self {
            connector: services.get(crate::SERVICE)?,
            db: services.get::<Db>("base/database.pool")?.as_ref().clone(),
            display: services.get("base/datetime.display")?,
            locales: services.get("base/locale.locales")?,
            settings: services.get("base/settings.store")?,
        })
    }
}
struct State {
    pages: Pages,
    company: Option<i64>,
    companies: Vec<NamedOrganisation>,
    summary: Option<BackupSummary>,
    preview: Option<BackupPreview>,
    confirming: bool,
    history: ListColumns,
    holds: ListColumns,
    changes: ListColumns,
    list: ListState,
    flash: Option<(String, StatusKind)>,
    display: DisplayContext,
    commits: CommitStates,
    instance_banner: Html,
}
fn columns(id: &str, fields: &[(&str, &str)]) -> Result<ListColumns> {
    ListColumns::mount(
        id,
        fields
            .iter()
            .map(|(key, label)| BuiltinColumn::new(key, label))
            .collect(),
    )
    .map_err(|_| Error::Internal)
}
impl State {
    async fn new(pages: Pages, scope: &Scope, query: &HashMap<String, String>) -> Result<Self> {
        let mut state = Self {
            pages,
            company: None,
            companies: vec![],
            summary: None,
            preview: None,
            confirming: false,
            history: columns(
                "people-backups-history",
                &[
                    ("inserted_at", "Created"),
                    ("expires_at", "Expires"),
                    ("state", "State"),
                ],
            )?,
            holds: columns(
                "people-backups-holds",
                &[
                    ("inserted_at", "Created"),
                    ("purge_attempts", "Attempts"),
                    ("purge_last_error", "Reason"),
                    ("purge_held_at", "Held"),
                ],
            )?,
            changes: columns(
                "people-backups-preview",
                &[
                    ("label", "Fact"),
                    ("before", "Current"),
                    ("after", "Backup"),
                ],
            )?,
            list: ListState::parse(
                query,
                ListSpec::default().shared().map_err(|_| Error::Internal)?,
            )
            .map_err(|_| Error::Invalid)?,
            flash: None,
            display: DisplayContext::anonymous(),
            commits: CommitStates::default(),
            instance_banner: Html::EMPTY,
        };
        state.select(scope, query).await?;
        Ok(state)
    }
    async fn select(&mut self, scope: &Scope, query: &HashMap<String, String>) -> Result<()> {
        let client = self.pages.db.get().await.map_err(Error::from)?;
        self.companies = bilibi_company::organisation::Directory
            .selectable(&**client, scope, MANAGE)
            .await?;
        self.companies.retain(|c| !c.archived);
        self.company = if query.contains_key("company_id") {
            params::positive_id(query.get("company_id").map(String::as_str))
                .filter(|id| self.companies.iter().any(|c| c.id.get() == *id))
        } else {
            self.companies.first().map(|c| c.id.get())
        };
        self.instance_banner = bilibi_system::Instance::resolve(&self.pages.settings, &**client)
            .await?
            .banner("people-backups-instance", "mb-3")
            .map_err(|_| Error::Internal)?;
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
        self.preview = None;
        self.confirming = false;
        self.commits = CommitStates::default();
        drop(client);
        self.refresh(scope).await;
        Ok(())
    }
    async fn refresh(&mut self, scope: &Scope) {
        self.summary = match self.company {
            Some(company) => self
                .pages
                .connector
                .backups()
                .summary(scope, company)
                .await
                .ok(),
            None => None,
        };
    }
    fn outcome<T>(&mut self, result: crate::Result<T>, message: &str) {
        self.flash = Some(match result {
            Ok(_) => (message.into(), StatusKind::Success),
            Err(e) => (message_for(&e), StatusKind::Error),
        });
    }
    async fn event(&mut self, scope: &Scope, payload: &Value) -> Result<()> {
        let event = payload["type"].as_str().unwrap_or("");
        match event {
            "clear-flash" => {
                self.flash = None;
                return Ok(());
            }
            "history_grid" | "holds_grid" | "preview_grid" => {
                let columns = match event {
                    "history_grid" => &mut self.history,
                    "holds_grid" => &mut self.holds,
                    _ => &mut self.changes,
                };
                columns.handle(&params::string_values(payload));
                return Ok(());
            }
            "select_company" => {
                self.select(scope, &params::string_values(payload)).await?;
                return Ok(());
            }
            "cancel_restore" => {
                self.confirming = false;
                return Ok(());
            }
            _ => {}
        }
        if ![
            "save_policy",
            "backup",
            "preview",
            "request_restore",
            "restore",
            "recover",
            "purge",
            "retry_purge",
        ]
        .contains(&event)
        {
            return Ok(());
        }
        let Some(company) = self.company else {
            self.flash = Some(("Select a company you may manage.".into(), StatusKind::Error));
            return Ok(());
        };
        let client = self.pages.db.get().await.map_err(Error::from)?;
        if crate::authorize(&**client, scope, company).await.is_err() {
            self.preview = None;
            self.confirming = false;
            self.flash = Some((
                "Connection management permission is required for this company.".into(),
                StatusKind::Error,
            ));
            return Ok(());
        }
        if matches!(event, "preview" | "request_restore") {
            self.instance_banner =
                bilibi_system::Instance::resolve(&self.pages.settings, &**client)
                    .await?
                    .banner("people-backups-instance", "mb-3")
                    .map_err(|_| Error::Internal)?;
        }
        drop(client);
        let values = params::string_values(payload);
        match event {
            "save_policy" => {
                let field = values.get("field").map(String::as_str).unwrap_or("");
                let value = values.get("value").and_then(|v| v.parse::<i64>().ok());
                let update = match (field, value) {
                    ("retention_days", Some(n)) => Some(BackupPolicyUpdate {
                        retention_days: Some(n),
                        ..Default::default()
                    }),
                    ("preview_minutes", Some(n)) => Some(BackupPolicyUpdate {
                        preview_minutes: Some(n),
                        ..Default::default()
                    }),
                    _ => None,
                };
                let result = match update {
                    Some(update) => {
                        self.pages
                            .connector
                            .backups()
                            .configure(scope, company, update)
                            .await
                    }
                    None => Err(ConnectorError::InvalidBackupPolicy),
                };
                self.commits.put(
                    field,
                    match result {
                        Ok(_) => CommitStatus::Saved,
                        Err(_) => CommitStatus::Error(
                            "Enter a whole number within the stated range.".into(),
                        ),
                    },
                );
            }
            "backup" => {
                let result = self.pages.connector.backups().create(scope, company).await;
                self.outcome(result, "Backup stored privately.");
            }
            "preview" => {
                self.confirming = false;
                self.preview = None;
                if let Some(id) = values.get("id") {
                    match self
                        .pages
                        .connector
                        .backups()
                        .preview(scope, company, id)
                        .await
                    {
                        Ok(p) => {
                            self.preview = Some(p);
                            self.flash = None;
                        }
                        Err(e) => self.outcome::<()>(Err(e), ""),
                    }
                }
            }
            "request_restore" if self.preview.is_some() => {
                self.confirming = true;
                self.flash = None;
            }
            "restore" if self.confirming => {
                self.confirming = false;
                if let Some(p) = self.preview.take() {
                    let result = self
                        .pages
                        .connector
                        .backups()
                        .restore(scope, company, &p.id, &p.token, true)
                        .await;
                    self.outcome(result,"Backup restored. Re-enter secrets on People connections before enabling webhook intake.");
                }
            }
            "recover" => {
                if let Some(id) = values.get("id") {
                    let result = self
                        .pages
                        .connector
                        .backups()
                        .recover(scope, company, id)
                        .await;
                    match result {Ok(run) if run.state==crate::RunState::Succeeded=>self.outcome(Ok(run),"Recovery synchronisation completed."),Ok(_)=>self.flash=Some(("Recovery did not complete. Review connection health and sync outcomes.".into(),StatusKind::Error)),Err(e)=>self.outcome::<()>(Err(e),"")}
                }
            }
            "purge" => {
                let result = self
                    .pages
                    .connector
                    .backups()
                    .purge_expired(scope, company)
                    .await;
                match result {Ok(r) if !r.errors.is_empty()=>self.flash=Some(("Some backups could not be removed. Review private storage settings and retry.".into(),StatusKind::Error)),r=>self.outcome(r,"Expired backup cleanup completed.")}
            }
            "retry_purge" => {
                if let Some(id) = values.get("id") {
                    let result = self
                        .pages
                        .connector
                        .backups()
                        .retry_purge(scope, company, id)
                        .await;
                    self.outcome(result, "Held backup removed.");
                }
            }
            _ => {}
        }
        self.refresh(scope).await;
        Ok(())
    }
    fn instant(&self, value: Option<&str>) -> Cell {
        value
            .and_then(|v| v.parse().ok())
            .map(|t| {
                let d = self.display.render(t, Format::DateTime, Precision::Minute);
                Cell::instant(Instant::new(
                    &d.iso,
                    &d.text,
                    &d.company,
                    &d.utc,
                    d.mode.as_str(),
                ))
            })
            .unwrap_or_else(|| Cell::text("None"))
    }
    fn table(
        &self,
        id: &str,
        columns: &ListColumns,
        rows: Vec<FlexRow>,
        caption: &str,
        empty: &str,
        actions: bool,
    ) -> Result<Html> {
        let mut table = FlexTable::new(id, columns.flex_columns(), rows);
        table.event = match id {
            "people-backups-history" => "history_grid",
            "people-backups-holds" => "holds_grid",
            _ => "preview_grid",
        };
        table.zoom = columns.zoom;
        table.suggestions = columns.flex_suggestions();
        table.add_query = &columns.add_query;
        table.caption = caption;
        table.actions = actions;
        table.empty = Some(TableEmpty::Content(Html::text(empty)));
        render(table)
    }
    fn render(&self) -> Result<String> {
        let mut field = Field::named("company_id")
            .id("people-backups-company")
            .label("Company")
            .kind(InputKind::Select)
            .value(self.company.map(|id| id.to_string()).unwrap_or_default());
        field.options = self
            .companies
            .iter()
            .map(|c| Choice::new(&c.name, c.id.get().to_string()))
            .collect();
        let selector = if self.companies.is_empty() {
            Html::EMPTY
        } else {
            render(Form {
                id: "people-backups-company-form",
                event: "select_company",
                change: true,
                body: render(field)?,
                ..Default::default()
            })?
        };
        let mut content = Html::EMPTY;
        if let Some(summary) = &self.summary {
            let mut facts = Vec::new();
            for (key, label, value, hint) in [
                (
                    "retention_days",
                    "Retention (days)",
                    summary.policy.retention_days,
                    "1–3650",
                ),
                (
                    "preview_minutes",
                    "Preview validity (minutes)",
                    summary.policy.preview_minutes,
                    "1–60",
                ),
            ] {
                let mut field = InlineField::new(
                    format!("people-backups-{key}"),
                    key,
                    label,
                    value.to_string(),
                )
                .hint(hint)
                .status(self.commits.get(key));
                field.save_event = "save_policy".into();
                facts.push(Fact {
                    title: label,
                    value: render(InlineEdit { field })?,
                    ..Default::default()
                });
            }
            let mut body = render(Facts {
                id: "people-backups-policy",
                items: facts,
            })?;
            body.push(&Html::text("Backups exclude secrets. Retention is capped by the installation's private artifact retention; configure storage in Operator Settings."));
            content.push(&render(Card {
                class: "mt-5",
                title: "Backup policy",
                body,
                ..Default::default()
            })?);
            let available = self.pages.connector.people_available();
            let mut body = render(SectionHeading {
                title: "Recent backups",
                actions: if available {
                    button(
                        "people-backups-create",
                        "backup",
                        "Back up connection",
                        "Backing up…",
                        None,
                    )?
                } else {
                    Html::EMPTY
                },
                ..Default::default()
            })?;
            if !available {
                body.push(&render(EmptyState::missing(
                    "People is absent.",
                    "Mount People to back up or restore a connection.",
                ))?);
            }
            let mut rows = Vec::new();
            for r in &summary.records {
                let mut row = FlexRow::new(&r.id)
                    .cell("inserted_at", self.instant(Some(&r.inserted_at)))
                    .cell("expires_at", self.instant(Some(&r.expires_at)))
                    .cell(
                        "state",
                        Cell::text(if r.restored_at.is_some() {
                            "Restored"
                        } else {
                            &r.state
                        }),
                    );
                if r.state == "ready" && available {
                    row.actions = if r.restored_at.is_some() {
                        button(
                            &format!("recover-{}", r.id),
                            "recover",
                            "Sync from checkpoint",
                            "Recovering…",
                            Some(&r.id),
                        )?
                    } else {
                        button(
                            &format!("preview-{}", r.id),
                            "preview",
                            "Preview restore",
                            "",
                            Some(&r.id),
                        )?
                    };
                }
                rows.push(row);
            }
            body.push(&self.table(
                "people-backups-history",
                &self.history,
                rows,
                "Recent connection backups",
                "No backups created. Back up a configured native connection to begin.",
                true,
            )?);
            body.push(&button(
                "people-backups-purge",
                "purge",
                "Clean expired backups",
                "Cleaning…",
                None,
            )?);
            content.push(&render(Card {
                class: "mt-5",
                body,
                ..Default::default()
            })?);
            if !summary.holds.is_empty() {
                let mut rows = Vec::new();
                for r in &summary.holds {
                    let mut row = FlexRow::new(&r.id)
                        .cell("inserted_at", self.instant(Some(&r.inserted_at)))
                        .cell("purge_attempts", Cell::text(r.purge_attempts.to_string()))
                        .cell(
                            "purge_last_error",
                            Cell::text(r.purge_last_error.as_deref().unwrap_or("error")),
                        )
                        .cell("purge_held_at", self.instant(r.purge_held_at.as_deref()));
                    row.actions = button(
                        &format!("retry-purge-{}", r.id),
                        "retry_purge",
                        "Retry cleanup",
                        "Retrying…",
                        Some(&r.id),
                    )?;
                    rows.push(row);
                }
                let mut body = self.table(
                    "people-backups-holds",
                    &self.holds,
                    rows,
                    "Expired backups whose cleanup is held",
                    "No cleanup holds.",
                    true,
                )?;
                body.push(&Html::text("Cleanup stopped after repeated failures. Review private storage in Operator Settings, then retry."));
                content.push(&render(Card {
                    class: "mt-5",
                    title: "Held cleanup",
                    body,
                    ..Default::default()
                })?);
            }
            if let Some(preview) = &self.preview {
                let rows = preview
                    .changes
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        FlexRow::new(i.to_string())
                            .cell("label", Cell::text(&c.label))
                            .cell(
                                "before",
                                if c.instant {
                                    self.instant(c.before.as_str())
                                } else {
                                    Cell::text(display(&c.before))
                                },
                            )
                            .cell(
                                "after",
                                if c.instant {
                                    self.instant(c.after.as_str())
                                } else {
                                    Cell::text(display(&c.after))
                                },
                            )
                    })
                    .collect();
                let mut body = self.instance_banner.clone();
                body.push(&self.table(
                    "people-backups-preview",
                    &self.changes,
                    rows,
                    "Changes on restore",
                    "No changes.",
                    false,
                )?);
                body.push(&Html::text("Directory contents and checkpoint cursor will be replaced. The checkpoint generation advances. Sync outcomes, reconciliation, audit history and webhook replay guards remain."));
                body.push(&button(
                    "people-backups-request-restore",
                    "request_restore",
                    "Restore backup",
                    "",
                    None,
                )?);
                content.push(&render(Card {
                    class: "mt-5",
                    title: "Restore preview",
                    body,
                    ..Default::default()
                })?);
            }
        } else {
            content.push(&render(
                EmptyState::missing(
                    "Backups unavailable",
                    "Select an accessible active company with connection management permission.",
                )
                .id("people-backups-unavailable"),
            )?);
        }
        let consequence = format!(
            "Connection state for {} will be replaced.",
            self.companies
                .iter()
                .find(|c| Some(c.id.get()) == self.company)
                .map(|c| c.name.as_str())
                .unwrap_or("this company")
        );
        let dialog = if self.confirming && self.preview.is_some() {
            render(ConfirmDialog{id:"people-backups-confirm",consequence:&consequence,detail:"Configuration, checkpoint and directory records will be restored to the reviewed backup. Credentials and webhook secrets will be cleared; webhook intake will be disabled. This replaces current connector state.",confirm:"Restore",working:"Restoring…",confirm_event:"restore",cancel_event:"cancel_restore"})?
        } else {
            Html::EMPTY
        };
        let mut flash = FlashMap::default();
        if let Some((message, kind)) = &self.flash {
            match kind {
                StatusKind::Success => flash.success = message,
                StatusKind::Info => flash.info = message,
                _ => flash.error = message,
            }
        }
        View {
            actions: render(ActionLink {
                id: "people-backups-connections",
                icon: "manage",
                title: "People connections",
                href: "/integrations/people/connections",
                method: "",
                body: Html::text("People connections"),
            })?,
            selector,
            content,
            dialog,
            flash,
        }
        .render()
        .map_err(|_| Error::Internal)
    }
    fn query(&self) -> HashMap<String, String> {
        let mut q = self.list.to_params();
        if let Some(c) = self.company {
            q.insert("company_id".into(), c.to_string());
        }
        q
    }
}
fn display(value: &Value) -> String {
    match value {
        Value::Null => "None".into(),
        Value::Bool(true) => "Enabled".into(),
        Value::Bool(false) => "Disabled".into(),
        Value::String(s) => s.clone(),
        v => v.to_string(),
    }
}
fn message_for(error: &ConnectorError) -> String {
    match error {
        ConnectorError::Disconnected => "Enable the native connection on People connections before recovery.".into(),
        ConnectorError::CheckpointMoved => "The restored checkpoint was advanced by another sync. Use normal synchronisation on People connections.".into(),
        ConnectorError::SyncInProgress => "A sync is running. Wait for its recorded outcome before backup or restore.".into(),
        ConnectorError::Unauthorized => "Connection management permission is required for this company.".into(),
        ConnectorError::FileStorage(bilibi_artifacts::Error::Policy) => "Configure private artifact storage in Operator Settings.".into(),
        ConnectorError::MappingChanged | ConnectorError::Unsupported => ConnectorError::ConnectionUnavailable.to_string(),
        e @ (ConnectorError::PreviewChanged | ConnectorError::PreviewExpired | ConnectorError::ConnectionChanged | ConnectorError::ConnectionUnavailable | ConnectorError::BackupIntegrity | ConnectorError::BackupCleanupPending | ConnectorError::RestoreRequired | ConnectorError::PeopleUnavailable | ConnectorError::CompanyArchived) => e.to_string(),
        _ => ConnectorError::InvalidBackup.to_string(),
    }
}

fn render(template: impl Template) -> Result<Html> {
    Html::render(&template).map_err(|_| Error::Internal)
}
fn button(id: &str, event: &str, label: &str, working: &str, value: Option<&str>) -> Result<Html> {
    render(Button {
        id,
        event,
        working,
        class: "whitespace-nowrap",
        name: if value.is_some() { "id" } else { "" },
        value: value.unwrap_or(""),
        body: Html::text(label),
        ..Default::default()
    })
}
#[derive(Template)]
#[template(path = "backups.html")]
struct View<'a> {
    actions: Html,
    selector: Html,
    content: Html,
    dialog: Html,
    flash: FlashMap<'a>,
}
impl Page<Scope, Error> for Pages {
    fn title(&self, _: &str) -> Option<&'static str> {
        Some("People connection backups")
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
