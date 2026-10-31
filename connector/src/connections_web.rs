//! Bilimbi Connector.Web.ConnectionsLive: company setup, synchronisation and inbound policy.
use crate::{
    ConnectionState, Connector, ConnectorError, Credential, Freshness, Status, SyncOptions,
    SyncPolicyUpdate, SyncSummary, WebhookSettingsUpdate, WebhookSummary, VIEW,
};
use askama::Template;
use bilibi_database::{Db, Error, Result};
use bilibi_datetime::{DisplayContext, DisplayPolicy, Format, Precision};
use bilibi_module_registry::{
    contributions::{Access, Method, PageWrites, Pending, Route, PAGE_WRITES, ROUTES},
    Catalog, Contribution, Provenance, Services,
};
use bilibi_tenancy::{NamedOrganisation, OrganisationDirectory, Scope};
use bilibi_ui::{
    page::{
        LiveBody, LiveFuture, LiveInput, LiveRequest, LiveState, MountFuture, Page, PageBody,
        PageFuture, PageRequest, PageService,
    },
    params, Badge, BadgeKind, BuiltinColumn, Button, Cell, ColumnsOutcome, ConfirmDialog,
    EmptyState, Fact, Facts, Field, FlashMap, FlexRow, FlexTable, Form, Html, InputKind, Instant,
    ListColumns, ListSpec, ListState, PageCounts, Pagination, SecretInput, SortDir, StatusKind,
    TableEmpty,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub(crate) const PAGE_ID: &str = "people_connector/connector.connections_pages";
use bilibi_ui::SECRET_MASK as MASK;
const WRITES: &[&str] = &[
    "configure",
    "set_enabled",
    "save_credential",
    "request_remove",
    "remove",
    "synchronise",
    "resolve_issue",
    "save_policy",
    "save_webhook",
];
inventory::submit! { Contribution::new(ROUTES, "people_connector/connector.connections", Provenance { module: crate::MODULE, source: file!(), line: line!() }, &Route { company_scope: bilibi_module_registry::contributions::CompanyScope::Tenant, method: Method::Get, path: crate::CONNECTIONS_PATH, service: PAGE_ID, operation: "connections", access: Access::Capability(VIEW), framed: true }) }
inventory::submit! { Contribution::new(PAGE_WRITES, PAGE_ID, Provenance { module: crate::MODULE, source: file!(), line: line!() }, &PageWrites { service: PAGE_ID, contracts: &[], pending: Pending::Events { events: WRITES, reason: "Bilimbi connection facade writes; reviewed operation contracts are a separate port slice." } }) }
pub(crate) fn pages_service(
    services: &Services,
) -> bilibi_module_registry::Result<Arc<dyn std::any::Any + Send + Sync>> {
    // Catalog validation has run before service installation. The Operations
    // slice is independent; only link to it once its route is contributed.
    let modules = bilibi_module_registry::discover()?;
    let catalog = Catalog::discover(&modules)?;
    let operations_available = catalog.entries::<Route>(ROUTES)?.iter().any(|entry| {
        entry
            .get::<Route>()
            .is_ok_and(|route| route.path == "/integrations/people/operations")
    });
    Ok(Arc::new(PageService(Arc::new(Pages {
        operations_available,
        connector: services.get(crate::SERVICE)?,
        db: services.get::<Db>("base/database.pool")?.as_ref().clone(),
        display: services.get("base/datetime.display")?,
        locales: services.get("base/locale.locales")?,
        settings: services.get("base/settings.store")?,
    }))))
}
#[derive(Clone)]
struct Pages {
    operations_available: bool,
    connector: Arc<Connector>,
    db: Db,
    display: Arc<DisplayPolicy>,
    locales: Arc<bilibi_locale::Locales>,
    settings: Arc<bilibi_settings::Settings>,
}
struct Company {
    record: NamedOrganisation,
    status: crate::Result<Status>,
}
struct State {
    pages: Pages,
    companies: Vec<Company>,
    company: Option<i64>,
    choose_default_company: bool,
    can_manage: bool,
    summary: Option<SyncSummary>,
    webhook: Option<WebhookSummary>,
    pending_remove: Option<i64>,
    sync_keys: VecDeque<String>,
    columns: ListColumns,
    issues_columns: ListColumns,
    list: ListState,
    issues_list: ListState,
    display: DisplayContext,
    instance_banner: Html,
    flash: Option<(String, StatusKind)>,
    form_error: Option<(String, String)>,
}
fn list(
    query: &HashMap<String, String>,
    prefix: &str,
    columns: &[(&str, &str)],
) -> Result<ListState> {
    let spec = ListSpec {
        sortable: columns
            .iter()
            .map(|(id, _)| (id.to_string(), SortDir::Asc))
            .collect(),
        default_sort: Some(columns[0].0.into()),
        param_prefix: prefix.into(),
        page_sizes: if prefix.is_empty() {
            vec![10, 25, 50, 100]
        } else {
            vec![25, 50, 100, 300]
        },
        default_page_size: if prefix.is_empty() { 10 } else { 25 },
        omit_defaults: true,
        ..Default::default()
    };
    ListState::parse(query, spec.shared().map_err(|_| Error::Internal)?).map_err(|_| Error::Invalid)
}
const COMPANY_COLUMNS: &[(&str, &str)] = &[
    ("company", "Company"),
    ("state", "State"),
    ("provider", "Provider"),
    ("platform_company", "Platform company"),
    ("workforce_company", "Workforce company"),
    ("credential", "Credential"),
];
const ISSUE_COLUMNS: &[(&str, &str)] = &[
    ("issue", "Issue"),
    ("record", "Record"),
    ("seen", "Seen"),
    ("last_seen", "Last seen"),
];
fn columns(id: &str, fields: &[(&str, &str)]) -> Result<ListColumns> {
    ListColumns::mount(
        id,
        fields
            .iter()
            .map(|(key, label)| BuiltinColumn::new(key, label).sortable(key))
            .collect(),
    )
    .map_err(|_| Error::Internal)
}
impl State {
    async fn new(pages: Pages, scope: &Scope, query: &HashMap<String, String>) -> Result<Self> {
        let mut state = Self {
            pages,
            companies: vec![],
            company: None,
            choose_default_company: false,
            can_manage: false,
            summary: None,
            webhook: None,
            pending_remove: None,
            sync_keys: VecDeque::new(),
            columns: columns("people-connections-companies", COMPANY_COLUMNS)?,
            issues_columns: columns("people-connections-issues", ISSUE_COLUMNS)?,
            list: list(query, "", COMPANY_COLUMNS)?,
            issues_list: list(query, "issues_", ISSUE_COLUMNS)?,
            display: DisplayContext::anonymous(),
            instance_banner: Html::EMPTY,
            flash: None,
            form_error: None,
        };
        state.select(scope, query).await?;
        Ok(state)
    }
    fn mint_key(&mut self) {
        self.sync_keys
            .push_back(format!("page-{}", uuid::Uuid::new_v4()));
        if self.sync_keys.len() > 8 {
            self.sync_keys.pop_front();
        }
    }
    async fn select(&mut self, scope: &Scope, query: &HashMap<String, String>) -> Result<()> {
        self.list = list(query, "", COMPANY_COLUMNS)?;
        self.issues_list = list(query, "issues_", ISSUE_COLUMNS)?;
        self.company = params::positive_id(query.get("company_id").map(String::as_str));
        self.choose_default_company = !query.contains_key("company_id");
        self.pending_remove = None;
        self.form_error = None;
        self.sync_keys.clear();
        self.mint_key();
        self.refresh(scope).await
    }
    async fn refresh(&mut self, scope: &Scope) -> Result<()> {
        let client = self.pages.db.get().await.map_err(Error::from)?;
        let companies = bilibi_company::Directory
            .reachable(&**client, scope)
            .await?;
        let companies: Vec<_> = companies.into_iter().filter(|c| !c.archived).collect();
        let previous = self.company;
        // Default only on an address with no selection, never after reach changes.
        self.company = if self.choose_default_company {
            companies.first().map(|c| c.id.get())
        } else {
            self.company
                .filter(|id| companies.iter().any(|c| c.id.get() == *id))
        };
        self.choose_default_company = false;
        if previous != self.company {
            self.pending_remove = None;
            self.sync_keys.clear();
            self.mint_key();
        }
        self.can_manage = match self.company {
            Some(id) => crate::authorize(&**client, scope, id).await.is_ok(),
            None => false,
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
        self.instance_banner = bilibi_system::Instance::resolve(&self.pages.settings, &**client)
            .await?
            .banner("people-connections-instance", "mb-3")
            .map_err(|_| Error::Internal)?;
        drop(client);
        self.companies.clear();
        for record in companies {
            let status = self.pages.connector.status(scope, record.id.get()).await;
            self.companies.push(Company { record, status });
        }
        self.summary = None;
        self.webhook = None;
        if let Some(id) = self.company {
            if self.selected().is_some_and(|c| c.status.is_ok()) {
                self.summary = self.pages.connector.sync_summary(scope, id).await.ok();
                self.webhook = self.pages.connector.webhook_summary(scope, id).await.ok();
            }
        }
        self.clamp_pages();
        Ok(())
    }
    fn clamp_pages(&mut self) {
        let pages = (self.companies.len() as u64).div_ceil(self.list.page_size);
        self.list.clamp_to_last_page(pages, true);
        let pages = self
            .summary
            .as_ref()
            .map_or(0, |s| s.open_issues.len() as u64)
            .div_ceil(self.issues_list.page_size);
        self.issues_list.clamp_to_last_page(pages, true);
    }
    fn selected(&self) -> Option<&Company> {
        self.companies
            .iter()
            .find(|c| Some(c.record.id.get()) == self.company)
    }
    fn outcome<T>(&mut self, result: crate::Result<T>, message: &str) {
        self.flash = Some(match result {
            Ok(_) => (message.into(), StatusKind::Success),
            Err(error) => (sentence(&error), StatusKind::Error),
        });
    }
    async fn event(&mut self, scope: &Scope, payload: &serde_json::Value) -> Result<()> {
        let event = payload["type"].as_str().unwrap_or("");
        let values = params::string_values(payload);
        match event {
            "clear-flash" => {
                self.flash = None;
                return Ok(());
            }
            "grid" | "issues_grid" => {
                let (columns, list) = if event == "grid" {
                    (&mut self.columns, &mut self.list)
                } else {
                    (&mut self.issues_columns, &mut self.issues_list)
                };
                if let ColumnsOutcome::Sort(key) = columns.handle(&values) {
                    list.next_sort(&key);
                }
                return Ok(());
            }
            "page" | "issues_page" | "filters" | "issues_filters" => {
                let list = if event.starts_with("issues_") {
                    &mut self.issues_list
                } else {
                    &mut self.list
                };
                if event.ends_with("filters") {
                    list.apply_filters(&params::form_values(payload, "filters"));
                } else {
                    list.put_page(values.get("page").map(String::as_str));
                }
                self.clamp_pages();
                return Ok(());
            }
            "select_company" => {
                let mut query = self.query();
                query.extend(values);
                return self.select(scope, &query).await;
            }
            "cancel_remove" => {
                self.pending_remove = None;
                return Ok(());
            }
            _ => {}
        }
        if !WRITES.contains(&event) {
            return Ok(());
        }
        let Some(company) = self.company else {
            self.flash = Some(("Select a company you may manage.".into(), StatusKind::Error));
            return Ok(());
        };
        // Presentation flags are never authority: every write-shaped event,
        // including opening the dialog, proves the current grant and company reach.
        {
            let client = self.pages.db.get().await.map_err(Error::from)?;
            if crate::authorize(&**client, scope, company).await.is_err() {
                self.pending_remove = None;
                self.can_manage = false;
                self.outcome::<()>(Err(ConnectorError::Unauthorized), "");
                return Ok(());
            }
        }
        self.form_error = None;
        let connector = self.pages.connector.clone();
        match event {
            "configure" => self.outcome(
                connector
                    .configure_connection(
                        scope,
                        company,
                        values.get("provider_id").map(String::as_str).unwrap_or(""),
                    )
                    .await,
                "Connection saved. It stays disabled until you enable it.",
            ),
            "set_enabled" => {
                if let Some(enabled) = values.get("enabled").and_then(|s| s.parse::<bool>().ok()) {
                    self.outcome(
                        connector.set_enabled(scope, company, enabled).await,
                        if enabled {
                            "Connection enabled."
                        } else {
                            "Connection disabled."
                        },
                    );
                }
            }
            "save_credential" => {
                // Shared form coercion caps values at 1 KiB. Secret contracts allow
                // 4,096 bytes, so let the facade validate this exact field instead.
                if let Some(secret) = payload
                    .get("credential[secret]")
                    .and_then(serde_json::Value::as_str)
                {
                    if secret == MASK
                        && self
                            .selected()
                            .and_then(|c| c.status.as_ref().ok())
                            .is_some_and(|s| s.credential_stored)
                    {
                        self.flash =
                            Some(("The stored credential was kept.".into(), StatusKind::Info));
                    } else if secret.is_empty() {
                        self.outcome(
                            connector.clear_credential(scope, company).await,
                            "Credential cleared.",
                        );
                    } else {
                        self.outcome(
                            connector.put_credential(scope, company, secret).await,
                            "Credential saved.",
                        );
                    }
                }
            }
            "request_remove" => {
                if self
                    .selected()
                    .and_then(|c| c.status.as_ref().ok())
                    .is_some_and(|s| s.state != ConnectionState::Disconnected)
                {
                    self.pending_remove = Some(company);
                }
            }
            "remove" => {
                let pending = self.pending_remove.take();
                self.outcome(
                    if pending == Some(company) {
                        connector.remove_connection(scope, company).await
                    } else {
                        Err(ConnectorError::Conflict)
                    },
                    "Connection removed.",
                );
            }
            "synchronise" => {
                let key = values.get("key").map(String::as_str).unwrap_or("");
                if !self.sync_keys.iter().any(|issued| issued == key) {
                    self.outcome::<()>(Err(ConnectorError::InvalidIdempotencyKey), "");
                } else {
                    if self.sync_keys.back().is_some_and(|current| current == key) {
                        self.mint_key();
                    }
                    let full = values.get("full").is_some_and(|v| v == "true");
                    let key = if full {
                        format!("{key}:full")
                    } else {
                        key.to_owned()
                    };
                    match connector
                        .synchronise(
                            scope,
                            company,
                            &key,
                            SyncOptions {
                                full,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(run) => {
                            self.flash = Some((
                                format!("Synchronisation: {}.", run.state.label()),
                                if run.state == crate::RunState::Succeeded {
                                    StatusKind::Success
                                } else {
                                    StatusKind::Error
                                },
                            ))
                        }
                        Err(ConnectorError::Disconnected) => {
                            self.flash = Some((
                                "Enable the connection before synchronising.".into(),
                                StatusKind::Error,
                            ))
                        }
                        Err(error) => self.outcome::<()>(Err(error), ""),
                    }
                }
            }
            "resolve_issue" => {
                let result = match params::positive_id(values.get("id").map(String::as_str)) {
                    Some(id) => connector.resolve_issue(scope, company, id).await,
                    None => Err(ConnectorError::NotFound),
                };
                self.outcome(result, "Issue marked resolved.");
            }
            "save_policy" => {
                let form = params::form_values(payload, "policy");
                let integer = |key| {
                    form.get(key)
                        .and_then(|v| v.trim().parse().ok())
                        .ok_or(ConnectorError::InvalidPolicy("values"))
                };
                let parsed = (|| {
                    Ok(SyncPolicyUpdate {
                        page_limit: Some(integer("page_limit")?),
                        max_age_minutes: Some(integer("max_age_minutes")?),
                        run_timeout_minutes: Some(integer("run_timeout_minutes")?),
                    })
                })();
                let result = match parsed {
                    Ok(update) => connector.put_sync_policy(scope, company, update).await,
                    Err(error) => Err(error),
                };
                self.outcome(result, "Synchronisation policy saved.");
            }
            "save_webhook" => {
                let form = params::form_values(payload, "webhook");
                let parsed = (|| {
                    let enabled = form
                        .get("enabled")
                        .and_then(|s| s.parse().ok())
                        .ok_or(ConnectorError::InvalidWebhookSettings)?;
                    let skew = form
                        .get("max_skew_seconds")
                        .and_then(|s| s.parse().ok())
                        .ok_or(ConnectorError::InvalidWebhookSettings)?;
                    let secret = payload
                        .get("webhook[password]")
                        .and_then(serde_json::Value::as_str)
                        .ok_or(ConnectorError::InvalidWebhookSettings)?;
                    Ok(WebhookSettingsUpdate {
                        enabled: Some(enabled),
                        max_skew_seconds: Some(skew),
                        secret: (secret != MASK).then(|| secret.to_owned()),
                    })
                })();
                let result = match parsed {
                    Ok(update) => connector.put_webhook_settings(scope, company, update).await,
                    Err(error) => Err(error),
                };
                self.outcome(result, "Webhook settings saved.");
            }
            _ => {}
        }
        if matches!(
            event,
            "configure" | "save_policy" | "save_webhook" | "save_credential"
        ) {
            if let Some((message, StatusKind::Error)) = &self.flash {
                self.form_error = Some((event.into(), message.clone()));
            }
        }
        self.refresh(scope).await
    }
    fn query(&self) -> HashMap<String, String> {
        let mut query = self.list.to_params();
        query.extend(self.issues_list.to_params());
        query.insert(
            "company_id".into(),
            self.company.map(|id| id.to_string()).unwrap_or_default(),
        );
        query
    }
}
fn sentence(error: &ConnectorError) -> String {
    match error {
        ConnectorError::Failed(_) => {
            "Workforce identity is unavailable for this company. Try again later.".into()
        }
        _ => error.to_string(),
    }
}
fn render(template: impl Template) -> Result<Html> {
    Html::render(&template).map_err(|_| Error::Internal)
}
impl Page<Scope, Error> for Pages {
    fn title(&self, _: &str) -> Option<&'static str> {
        Some("People connections")
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
                LiveInput::Refresh => self.refresh(request.scope).await?,
            }
            Ok(LiveBody {
                html: self.render()?,
                query: self.query(),
                ..Default::default()
            })
        })
    }
}

impl State {
    fn render(&self) -> Result<String> {
        let mut selector = Field::named("company_id")
            .id("people-connections-company")
            .label("Company")
            .kind(InputKind::Select)
            .value(self.company.map(|id| id.to_string()).unwrap_or_default());
        selector.options = self
            .companies
            .iter()
            .map(|c| bilibi_ui::Choice::new(&c.record.name, c.record.id.get().to_string()))
            .collect();
        if self.company.is_none() {
            selector
                .options
                .insert(0, bilibi_ui::Choice::new("Select a company", ""));
        }
        let selector = if self.companies.is_empty() {
            Html::EMPTY
        } else {
            render(Form {
                id: "people-connections-company-form",
                event: "select_company",
                change: true,
                body: render(selector)?,
                ..Default::default()
            })?
        };
        let mut content = Html::EMPTY;
        if self.companies.is_empty() {
            content.push(&render(
                EmptyState::missing(
                    "No active company is available.",
                    "Choose a company you can view to see its connection state.",
                )
                .id("people-connections-no-company"),
            )?);
        } else {
            content.push(&self.companies_table()?);
        }
        if self.company.is_none() && !self.companies.is_empty() {
            content.push(&render(EmptyState::missing(
                "No company is selected.",
                "Choose a company you can view to see its connection state.",
            ))?);
        }
        if let Some(selected) = self.selected() {
            match &selected.status {
                Ok(status) => {
                    let mut body = self.connection_facts(selected, status)?;
                    if self.can_manage {
                        body.push(&render(ConnectionLinks {
                            company: selected.record.id.get(),
                            operations_available: self.pages.operations_available,
                        })?);
                        body.push(&self.provider_form(Some(status))?);
                        if self
                            .provider(status)
                            .is_some_and(|p| p.credential == Credential::Secret)
                        {
                            let mut secret = SecretInput::new(
                                "people-connections-credential",
                                "credential[secret]",
                                "credential",
                            )
                            .label("Provider credential")
                            .errors(self.field_errors("save_credential"));
                            if status.credential_stored {
                                secret = secret.stored();
                            }
                            body.push(&self.form(
                                "people-connections-credential-form",
                                "save_credential",
                                render(secret)?,
                                "people-connections-save-credential",
                                "Save credential",
                            )?);
                        }
                        if status.state != ConnectionState::Disconnected {
                            body.push(&render(ConnectionButtons {
                                enabled: status.state == ConnectionState::Enabled,
                            })?);
                        }
                    } else {
                        body.push(&render(if status.state == ConnectionState::Disconnected { EmptyState::missing("No workforce connection is configured.", "Ask someone who can manage People connections to choose a provider.").id("people-connections-disconnected") } else { EmptyState::forbidden("change People connections for this company").id("people-connections-read-only") })?);
                    }
                    content.push(&render(bilibi_ui::Card {
                        id: "people-connections-status",
                        body,
                        ..Default::default()
                    })?);
                    if status.state != ConnectionState::Disconnected {
                        if let Some(summary) = &self.summary {
                            content.push(&self.sync_section(status, summary)?);
                        }
                    }
                }
                Err(error) => {
                    let notice = if matches!(error, ConnectorError::MappingChanged) {
                        "This company now maps to a different workforce company. Choose the provider again to record the current mapping.".into()
                    } else {
                        sentence(error)
                    };
                    content.push(&render(
                        EmptyState::missing("Workforce information is unavailable.", &notice)
                            .id("people-connections-workforce-unavailable"),
                    )?);
                    if self.can_manage && matches!(error, ConnectorError::MappingChanged) {
                        content.push(&self.provider_form(None)?);
                    }
                }
            }
        }
        let dialog = if self.pending_remove == self.company && self.pending_remove.is_some() {
            let selected = self.selected().ok_or(Error::Internal)?;
            let consequence = format!(
                "The workforce connection for {} will be removed.",
                selected.record.name
            );
            let detail = if selected.status.as_ref().is_ok_and(|s| s.credential_stored) {
                "Its stored credential, signing secret, synchronised records and notification history are deleted. People records are not changed. You can connect again later."
            } else {
                "Its synchronised records, signing secret and notification history are deleted. People records are not changed. You can connect again later."
            };
            let mut body = self.instance_banner.clone();
            body.push(&render(ConfirmDialog {
                id: "people-connections-remove-confirm",
                consequence: &consequence,
                detail,
                confirm: "Remove",
                working: "Removing…",
                confirm_event: "remove",
                cancel_event: "cancel_remove",
            })?);
            body
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
        ConnectionsView {
            flash,
            selector,
            content,
            dialog,
        }
        .render()
        .map_err(|_| Error::Internal)
    }
    fn field_errors(&self, event: &str) -> Vec<String> {
        self.form_error
            .as_ref()
            .filter(|(action, _)| action == event)
            .map(|(_, message)| vec![message.clone()])
            .unwrap_or_default()
    }
    fn form(
        &self,
        id: &str,
        event: &str,
        body: Html,
        button_id: &str,
        label: &str,
    ) -> Result<Html> {
        let mut prefix = Html::EMPTY;
        if let Some((action, message)) = &self.form_error {
            if action == event {
                let target = match event {
                    "configure" => "people-connections-provider-select",
                    "save_policy" => "people-connections-policy-page_limit",
                    "save_webhook" => "people-connections-webhook-skew",
                    _ => "people-connections-credential",
                };
                let mut summary = bilibi_ui::ErrorSummary::new(format!("{id}-errors"));
                summary.push(target, message);
                prefix = render(summary)?;
            }
        }
        prefix.push(&form(id, event, body, button_id, label)?);
        Ok(prefix)
    }
    fn provider<'a>(&'a self, status: &Status) -> Option<&'a crate::Provider> {
        status
            .provider_id
            .as_deref()
            .and_then(|id| self.pages.connector.registry().fetch(id).ok())
    }
    fn provider_label(&self, status: &Status) -> String {
        self.provider(status)
            .map(|p| p.name.clone())
            .unwrap_or_else(|| {
                status
                    .provider_id
                    .as_ref()
                    .map(|id| format!("Not installed ({id})"))
                    .unwrap_or_else(|| "None chosen".into())
            })
    }
    fn credential_label(&self, status: &Status) -> &'static str {
        if status.provider_id.is_none() {
            return "—";
        }
        if self
            .provider(status)
            .is_some_and(|p| p.credential == Credential::None)
        {
            "Not required"
        } else if status.credential_stored {
            "Stored"
        } else {
            "Missing"
        }
    }
    fn connection_facts(&self, company: &Company, status: &Status) -> Result<Html> {
        let mut items = vec![
            Fact {
                title: "State",
                id: "people-connections-state",
                value: badge(
                    if status.state == ConnectionState::Enabled {
                        BadgeKind::Success
                    } else {
                        BadgeKind::Neutral
                    },
                    status.state.label(),
                )?,
            },
            Fact {
                title: "Provider",
                id: "people-connections-provider",
                value: Html::text(&self.provider_label(status)),
            },
            Fact {
                title: "Platform company",
                id: "people-connections-platform-company",
                value: Html::text(&company.record.name),
            },
            Fact {
                title: "Workforce company",
                id: "people-connections-workforce-company",
                value: Html::text(&format!(
                    "{} · {}",
                    status.workforce_source_id, status.workforce_company_id
                )),
            },
        ];
        if self.provider(status).is_some() {
            items.push(Fact {
                title: "Credential",
                id: "people-connections-credential-state",
                value: Html::text(self.credential_label(status)),
            });
        }
        render(Facts {
            id: "people-connections-facts",
            items,
        })
    }
    fn companies_table(&self) -> Result<Html> {
        let mut rows = Vec::new();
        for company in &self.companies {
            let id = company.record.id.get().to_string();
            let mut row = FlexRow::new(&id)
                .cell("company", Cell::text(&company.record.name))
                .cell(
                    "platform_company",
                    Cell::text(format!("{} · {}", company.record.name, id)),
                );
            let sort = match &company.status {
                Ok(status) => {
                    row = row
                        .slot(
                            "state",
                            badge(
                                if status.state == ConnectionState::Enabled {
                                    BadgeKind::Success
                                } else {
                                    BadgeKind::Neutral
                                },
                                status.state.label(),
                            )?,
                        )
                        .cell("provider", Cell::text(self.provider_label(status)))
                        .cell(
                            "workforce_company",
                            Cell::text(format!(
                                "{} · {}",
                                status.workforce_source_id, status.workforce_company_id
                            )),
                        )
                        .cell("credential", Cell::text(self.credential_label(status)));
                    match self.list.sort_by.as_deref() {
                        Some("state") => status.state.label().into(),
                        Some("provider") => self.provider_label(status),
                        Some("platform_company") => format!("{:020}", status.platform_company_id),
                        Some("workforce_company") => format!(
                            "{} {:020}",
                            status.workforce_source_id, status.workforce_company_id
                        ),
                        Some("credential") => self.credential_label(status).into(),
                        _ => company.record.name.to_lowercase(),
                    }
                }
                Err(error) => {
                    row = row
                        .cell("state", Cell::text(sentence(error)))
                        .cell("provider", Cell::text("—"))
                        .cell("workforce_company", Cell::text("—"))
                        .cell("credential", Cell::text("—"));
                    company.record.name.to_lowercase()
                }
            };
            row.actions = render(Button {
                event: "select_company",
                name: "company_id",
                value: &id,
                body: Html::text(if Some(company.record.id.get()) == self.company {
                    "Selected"
                } else {
                    "Select"
                }),
                disabled: Some(company.record.id.get()) == self.company,
                ..Default::default()
            })?;
            rows.push((sort, row));
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        if self.list.sort_dir == Some(SortDir::Desc) {
            rows.reverse();
        }
        table(
            "people-connections-companies",
            "Company connections",
            &self.columns,
            &self.list,
            "grid",
            rows.into_iter().map(|(_, row)| row).collect(),
            "No active company is available.",
        )
    }
    fn provider_form(&self, status: Option<&Status>) -> Result<Html> {
        let field = Field::named("provider_id")
            .id("people-connections-provider-select")
            .label("Provider")
            .wrapper_class("mb-0")
            .kind(InputKind::Select)
            .value(
                status
                    .and_then(|s| s.provider_id.clone())
                    .unwrap_or_default(),
            )
            .errors(self.field_errors("configure"))
            .options(
                self.pages
                    .connector
                    .registry()
                    .providers()
                    .iter()
                    .map(|p| bilibi_ui::Choice::new(&p.name, &p.id))
                    .collect(),
            );
        self.form(
            "people-connections-provider-form",
            "configure",
            render(field)?,
            "people-connections-save-provider",
            if status.is_some_and(|s| s.state != ConnectionState::Disconnected) {
                "Save provider"
            } else {
                "Connect"
            },
        )
    }
    fn date(&self, id: &str, value: Option<jiff::Timestamp>, empty: &str) -> Result<Html> {
        match value {
            Some(value) => {
                let date = self
                    .display
                    .render(value, Format::DateTime, Precision::Minute);
                render(DateTime {
                    id,
                    instant: Instant::new(
                        &date.iso,
                        &date.text,
                        &date.company,
                        &date.utc,
                        date.mode.as_str(),
                    ),
                })
            }
            None => Ok(Html::text(empty)),
        }
    }
    fn sync_section(&self, status: &Status, summary: &SyncSummary) -> Result<Html> {
        let (label, kind) = match &summary.freshness {
            Freshness::Current => ("Current", BadgeKind::Success),
            Freshness::Stale(_) => ("Stale", BadgeKind::Warning),
            Freshness::Unavailable(reason) if reason == "never_synchronised" => {
                ("Never synchronised", BadgeKind::Neutral)
            }
            _ => ("Not enabled", BadgeKind::Neutral),
        };
        let last = match &summary.last_run {
            None => Html::text("None yet"),
            Some(run) => {
                let mut value = badge(
                    match run.state {
                        crate::RunState::Succeeded => BadgeKind::Success,
                        crate::RunState::Running => BadgeKind::Neutral,
                        crate::RunState::Stale | crate::RunState::Unknown => BadgeKind::Warning,
                        _ => BadgeKind::Danger,
                    },
                    run.state.label(),
                )?;
                value.push(&Html::text(&format!(
                    " {} · {} applied · {} unchanged · {} deactivated · {} refused",
                    if run.pass == crate::RunPass::Bootstrap {
                        "Full read"
                    } else {
                        "Changes"
                    },
                    run.applied,
                    run.unchanged,
                    run.deactivated,
                    run.refused
                )));
                value
            }
        };
        let facts = render(Facts {
            id: "people-connections-sync-facts",
            items: vec![
                Fact {
                    title: "Freshness",
                    id: "people-connections-freshness",
                    value: badge(kind, label)?,
                },
                Fact {
                    title: "Data as of",
                    id: "people-connections-as-of",
                    value: self.date(
                        "people-connections-as-of-value",
                        summary.as_of_at,
                        "No completed pass",
                    )?,
                },
                Fact {
                    title: "Last pass",
                    id: "people-connections-last-run",
                    value: last,
                },
            ],
        })?;
        let mut actions = Html::EMPTY;
        if self.can_manage && self.pages.connector.people_available() {
            actions = render(SyncButtons {
                key: self.sync_keys.back().map(String::as_str).unwrap_or(""),
                disabled: status.state != ConnectionState::Enabled
                    || !self
                        .pages
                        .connector
                        .adapters()
                        .serves(status.provider_id.as_deref().unwrap_or("")),
            })?;
            if !self
                .pages
                .connector
                .adapters()
                .serves(status.provider_id.as_deref().unwrap_or(""))
            {
                actions.push(&render(EmptyState::missing(
                    "Synchronisation is unavailable.",
                    "No adapter serves this provider yet, so it cannot be synchronised.",
                ))?);
            }
        }
        let mut issue_rows = Vec::new();
        for issue in &summary.open_issues {
            let subject = match (&issue.record_kind, &issue.stable_id) {
                (Some(kind), Some(id)) => format!("{kind} {id}"),
                (Some(kind), _) => kind.clone(),
                _ => "—".into(),
            };
            let mut message = badge(
                if issue.severity == "error" {
                    BadgeKind::Danger
                } else {
                    BadgeKind::Warning
                },
                if issue.severity == "error" {
                    "Error"
                } else {
                    "Warning"
                },
            )?;
            message.push(&Html::text(&format!(" {}", issue.message())));
            let mut row = FlexRow::new(format!("people-connections-issue-{}", issue.id))
                .slot("issue", message)
                .cell("record", Cell::text(&subject))
                .cell("seen", Cell::text(issue.occurrences.to_string()))
                .slot(
                    "last_seen",
                    self.date(
                        &format!("people-connections-issue-{}-seen", issue.id),
                        Some(issue.last_seen_at),
                        "",
                    )?,
                );
            if self.can_manage {
                row.actions = render(Button {
                    id: &format!("people-connections-resolve-{}", issue.id),
                    event: "resolve_issue",
                    name: "id",
                    value: &issue.id.to_string(),
                    body: Html::text("Mark resolved"),
                    ..Default::default()
                })?;
            }
            let sort = match self.issues_list.sort_by.as_deref() {
                Some("record") => subject,
                Some("seen") => format!("{:020}", issue.occurrences),
                Some("last_seen") => issue.last_seen_at.to_string(),
                _ => issue.message().into(),
            };
            issue_rows.push((sort, row));
        }
        issue_rows.sort_by(|a, b| a.0.cmp(&b.0));
        if self.issues_list.sort_dir == Some(SortDir::Desc) {
            issue_rows.reverse();
        }
        let issues = table(
            "people-connections-issues",
            "Open reconciliation issues",
            &self.issues_columns,
            &self.issues_list,
            "issues_grid",
            issue_rows.into_iter().map(|(_, row)| row).collect(),
            "No open issues.",
        )?;
        let policy = if self.can_manage {
            let mut body = Html::EMPTY;
            for (name, label, hint, value, min, max) in [
                (
                    "page_limit",
                    "Page size",
                    "Most records the provider returns on one page.",
                    summary.policy.page_limit,
                    1,
                    1000,
                ),
                (
                    "max_age_minutes",
                    "Maximum age (minutes)",
                    "After this long without a completed pass, records are stale.",
                    summary.policy.max_age_minutes,
                    5,
                    43200,
                ),
                (
                    "run_timeout_minutes",
                    "Run timeout (minutes)",
                    "A pass still running after this is recorded as unknown.",
                    summary.policy.run_timeout_minutes,
                    1,
                    1440,
                ),
            ] {
                body.push(&render(
                    Field::in_form("policy", name)
                        .wrapper_class("mb-0")
                        .id(format!("people-connections-policy-{name}"))
                        .label(label)
                        .hint(hint)
                        .kind(InputKind::Number)
                        .value(value.to_string())
                        .attr("min", &min.to_string())
                        .attr("max", &max.to_string())
                        .attr("required", "")
                        .errors(if name == "page_limit" {
                            self.field_errors("save_policy")
                        } else {
                            vec![]
                        }),
                )?);
            }
            self.form(
                "people-connections-policy-form",
                "save_policy",
                render(PolicyFieldsView { fields: body })?,
                "people-connections-save-policy",
                "Save policy",
            )?
        } else {
            Html::EMPTY
        };
        let webhook = match &self.webhook {
            Some(webhook) => self.webhook_section(status, webhook)?,
            None => Html::EMPTY,
        };
        render(SyncView {
            facts,
            actions,
            issues,
            issue_count: summary.open_issues.len(),
            policy,
            webhook,
        })
    }
    fn webhook_section(&self, status: &Status, webhook: &WebhookSummary) -> Result<Html> {
        let facts = render(Facts {
            id: "people-connections-webhook-facts",
            items: vec![
                Fact {
                    title: "Intake",
                    id: "people-connections-webhook-state",
                    value: Html::text(
                        if webhook.enabled && status.state == ConnectionState::Enabled {
                            "Enabled"
                        } else {
                            "Disabled"
                        },
                    ),
                },
                Fact {
                    title: "Signing secret",
                    id: "people-connections-webhook-secret-state",
                    value: Html::text(if webhook.secret_stored {
                        "Stored"
                    } else {
                        "Not stored"
                    }),
                },
                Fact {
                    title: "Last directory-change notification",
                    id: "people-connections-webhook-received",
                    value: self.date(
                        "people-connections-webhook-received-value",
                        webhook.last_received_at,
                        "None yet",
                    )?,
                },
            ],
        })?;
        let policy = if self.can_manage {
            let mut body = render(
                Field::in_form("webhook", "enabled")
                    .id("people-connections-webhook-enabled")
                    .label("Inbound intake")
                    .hint("Receiving notifications also requires an enabled workforce connection.")
                    .kind(InputKind::Select)
                    .value(webhook.enabled.to_string())
                    .options(vec![
                        bilibi_ui::Choice::new("Disabled", "false"),
                        bilibi_ui::Choice::new("Enabled", "true"),
                    ]),
            )?;
            let mut secret = SecretInput::new(
                "people-connections-webhook-secret",
                "webhook[password]",
                "webhook signing secret",
            )
            .label("Signing secret")
            .no_reveal();
            if webhook.secret_stored {
                secret = secret.stored();
            }
            body.push(&render(secret)?);
            body.push(&render(
                Field::in_form("webhook", "max_skew_seconds")
                    .id("people-connections-webhook-skew")
                    .label("Allowed clock difference (seconds)")
                    .kind(InputKind::Number)
                    .value(webhook.max_skew_seconds.to_string())
                    .attr("min", "1")
                    .attr("max", "86400")
                    .attr("required", "")
                    .errors(self.field_errors("save_webhook")),
            )?);
            self.form(
                "people-connections-webhook-form",
                "save_webhook",
                body,
                "people-connections-webhook-save",
                "Save webhook settings",
            )?
        } else {
            Html::EMPTY
        };
        render(WebhookView { facts, policy })
    }
}
fn badge(kind: BadgeKind, label: &str) -> Result<Html> {
    render(Badge::status(kind, label))
}
fn form(id: &str, event: &str, body: Html, button_id: &str, label: &str) -> Result<Html> {
    render(Form {
        id,
        event,
        class: if event == "configure" {
            "flex flex-wrap items-end gap-3"
        } else {
            ""
        },
        body,
        actions: render(Button {
            id: button_id,
            button_type: "submit",
            body: Html::text(label),
            working: "Saving…",
            ..Default::default()
        })?,
        ..Default::default()
    })
}
fn table(
    id: &str,
    caption: &str,
    columns: &ListColumns,
    list: &ListState,
    event: &str,
    rows: Vec<FlexRow>,
    empty: &str,
) -> Result<Html> {
    let counts = PageCounts::new(list.page, list.page_size, rows.len() as u64);
    let rows = rows
        .into_iter()
        .skip(((counts.page - 1) * counts.page_size) as usize)
        .take(counts.page_size as usize)
        .collect();
    let mut table = FlexTable::new(id, columns.flex_columns(), rows)
        .sorted(list.sort_by.as_deref(), list.sort_dir);
    table.zoom = columns.zoom;
    table.suggestions = columns.flex_suggestions();
    table.add_query = &columns.add_query;
    table.event = event;
    table.caption = caption;
    table.actions = true;
    table.empty = Some(TableEmpty::Content(Html::text(empty)));
    let mut html = render(table)?;
    if counts.total_pages > 1 {
        let pager_id = format!("{id}-pagination");
        let mut pager = Pagination::new(&pager_id, counts, &list.spec().page_sizes);
        pager.page_event = if event == "grid" {
            "page"
        } else {
            "issues_page"
        };
        pager.filters_event = if event == "grid" {
            "filters"
        } else {
            "issues_filters"
        };
        html.push(&render(pager)?);
    }
    Ok(html)
}
#[derive(Template)]
#[template(path = "connections.html")]
struct ConnectionsView<'a> {
    flash: FlashMap<'a>,
    selector: Html,
    content: Html,
    dialog: Html,
}
#[derive(Template)]
#[template(path = "connections_sync.html")]
struct SyncView {
    facts: Html,
    actions: Html,
    issues: Html,
    issue_count: usize,
    policy: Html,
    webhook: Html,
}
#[derive(Template)]
#[template(path = "connections_webhook.html")]
struct WebhookView {
    facts: Html,
    policy: Html,
}
#[derive(Template)]
#[template(path = "connections_buttons.html")]
struct ConnectionButtons {
    enabled: bool,
}
#[derive(Template)]
#[template(path = "connections_sync_buttons.html")]
struct SyncButtons<'a> {
    key: &'a str,
    disabled: bool,
}
#[derive(Template)]
#[template(path = "connections_links.html")]
struct ConnectionLinks {
    company: i64,
    operations_available: bool,
}

#[derive(Template)]
#[template(
    source = "{% import \"flex_table.html\" as flex %}{% call flex::instant(id=id, i=instant) %}{% endcall %}",
    ext = "html"
)]
struct DateTime<'a> {
    id: &'a str,
    instant: Instant,
}

#[derive(Template)]
#[template(
    source = "<div class=\"grid gap-3 sm:grid-cols-3\">{{ fields }}</div>",
    ext = "html"
)]
struct PolicyFieldsView {
    fields: Html,
}
