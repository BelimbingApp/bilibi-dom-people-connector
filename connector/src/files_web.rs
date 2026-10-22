//! Bilimbi Connector.Web.FilesLive and FilesController; server-owned uploads and confirmation.
use crate::{Connector, FilePolicy, FileSummary, MANAGE};
use askama::Template;
use bilibi_database::{Db, Error, Result};
use bilibi_datetime::{DisplayContext, DisplayPolicy, Format, Precision};
use bilibi_module_registry::{
    contributions::{
        Access, Download as DownloadRoute, Method, PageWrites, Pending, Route, DOWNLOADS,
        PAGE_WRITES, ROUTES,
    },
    Contribution, Provenance, Services,
};
use bilibi_tenancy::{NamedOrganisation, OrganisationDirectory, Scope};
use bilibi_ui::{
    download::{Download, DownloadFuture, DownloadRequest, DownloadService, FileBody},
    page::{
        LiveBody, LiveFuture, LiveInput, LiveRequest, LiveState, MountFuture, Page, PageBody,
        PageFuture, PageRequest, PageService,
    },
    params, BuiltinColumn, Cell, Choice, ConfirmDialog, EmptyState, Field, FlashMap, FlexRow,
    FlexTable, Form, Html, InputKind, Instant, ListColumns, ListSpec, ListState, TableEmpty,
    UploadSpec, Uploads,
};
use std::{collections::HashMap, sync::Arc};
const PAGE_SERVICE: &str = "people_connector/connector.files_pages";
const DOWNLOAD_SERVICE: &str = "people_connector/connector.files_download";
pub(crate) fn pages_service(
    services: &Services,
) -> bilibi_module_registry::Result<Arc<dyn std::any::Any + Send + Sync>> {
    Ok(Arc::new(PageService(Arc::new(Pages::new(services)?))))
}
pub(crate) fn download_service(
    services: &Services,
) -> bilibi_module_registry::Result<Arc<dyn std::any::Any + Send + Sync>> {
    Ok(Arc::new(DownloadService(Arc::new(Pages::new(services)?))))
}
pub(crate) const PAGE_ID: &str = PAGE_SERVICE;
pub(crate) const DOWNLOAD_ID: &str = DOWNLOAD_SERVICE;
macro_rules! provenance {
    () => {
        Provenance {
            module: crate::MODULE,
            source: file!(),
            line: line!(),
        }
    };
}
inventory::submit! {Contribution::new(ROUTES,"people_connector/connector.files",provenance!(),&Route{method:Method::Get,path:"/integrations/people/files",service:PAGE_SERVICE,operation:"files",access:Access::Capability(MANAGE),framed:true})}
inventory::submit! {Contribution::new(DOWNLOADS,"people_connector/connector.file_download",provenance!(),&DownloadRoute{path:"/integrations/people/files/{company_id}/{id}",service:DOWNLOAD_SERVICE,operation:"download",access:Access::Capability(MANAGE)})}
inventory::submit! {Contribution::new(PAGE_WRITES,PAGE_SERVICE,provenance!(),&PageWrites{service:PAGE_SERVICE,contracts:&[],pending:Pending::Events{events:&["save_policy","import","export","purge"],reason:"Bilimbi file exchange facade writes; operation contracts are a separate port slice."}})}
#[derive(Clone)]
struct Pages {
    connector: Arc<Connector>,
    db: Db,
    display: Arc<DisplayPolicy>,
    locales: Arc<bilibi_locale::Locales>,
}
impl Pages {
    fn new(services: &Services) -> bilibi_module_registry::Result<Self> {
        Ok(Self {
            connector: services.get(crate::SERVICE)?,
            db: services.get::<Db>("base/database.pool")?.as_ref().clone(),
            display: services.get("base/datetime.display")?,
            locales: services.get("base/locale.locales")?,
        })
    }
}
struct State {
    pages: Pages,
    company: Option<i64>,
    companies: Vec<NamedOrganisation>,
    summary: Option<FileSummary>,
    uploads: Uploads,
    pending_purge: bool,
    columns: ListColumns,
    list: ListState,
    flash: Option<(String, bool)>,
    private_settings: bool,
    replayed: bool,
    display: DisplayContext,
}
impl State {
    async fn new(pages: Pages, scope: &Scope, query: &HashMap<String, String>) -> Result<Self> {
        let columns = ListColumns::mount(
            "people-files-history",
            vec![
                BuiltinColumn::new("direction", "Direction"),
                BuiltinColumn::new("record_count", "Records"),
                BuiltinColumn::new("state", "State"),
                BuiltinColumn::new("inserted_at", "Created"),
            ],
        )
        .map_err(|_| Error::Internal)?;
        let list = ListState::parse(
            query,
            ListSpec::default().shared().map_err(|_| Error::Internal)?,
        )
        .map_err(|_| Error::Invalid)?;
        let mut state = Self {
            pages,
            company: None,
            companies: vec![],
            summary: None,
            uploads: Uploads::new([UploadSpec::new("directory", 10_485_760).accept([".json"])]),
            pending_purge: false,
            columns,
            list,
            flash: None,
            private_settings: false,
            replayed: false,
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
        self.company = params::positive_id(query.get("company_id").map(String::as_str))
            .filter(|id| self.companies.iter().any(|c| c.id.get() == *id))
            .or_else(|| self.companies.first().map(|c| c.id.get()));
        self.private_settings =
            bilibi_authz::decide(&**client, scope, "base.settings.global.manage")
                .await?
                .allowed
                && bilibi_authz::decide(&**client, scope, "admin.system.artifacts.manage")
                    .await?
                    .allowed;
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
        self.pending_purge = false;
        self.uploads = Uploads::new([UploadSpec::new("directory", 10_485_760).accept([".json"])]);
        drop(client);
        self.refresh(scope).await;
        Ok(())
    }
    async fn refresh(&mut self, scope: &Scope) {
        self.summary = match self.company {
            Some(company) => self.pages.connector.file_summary(scope, company).await.ok(),
            None => None,
        };
    }
    fn outcome<T>(&mut self, result: crate::Result<T>, message: &str) {
        self.replayed = false;
        self.flash=Some(match result{Ok(_)=>(message.to_owned(),true),Err(_)=>("File exchange refused. Check the connection, file policy and private storage settings.".into(),false)});
    }
    async fn event(&mut self, scope: &Scope, payload: &serde_json::Value) -> Result<()> {
        let event = payload["type"].as_str().unwrap_or("");
        if event == "clear-flash" {
            self.flash = None;
            return Ok(());
        }
        if event == "grid" {
            self.columns.handle(&params::string_values(payload));
            return Ok(());
        }
        if event == "select_company" {
            let values = params::string_values(payload);
            self.select(scope, &values).await?;
            return Ok(());
        }
        if event == "validate" || event == "cancel_purge" {
            if event == "cancel_purge" {
                self.pending_purge = false;
            }
            return Ok(());
        }
        let Some(company) = self.company else {
            self.flash = Some(("No company is available for file exchange.".into(), false));
            return Ok(());
        };
        // Recheck every write-shaped event, including opening the confirmation.
        {
            let client = self.pages.db.get().await.map_err(Error::from)?;
            if crate::authorize(&**client, scope, company).await.is_err() {
                self.pending_purge = false;
                self.flash=Some(("You cannot change connections for this company. Ask an administrator for access.".into(),false));
                return Ok(());
            }
        }
        match event {
            "save_policy" => {
                let values = params::form_values(payload, "policy");
                let integer = |key| {
                    values
                        .get(key)
                        .and_then(|v| v.parse().ok())
                        .ok_or(crate::ConnectorError::InvalidFile)
                };
                let policy = (|| {
                    Ok(FilePolicy {
                        enabled: match values.get("enabled").map(String::as_str) {
                            Some("true") => true,
                            Some("false") => false,
                            _ => return Err(crate::ConnectorError::InvalidFile),
                        },
                        json_enabled: match values.get("json_enabled").map(String::as_str) {
                            Some("true") => true,
                            Some("false") => false,
                            _ => return Err(crate::ConnectorError::InvalidFile),
                        },
                        max_bytes: integer("max_bytes")?,
                        max_records: integer("max_records")?,
                        stale_minutes: integer("stale_minutes")?,
                    })
                })();
                let result = match policy {
                    Ok(policy) => {
                        self.pages
                            .connector
                            .put_file_policy(scope, company, policy)
                            .await
                    }
                    Err(error) => Err(error),
                };
                self.outcome(result, "File policy saved.");
            }
            "import" => {
                let files = self.uploads.consume("directory");
                let result = match files.as_slice() {
                    [file] => {
                        self.pages
                            .connector
                            .import_file(scope, company, &file.bytes)
                            .await
                    }
                    _ => Err(crate::ConnectorError::MissingFile),
                };
                self.file_outcome(result, "Directory file imported for review.");
            }
            "export" => {
                let result = self.pages.connector.export_file(scope, company).await;
                self.file_outcome(
                    result,
                    "Directory file exported. Download it from the exchange history.",
                );
            }
            "request_purge" => self.pending_purge = true,
            "purge" if self.pending_purge => {
                self.pending_purge = false;
                let result = self
                    .pages
                    .connector
                    .purge_expired_files(scope, company)
                    .await
                    .and_then(|r| {
                        if r.failed.is_empty() {
                            Ok(r)
                        } else {
                            Err(crate::ConnectorError::PurgeIncomplete)
                        }
                    });
                self.outcome(result, "Expired files removed.");
            }
            _ => {}
        }
        self.refresh(scope).await;
        Ok(())
    }
    fn file_outcome(&mut self, result: crate::Result<crate::FileExchange>, message: &str) {
        match result {
            Ok(exchange) if exchange.replayed => {
                self.replayed = true;
                self.flash = Some(("This file is already recorded.".into(), true))
            }
            result => self.outcome(result, message),
        }
    }
    fn query(&self) -> HashMap<String, String> {
        let mut query = self.list.to_params();
        if let Some(company) = self.company {
            query.insert("company_id".into(), company.to_string());
        }
        query
    }
    fn render(&self) -> Result<String> {
        let mut selector = Field::named("company_id")
            .id("people-files-company")
            .label("Company")
            .kind(InputKind::Select)
            .value(self.company.map(|id| id.to_string()).unwrap_or_default());
        selector.options = self
            .companies
            .iter()
            .map(|c| Choice::new(c.name.clone(), c.id.get().to_string()))
            .collect();
        let selector = if self.companies.is_empty() {
            Html::EMPTY
        } else {
            render(Form {
                id: "people-files-company-form",
                event: "select_company",
                change: true,
                body: render(selector)?,
                ..Default::default()
            })?
        };
        let mut content = Html::EMPTY;
        if let Some(summary) = &self.summary {
            let p = summary.policy;
            let mut fields = Vec::new();
            for (key, label, value) in [
                ("enabled", "File exchange", p.enabled),
                ("json_enabled", "Directory JSON format", p.json_enabled),
            ] {
                fields.push(
                    Field::in_form("policy", key)
                        .kind(InputKind::Select)
                        .label(label)
                        .value(value.to_string())
                        .options(vec![
                            Choice::new("Disabled", "false"),
                            Choice::new("Enabled", "true"),
                        ]),
                );
            }
            for (key, label, value, max) in [
                ("max_bytes", "Maximum file bytes", p.max_bytes, 10_485_760),
                (
                    "max_records",
                    "Maximum directory records",
                    p.max_records,
                    100_000,
                ),
                (
                    "stale_minutes",
                    "Abandon in-progress exchanges after (minutes)",
                    p.stale_minutes,
                    1440,
                ),
            ] {
                fields.push(
                    Field::in_form("policy", key)
                        .kind(InputKind::Number)
                        .label(label)
                        .value(value.to_string())
                        .attr("min", "1")
                        .attr("max", &max.to_string())
                        .attr("required", ""),
                );
            }
            let policy = render(Form {
                id: "people-files-policy",
                event: "save_policy",
                body: render(PolicyFields { fields: &fields })?,
                actions: render(SaveButton)?,
                ..Default::default()
            })?;
            let import = if self.pages.connector.people_available() {
                render(Form {
                    id: "people-files-import",
                    event: "import",
                    body: render(
                        self.uploads
                            .input("directory", "people-files-directory")
                            .label("Directory JSON file"),
                    )?,
                    actions: render(ImportButton {
                        disabled: !p.enabled
                            || !p.json_enabled
                            || !self.pages.connector.people_available(),
                    })?,
                    ..Default::default()
                })?
            } else {
                Html::EMPTY
            };
            let mut rows = Vec::new();
            for receipt in &summary.records {
                let mut row = FlexRow::new(&receipt.id)
                    .cell(
                        "direction",
                        Cell::text(if receipt.direction == "import" {
                            "Import for review"
                        } else {
                            "Export"
                        }),
                    )
                    .cell("record_count", Cell::text(receipt.record_count.to_string()))
                    .cell("state", Cell::text(receipt.state_label()));
                if let Ok(timestamp) = receipt.inserted_at.parse() {
                    let date = self
                        .display
                        .render(timestamp, Format::DateTime, Precision::Minute);
                    row = row.cell(
                        "inserted_at",
                        Cell::instant(Instant::new(
                            &date.iso,
                            &date.text,
                            &date.company,
                            &date.utc,
                            date.mode.as_str(),
                        )),
                    );
                }
                if receipt.state == "ready" && self.pages.connector.people_available() {
                    row.actions = render(DownloadLink {
                        company: self.company.unwrap(),
                        id: &receipt.id,
                    })?;
                }
                rows.push(row);
            }
            let mut table =
                FlexTable::new("people-files-history", self.columns.flex_columns(), rows);
            table.zoom = self.columns.zoom;
            table.suggestions = self.columns.flex_suggestions();
            table.add_query = &self.columns.add_query;
            table.actions = true;
            table.caption = "Recent file exchanges";
            table.empty = Some(TableEmpty::Content(Html::text("No files exchanged.")));
            content = render(Directory {
                policy,
                import,
                history: render(table)?,
                private_settings: self.private_settings,
                disabled: !p.enabled || !p.json_enabled || !self.pages.connector.people_available(),
                people_available: self.pages.connector.people_available(),
            })?;
        } else {
            content.push(&render(
                EmptyState::missing(
                    "File exchange is unavailable.",
                    "A connection manager must select an accessible company.",
                )
                .id("people-files-unavailable"),
            )?);
        }
        let dialog = if self.pending_purge {
            render(ConfirmDialog {
                id: "people-files-purge-confirm",
                consequence: "Expired directory files for this company will be removed.",
                detail: "Exchange receipts remain as audit history. Expired files are already unavailable for download.",
                confirm: "Remove",
                working: "Removing…",
                confirm_event: "purge",
                cancel_event: "cancel_purge",
            })?
        } else {
            Html::EMPTY
        };
        let mut flash = FlashMap::default();
        if let Some((message, success)) = &self.flash {
            if self.replayed {
                flash.info = message;
            } else if *success {
                flash.success = message;
            } else {
                flash.error = message;
            }
        }
        FilesView {
            selector,
            content,
            dialog,
            flash,
        }
        .render()
        .map_err(|_| Error::Internal)
    }
}
fn render(template: impl Template) -> Result<Html> {
    Html::render(&template).map_err(|_| Error::Internal)
}
#[derive(Template)]
#[template(path = "files.html")]
struct FilesView<'a> {
    selector: Html,
    content: Html,
    dialog: Html,
    flash: FlashMap<'a>,
}
#[derive(Template)]
#[template(path = "files_directory.html")]
struct Directory {
    policy: Html,
    import: Html,
    history: Html,
    private_settings: bool,
    disabled: bool,
    people_available: bool,
}
#[derive(Template)]
#[template(
    source = "{% import \"forms.html\" as forms %}{% for field in fields %}{% call forms::input(field=field) %}{% endcall %}{% endfor %}",
    ext = "html"
)]
struct PolicyFields<'a> {
    fields: &'a [Field],
}
#[derive(Template)]
#[template(
    source = "{% import \"components.html\" as ui %}{% call ui::button(id=\"people-files-save\",button_type=\"submit\",working=\"Saving…\") %}Save file policy{% endcall %}",
    ext = "html"
)]
struct SaveButton;
#[derive(Template)]
#[template(
    source = "{% import \"components.html\" as ui %}{% call ui::button(button_type=\"submit\",disabled=disabled,working=\"Importing…\") %}Import for review{% endcall %}",
    ext = "html"
)]
struct ImportButton {
    disabled: bool,
}
#[derive(Template)]
#[template(
    source = "<a href=\"/integrations/people/files/{{ company }}/{{ id }}\">Download</a>",
    ext = "html"
)]
struct DownloadLink<'a> {
    company: i64,
    id: &'a str,
}
impl Page<Scope, Error> for Pages {
    fn title(&self, _: &str) -> Option<&'static str> {
        Some("People file exchange")
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
    fn uploads(&mut self) -> Option<&mut Uploads> {
        Some(&mut self.uploads)
    }
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
impl Download<Scope, Error> for Pages {
    fn serve<'a>(&'a self, request: DownloadRequest<'a, Scope>) -> DownloadFuture<'a, Error> {
        Box::pin(async move {
            let company = params::positive_id(request.path.get("company_id").map(String::as_str))
                .ok_or(Error::NotFound)?;
            let id = request.path.get("id").ok_or(Error::NotFound)?;
            let document = self
                .connector
                .download_file(request.scope, company, id)
                .await
                .map_err(|_| Error::NotFound)?;
            Ok(FileBody::attachment(
                document.bytes,
                "application/json",
                "people-directory.json",
            ))
        })
    }
}
