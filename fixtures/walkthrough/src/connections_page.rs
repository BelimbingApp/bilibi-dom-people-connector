//! KC2 disposable-database walkthrough of the real ConnectionsLive page service.
use bilibi_authz::grants::Grants;
use bilibi_database::Error;
use bilibi_module_registry::Services;
use bilibi_people_connector_connector::{
    ConnectionState, Connector, Page, PortAuthorization, PortContext, PortRequest, ReadFuture,
    ReadPort, MANAGE, SERVICE, VIEW,
};
use bilibi_tenancy::{Authentication, Scope};
use bilibi_ui::page::{LiveBody, LiveInput, LiveRequest, LiveState, PageRequest, PageService};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
/// A fixture directory, contributed only by the browser/proof examples.
/// One malformed fact creates an issue beside the accepted records.
pub struct ConnectionsDirectory;
impl ReadPort for ConnectionsDirectory {
    fn read<'a>(
        &'a self,
        context: PortContext<'a>,
        auth: &'a PortAuthorization<'a>,
        request: &'a PortRequest,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            let mut page: Page = crate::BackupDirectory.read(context, auth, request).await?;
            if auth.capability == "employee_directory" {
                page.entries.push(json!({"malformed":true}));
            }
            Ok(page)
        })
    }
}
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(format!("FAIL {message}").into());
    }
    println!("PASS {message}");
    Ok(())
}
async fn event(
    page: &mut dyn LiveState<Scope, Error>,
    scope: &Scope,
    payload: Value,
) -> Result<LiveBody> {
    let mut flat = serde_json::Map::new();
    for (key, value) in payload.as_object().ok_or("event object missing")? {
        if let Some(fields) = value.as_object() {
            for (field, value) in fields {
                flat.insert(format!("{key}[{field}]"), value.clone());
            }
        } else {
            flat.insert(key.clone(), value.clone());
        }
    }
    let payload = Value::Object(flat);
    Ok(page
        .update(LiveRequest {
            scope,
            credential: "connections-proof",
            input: LiveInput::Event(&payload),
        })
        .await?)
}
fn sync_key(html: &str) -> Result<String> {
    let part = html
        .split("id=\"people-connections-synchronise\"")
        .nth(1)
        .ok_or("sync button missing")?;
    let key = part
        .split("value=\"")
        .nth(1)
        .ok_or("page key missing")?
        .split('"')
        .next()
        .ok_or("page key missing")?;
    Ok(key.into())
}
async fn notice(services: &Services, secret: &str) -> Result<()> {
    use bilibi_base::integration::webhooks::{Delivery, WebhookService};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    let mut headers = vec![
        ("x-people-tenant".into(), b"1".to_vec()),
        ("x-people-company".into(), b"1".to_vec()),
        (
            "x-people-timestamp".into(),
            jiff::Timestamp::now().as_second().to_string().into_bytes(),
        ),
        (
            "x-people-nonce".into(),
            uuid::Uuid::new_v4().to_string().into_bytes(),
        ),
        (
            "x-people-delivery".into(),
            uuid::Uuid::new_v4().to_string().into_bytes(),
        ),
    ];
    let body = br#"{"event":"directory.changed"}"#;
    let mut bytes = b"people-native:v1\n".to_vec();
    for (_, value) in &headers {
        bytes.extend_from_slice(value);
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(body);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(&bytes);
    headers.push((
        "x-people-signature".into(),
        data_encoding::HEXLOWER
            .encode(&mac.finalize().into_bytes())
            .into_bytes(),
    ));
    let delivery = Delivery {
        method: "POST",
        headers: &headers,
        body,
        remote_ip: None,
    };
    let handler =
        services.get::<WebhookService>(bilibi_people_connector_connector::WEBHOOK_SERVICE)?;
    let verified = handler
        .0
        .verify(&delivery)
        .await
        .map_err(|_| "notification verification refused")?;
    handler
        .0
        .handle(&delivery, verified)
        .await
        .map_err(|_| "notification receipt refused")?;
    Ok(())
}
/// Runs only on a freshly created disposable database, with the example's fixture catalog.
pub async fn connections_page_walkthrough() -> Result<()> {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = bilibi_database::pool(&std::env::var("DATABASE_URL")?)?;
    let modules = bilibi_module_registry::discover()?;
    bilibi_database::migrate(&db, &modules).await?;
    bilibi_walkthrough_fixture::install(&db).await?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    services
        .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
        .set_enforced(false);
    let client = db.get().await?;
    let auth = services.get::<Authentication>("base/tenancy.authentication")?;
    let admin = auth
        .authenticate(&**client, 1, 4, "connections-proof".into())
        .await?;
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    grants.set_direct(&admin, 2, VIEW, Some(true)).await?;
    // The viewer has no company affiliation in the Platform fixture.
    grants
        .set_direct(&admin, 2, "admin.company.tenant-wide.manage", Some(true))
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "connections-proof".into())
        .await?;
    let viewer = auth
        .authenticate(&**client, 1, 2, "connections-proof".into())
        .await?;
    let pages = services
        .get::<PageService<Scope, Error>>("people_connector/connector.connections_pages")?;
    let connector = services.get::<Connector>(SERVICE)?;
    let query = HashMap::from([("company_id".into(), "1".into())]);
    let path = HashMap::new();
    let mut page = pages
        .0
        .mount(PageRequest {
            operation: "connections",
            scope: Some(&manager),
            path: &path,
            query: &query,
            body: &[],
            framed: false,
        })
        .await?
        .ok_or("no live state")?;
    let configured = event(
        page.as_mut(),
        &manager,
        json!({"type":"configure", "provider_id":"people.native"}),
    )
    .await?;
    require(
        configured.html.contains("Connection saved."),
        "provider configured through page",
    )?;
    require(
        configured.html.contains("people/native · 1001"),
        "both company axes remain distinct",
    )?;
    require(
        configured.html.contains("Showing 1 to 10 of 62 results"),
        "company table is bounded to ten rows",
    )?;
    let second_page = event(page.as_mut(), &manager, json!({"type":"page", "page":"2"})).await?;
    require(
        second_page.query.get("page").is_some_and(|v| v == "2")
            && second_page
                .query
                .get("company_id")
                .is_some_and(|v| v == "1")
            && second_page.html.contains("Showing 11 to 20 of 62 results"),
        "paging preserves the selected company and address",
    )?;
    event(page.as_mut(), &manager, json!({"type":"page", "page":"1"})).await?;
    let enabled = event(
        page.as_mut(),
        &manager,
        json!({"type":"set_enabled", "enabled":"true"}),
    )
    .await?;
    require(
        enabled.html.contains("Connection enabled."),
        "enable through page",
    )?;
    let key = sync_key(&enabled.html)?;
    let synced = event(
        page.as_mut(),
        &manager,
        json!({"type":"synchronise", "key":key}),
    )
    .await?;
    require(
        synced.html.contains("Synchronisation: succeeded."),
        "page key synchronises",
    )?;
    require(
        synced
            .html
            .contains("A record was incomplete or malformed."),
        "open issues table shows rejected fact",
    )?;
    let replay = event(
        page.as_mut(),
        &manager,
        json!({"type":"synchronise", "key":key}),
    )
    .await?;
    require(
        replay.html.contains("Synchronisation: succeeded."),
        "repeated page key replays",
    )?;
    let count: i64 = client
        .query_one(
            "SELECT count(*) FROM people_connector_sync_runs WHERE tenant_id=1",
            &[],
        )
        .await?
        .get(0);
    require(count == 1, "replayed click creates only one run")?;
    let full = event(
        page.as_mut(),
        &manager,
        json!({"type":"synchronise", "key":key, "full":"true"}),
    )
    .await?;
    require(
        full.html.contains("Synchronisation: succeeded."),
        "full read uses its own key suffix",
    )?;
    let forged = event(
        page.as_mut(),
        &manager,
        json!({"type":"synchronise", "key":"unissued"}),
    )
    .await?;
    require(
        forged.html.contains("Enter a request key"),
        "unissued sync key refuses",
    )?;
    let issue = connector.sync_summary(&manager, 1).await?.open_issues[0].id;
    let resolved = event(
        page.as_mut(),
        &manager,
        json!({"type":"resolve_issue", "id":issue.to_string()}),
    )
    .await?;
    require(
        resolved.html.contains("Issue marked resolved.")
            && resolved.html.contains("No open issues."),
        "mark resolved refreshes issue list",
    )?;
    let invalid = event(page.as_mut(), &manager, json!({"type":"save_policy", "policy":{"page_limit":"0", "max_age_minutes":"5", "run_timeout_minutes":"1"}})).await?;
    require(
        invalid
            .html
            .contains("Enter whole numbers within the ranges shown."),
        "invalid policy refuses visibly",
    )?;
    event(page.as_mut(), &manager, json!({"type":"save_policy", "policy":{"page_limit":"100", "max_age_minutes":"60", "run_timeout_minutes":"5"}})).await?;
    require(
        connector.sync_summary(&manager, 1).await?.policy.page_limit == 100,
        "policy saved through page",
    )?;
    let secret = uuid::Uuid::new_v4().simple().to_string().repeat(128);
    let webhook = event(page.as_mut(), &manager, json!({"type":"save_webhook", "webhook":{"enabled":"true", "password":secret, "max_skew_seconds":"300"}})).await?;
    require(
        webhook.html.contains("Webhook settings saved.") && !webhook.html.contains(&secret),
        "webhook secret stored without HTML exposure",
    )?;
    event(page.as_mut(), &manager, json!({"type":"save_webhook", "webhook":{"enabled":"true", "password":"••••••••", "max_skew_seconds":"301"}})).await?;
    require(
        connector.webhook_summary(&manager, 1).await?.secret_stored,
        "masked secret keeps ciphertext",
    )?;
    notice(&services, &secret).await?;
    let received = connector
        .webhook_summary(&manager, 1)
        .await?
        .last_received_at
        .ok_or("receipt missing")?;
    let refreshed = page
        .update(LiveRequest {
            scope: &manager,
            credential: "connections-proof",
            input: LiveInput::Refresh,
        })
        .await?;
    require(
        refreshed.html.contains(&received.to_string()),
        "last signed receipt renders through the page clock",
    )?;
    let unconfirmed = event(page.as_mut(), &manager, json!({"type":"remove"})).await?;
    require(
        unconfirmed
            .html
            .contains("Someone else changed this connection."),
        "remove requires server-held confirmation",
    )?;
    let requested = event(page.as_mut(), &manager, json!({"type":"request_remove"})).await?;
    require(
        requested.html.contains("people-connections-remove-confirm"),
        "remove opens ConfirmDialog",
    )?;
    event(page.as_mut(), &manager, json!({"type":"cancel_remove"})).await?;
    require(
        connector.status(&manager, 1).await?.state == ConnectionState::Enabled,
        "cancel leaves connection intact",
    )?;
    event(page.as_mut(), &manager, json!({"type":"request_remove"})).await?;
    grants.set_direct(&admin, 1, MANAGE, Some(false)).await?;
    let denied = event(page.as_mut(), &manager, json!({"type":"remove"})).await?;
    require(
        denied
            .html
            .contains("You cannot change connections for this company.")
            && !denied.html.contains("people-connections-remove-confirm"),
        "revoked grant refuses stale confirmed removal",
    )?;
    require(
        connector.status(&manager, 1).await?.state == ConnectionState::Enabled,
        "revoked write makes no mutation",
    )?;
    grants.set_direct(&admin, 1, MANAGE, Some(true)).await?;
    let viewer_html = pages
        .0
        .render(PageRequest {
            operation: "connections",
            scope: Some(&viewer),
            path: &path,
            query: &query,
            body: &[],
            framed: false,
        })
        .await?
        .html;
    require(
        viewer_html.contains("people/native · 1001")
            && !viewer_html.contains("people-connections-provider-form")
            && !viewer_html.contains("people-connections-remove\""),
        "viewer sees facts with write controls withheld",
    )?;
    event(page.as_mut(), &manager, json!({"type":"request_remove"})).await?;
    let removed = event(page.as_mut(), &manager, json!({"type":"remove"})).await?;
    require(
        removed.html.contains("Connection removed.")
            && connector.status(&manager, 1).await?.state == ConnectionState::Disconnected,
        "confirmed removal deletes connection",
    )?;
    // Leave an enabled connection and open issue for the browser proof.
    event(
        page.as_mut(),
        &manager,
        json!({"type":"configure", "provider_id":"people.native"}),
    )
    .await?;
    event(
        page.as_mut(),
        &manager,
        json!({"type":"set_enabled", "enabled":"true"}),
    )
    .await?;
    event(page.as_mut(), &manager, json!({"type":"save_webhook", "webhook":{"enabled":"true", "password":secret, "max_skew_seconds":"300"}})).await?;
    notice(&services, &secret).await?;
    println!("KC2 page walkthrough complete");
    Ok(())
}
