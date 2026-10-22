//! Bilimbi Connector.Webhooks runtime scenarios, not a product adapter or unit tests.
use bilibi_authz::grants::Grants;
use bilibi_base::integration::webhooks::{Delivery, WebhookService};
use bilibi_database::{Db, Error};
use bilibi_module_registry::{
    Catalog, Consumer, Contribution, Layer, Module, Provenance, Services,
};
use bilibi_people_connector_connector::*;
use bilibi_ports::{Port, PortClient, PortFuture, PortProvider, Ports, PORTS};
use bilibi_tenancy::Authentication;
use hmac::{Hmac, KeyInit, Mac};
use jiff::Timestamp;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
};

type Outcome<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const BODY: &[u8] = br#"{"event":"directory.changed"}"#;
static PORT_CALLS: AtomicUsize = AtomicUsize::new(0);
static MAPPING: AtomicI64 = AtomicI64::new(1000);
struct CompanyPort;
impl PortProvider for CompanyPort {
    fn call<'a>(
        &'a self,
        client: PortClient<'a>,
        scope: &'a bilibi_tenancy::Scope,
        body: Value,
    ) -> PortFuture<'a> {
        Box::pin(async move {
            PORT_CALLS.fetch_add(1, Ordering::SeqCst);
            let company = body["company_id"].as_i64().ok_or(Error::Invalid)?;
            bilibi_ports::on_client!(client, |c| bilibi_company::organisation::authorize_target(
                c, scope, company, VIEW
            )
            .await)?;
            Ok(
                json!({"value":{"reference":{"source_id":"people/native","type":"company","stable_id":company.to_string()},
                "platform_company_id":company,"workforce_company_id":company+MAPPING.load(Ordering::SeqCst),"name":"Workforce company","code":"WORKFORCE"},"freshness":"current"}),
            )
        })
    }
}
static COMPANY_PORT: Contribution = Contribution::new(
    PORTS,
    DIRECTORY_PORT,
    Provenance {
        module: "people/workforce",
        source: file!(),
        line: line!(),
    },
    &Port {
        provider: &CompanyPort,
    },
);

struct Request {
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
    method: &'static str,
}
impl Request {
    fn new(secret: &str) -> Self {
        let mut request = Self {
            headers: vec![
                ("x-people-tenant".into(), b"1".to_vec()),
                ("x-people-company".into(), b"1".to_vec()),
                (
                    "x-people-timestamp".into(),
                    Timestamp::now().as_second().to_string().into_bytes(),
                ),
                (
                    "x-people-nonce".into(),
                    uuid::Uuid::new_v4().to_string().into_bytes(),
                ),
                (
                    "x-people-delivery".into(),
                    uuid::Uuid::new_v4().to_string().into_bytes(),
                ),
                ("x-people-signature".into(), vec![]),
            ],
            body: BODY.to_vec(),
            method: "POST",
        };
        request.sign(secret);
        request
    }
    fn set(&mut self, name: &str, value: &[u8]) {
        self.headers
            .iter_mut()
            .find(|h| h.0 == name)
            .expect("fixture header")
            .1 = value.to_vec();
    }
    fn value(&self, name: &str) -> &[u8] {
        &self
            .headers
            .iter()
            .find(|h| h.0 == name)
            .expect("fixture header")
            .1
    }
    fn sign(&mut self, secret: &str) {
        let mut bytes = b"people-native:v1\n".to_vec();
        for name in [
            "x-people-tenant",
            "x-people-company",
            "x-people-timestamp",
            "x-people-nonce",
            "x-people-delivery",
        ] {
            bytes.extend_from_slice(self.value(name));
            bytes.push(b'\n');
        }
        bytes.extend_from_slice(&self.body);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC key");
        mac.update(&bytes);
        self.set(
            "x-people-signature",
            data_encoding::HEXLOWER
                .encode(&mac.finalize().into_bytes())
                .as_bytes(),
        );
    }
    fn delivery(&self) -> Delivery<'_> {
        Delivery {
            method: self.method,
            headers: &self.headers,
            body: &self.body,
            remote_ip: None,
        }
    }
    async fn accepted(&self, handler: &WebhookService) -> bool {
        let delivery = self.delivery();
        match handler.0.verify(&delivery).await {
            Ok(proof) => handler.0.handle(&delivery, proof).await.is_ok(),
            Err(_) => false,
        }
    }
    fn retry(&self, secret: &str) -> Self {
        let mut request = Self::new(secret);
        request.set("x-people-delivery", self.value("x-people-delivery"));
        request.body = self.body.clone();
        request.sign(secret);
        request
    }
    fn http(&self, port: u16) -> Outcome<u16> {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        let mut wire=format!("POST /webhooks/people-native HTTP/1.1\r\nHost: localhost:{port}\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n",self.body.len()).into_bytes();
        for (name, value) in &self.headers {
            wire.extend_from_slice(name.as_bytes());
            wire.extend_from_slice(b": ");
            wire.extend_from_slice(value);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(&self.body);
        stream.write_all(&wire)?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response
            .split_whitespace()
            .nth(1)
            .ok_or("missing HTTP status")?
            .parse()?)
    }
}
fn expect(condition: bool, label: &str) -> Outcome {
    if !condition {
        return Err(std::io::Error::other(label).into());
    }
    println!("proved: {label}");
    Ok(())
}
async fn counts(db: &Db) -> Outcome<(i64, i64)> {
    let client = db.get().await?;
    let row=client.query_one("SELECT (SELECT count(*) FROM people_connector_webhook_nonces),(SELECT count(*) FROM people_connector_webhook_deliveries)",&[]).await?;
    Ok((row.get(0), row.get(1)))
}
fn digest(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(bytes))
}

pub async fn run(url: &str) -> Outcome {
    bilibi_base::link();
    bilibi_core::link();
    bilibi_domains::link();
    bilibi_extensions::link();
    let db = bilibi_database::pool(url)?;
    let modules = bilibi_module_registry::discover()?;
    bilibi_module_registry::verify_composition(
        &modules,
        bilibi_domains::MODULES,
        bilibi_extensions::MODULES,
    )?;
    bilibi_database::migrate(&db, &modules).await?;
    bilibi_walkthrough_fixture::install(&db).await?;
    let mut services = Services::default();
    services.provide("bootstrap.database", Arc::new(db.clone()))?;
    services.provide("bootstrap.composition", Arc::new(bilibi_web::composition()))?;
    services.install(&modules)?;
    services
        .get::<bilibi_base::step_up::StepUp>(bilibi_base::step_up::SERVICE)?
        .set_enforced(false);
    let grants = services.get::<Grants>("base/authz.grants")?;
    grants.reconcile().await?;
    let auth = services.get::<Authentication>("base/tenancy.authentication")?;
    let client = db.get().await?;
    let admin = auth
        .authenticate(&**client, 1, 4, "webhook-proof".into())
        .await?;
    let manager = auth
        .authenticate(&**client, 1, 1, "webhook-proof".into())
        .await?;
    let viewer = auth
        .authenticate(&**client, 1, 2, "webhook-proof".into())
        .await?;
    let contractor = auth
        .authenticate(&**client, 1, 7, "webhook-proof".into())
        .await?;
    for key in [VIEW, MANAGE] {
        grants.set_direct(&admin, 1, key, Some(true)).await?;
    }
    grants.set_direct(&admin, 2, VIEW, Some(true)).await?;
    grants.set_direct(&admin, 7, MANAGE, Some(true)).await?;
    // The operator fixture has a current company port with deliberately unequal
    // company axes. The registered production handler has no such dependency.
    let workforce = Module {
        id: "people/workforce",
        layer: Layer::Domains,
        dependencies: &["base/ports"],
        migrations: &[],
        services: &[],
    };
    let mut deployment: Vec<&Module> = modules
        .iter()
        .copied()
        .filter(|m| m.id != "people/workforce")
        .collect();
    deployment.push(&workforce);
    let consumers: Vec<&Consumer> = inventory::iter::<Consumer>
        .into_iter()
        .filter(|c| [ADAPTERS, PORTS].contains(&c.id))
        .collect();
    let catalog = Catalog::build(&deployment, consumers, vec![&COMPANY_PORT])?;
    let ports = Arc::new(Ports::from_catalog(&catalog, &deployment)?);
    let foreign = Provider::new(
        "fixture.other",
        "Other fixture provider",
        "1.0.0",
        vec![Capability::new("company_directory", Direction::Read)?],
        Credential::None,
    )?;
    let registry = Providers::installed().register(foreign)?;
    let connector =
        Connector::from_catalog(&services, &catalog, &deployment, registry, ports.clone())?;
    let handler = services.get::<WebhookService>(WEBHOOK_SERVICE)?;
    expect(
        connector
            .webhook_summary(&manager, 1)
            .await?
            .max_skew_seconds
            == 300,
        "disconnected summary has default clock allowance",
    )?;
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    connector.set_enabled(&manager, 1, true).await?;
    expect(
        matches!(
            connector
                .put_webhook_settings(&viewer, 1, WebhookSettingsUpdate::default())
                .await,
            Err(ConnectorError::Unauthorized)
        ),
        "viewer cannot change webhook policy",
    )?;
    expect(
        matches!(
            connector
                .put_webhook_settings(&contractor, 1, WebhookSettingsUpdate::default())
                .await,
            Err(ConnectorError::Unauthorized)
        ),
        "same-tenant sibling requires company reach",
    )?;
    expect(
        matches!(
            connector
                .put_webhook_settings(&manager, 100, WebhookSettingsUpdate::default())
                .await,
            Err(ConnectorError::NotFound)
        ),
        "other tenant's company is refused",
    )?;
    expect(
        matches!(
            connector
                .put_webhook_settings(
                    &manager,
                    1,
                    WebhookSettingsUpdate {
                        enabled: Some(true),
                        ..Default::default()
                    }
                )
                .await,
            Err(ConnectorError::WebhookSecretMissing)
        ),
        "intake cannot be enabled without a secret",
    )?;
    for value in [0, 86401] {
        expect(
            matches!(
                connector
                    .put_webhook_settings(
                        &manager,
                        1,
                        WebhookSettingsUpdate {
                            max_skew_seconds: Some(value),
                            ..Default::default()
                        }
                    )
                    .await,
                Err(ConnectorError::InvalidWebhookSettings)
            ),
            "out-of-range clock allowance is refused",
        )?;
    }
    for secret in ["x".repeat(31), "x".repeat(4097), " ".repeat(32)] {
        expect(
            matches!(
                connector
                    .put_webhook_settings(
                        &manager,
                        1,
                        WebhookSettingsUpdate {
                            secret: Some(secret),
                            ..Default::default()
                        }
                    )
                    .await,
                Err(ConnectorError::InvalidWebhookSettings)
            ),
            "invalid signing secret is refused",
        )?;
    }
    let secret = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let replacement = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let policy = connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                secret: Some(secret.clone()),
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await?;
    expect(
        policy.enabled && policy.secret_stored && policy.last_received_at.is_none(),
        "manager stores encrypted policy without revealing it",
    )?;
    let stored:Value=client.query_one("SELECT webhook_secret FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1",&[]).await?.get(0);
    expect(
        stored.get("ciphertext").is_some() && !stored.to_string().contains(&secret),
        "the connection stores an encrypted envelope",
    )?;
    expect(
        matches!(
            connector
                .put_webhook_settings(
                    &manager,
                    1,
                    WebhookSettingsUpdate {
                        secret: Some(String::new()),
                        ..Default::default()
                    }
                )
                .await,
            Err(ConnectorError::WebhookSecretMissing)
        ),
        "clearing a secret while intake is enabled is refused",
    )?;
    let wrong_key = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    for key in [None, Some(wrong_key)] {
        let mut controlled_services = Services::default();
        controlled_services.provide("base/database.pool", Arc::new(db.clone()))?;
        controlled_services.provide(
            "base/database.mutations",
            services.get::<bilibi_database::MutationBoundary>("base/database.mutations")?,
        )?;
        controlled_services.provide(
            "base/settings.store",
            Arc::new(bilibi_base::settings::Settings::new(
                bilibi_base::settings::Definitions::from_catalog(services.catalog()?)?,
                bilibi_base::settings::Encryption::from_hex(key.as_deref()),
                services.get::<bilibi_tenancy::PartyProofs>("base/tenancy.parties")?,
            )),
        )?;
        let controlled = Arc::new(Connector::from_catalog(
            &controlled_services,
            &catalog,
            &deployment,
            Providers::installed(),
            ports.clone(),
        )?);
        expect(
            controlled.webhook_summary(&manager, 1).await?.secret_stored,
            "summary remains masked with a missing or wrong installation key",
        )?;
        let before:Value=client.query_one("SELECT webhook_secret FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1",&[]).await?.get(0);
        expect(
            controlled
                .put_webhook_settings(
                    &manager,
                    1,
                    WebhookSettingsUpdate {
                        secret: Some(replacement.clone()),
                        ..Default::default()
                    },
                )
                .await
                .is_err(),
            "missing or wrong installation key cannot overwrite a signing secret",
        )?;
        let after:Value=client.query_one("SELECT webhook_secret FROM people_connector_connections WHERE tenant_id=1 AND platform_company_id=1",&[]).await?.get(0);
        expect(
            before == after,
            "failed secret rotation preserves the original ciphertext",
        )?;
        expect(
            !Request::new(&secret)
                .accepted(&WebhookService(controlled.clone()))
                .await,
            "missing or wrong installation key refuses machine verification",
        )?;
        let cleared = controlled
            .put_webhook_settings(
                &manager,
                1,
                WebhookSettingsUpdate {
                    secret: Some(String::new()),
                    enabled: Some(false),
                    ..Default::default()
                },
            )
            .await?;
        expect(
            !cleared.secret_stored && !cleared.enabled,
            "ciphertext can be cleared with intake disabled without decrypting it",
        )?;
        connector
            .put_webhook_settings(
                &manager,
                1,
                WebhookSettingsUpdate {
                    secret: Some(secret.clone()),
                    enabled: Some(true),
                    ..Default::default()
                },
            )
            .await?;
    }
    let port_calls = PORT_CALLS.load(Ordering::SeqCst);

    let request = Request::new(&secret);
    expect(
        request.accepted(&handler).await,
        "first signed directory notification is received",
    )?;
    expect(
        counts(&db).await? == (1, 1),
        "first receipt atomically stores one nonce and delivery",
    )?;
    expect(
        !request.accepted(&handler).await,
        "used nonce always refuses",
    )?;
    let retry = request.retry(&secret);
    expect(
        retry.accepted(&handler).await && counts(&db).await? == (2, 1),
        "new nonce and identical delivery bytes acknowledge a duplicate",
    )?;
    let mut conflict = request.retry(&secret);
    conflict.body = b"{ \"event\": \"directory.changed\" }".to_vec();
    conflict.sign(&secret);
    expect(
        !conflict.accepted(&handler).await && counts(&db).await? == (2, 1),
        "different raw whitespace for a delivery refuses without consuming its nonce",
    )?;
    conflict.body = BODY.to_vec();
    conflict.sign(&secret);
    expect(
        conflict.accepted(&handler).await,
        "the refused conflict's nonce is still usable with the original bytes",
    )?;

    let mut boundary_keys = Request::new(&secret);
    boundary_keys.set("x-people-nonce", b"A");
    boundary_keys.set(
        "x-people-delivery",
        format!("A0._:-{}", "x".repeat(94)).as_bytes(),
    );
    boundary_keys.sign(&secret);
    expect(
        boundary_keys.accepted(&handler).await,
        "one-character and 100-character keys accept every permitted punctuation",
    )?;
    let baseline = counts(&db).await?;
    for index in 0..6 {
        let mut missing = Request::new(&secret);
        missing.headers.remove(index);
        expect(
            !missing.accepted(&handler).await,
            "each signing header is required",
        )?;
        let mut duplicate = Request::new(&secret);
        let mut extra = duplicate.headers[index].clone();
        extra.0 = extra.0.to_uppercase();
        duplicate.headers.push(extra);
        expect(
            !duplicate.accepted(&handler).await,
            "each differently cased duplicate signing header refuses",
        )?;
    }
    for name in ["x-people-tenant", "x-people-company", "x-people-timestamp"] {
        for value in [
            "0",
            "01",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1\n",
            "1000000000000000000",
            "١",
        ] {
            let mut invalid = Request::new(&secret);
            invalid.set(name, value.as_bytes());
            invalid.sign(&secret);
            expect(
                !invalid.accepted(&handler).await,
                "numeric headers enforce positive ASCII decimals of at most 18 digits",
            )?;
        }
    }
    for name in ["x-people-nonce", "x-people-delivery"] {
        for value in [
            String::new(),
            "-first".into(),
            "bad space".into(),
            "é".into(),
            "x".repeat(101),
        ] {
            let mut invalid = Request::new(&secret);
            invalid.set(name, value.as_bytes());
            invalid.sign(&secret);
            expect(
                !invalid.accepted(&handler).await,
                "nonce and delivery key shapes are enforced",
            )?;
        }
    }
    for timestamp in [
        Timestamp::now().as_second() - 600,
        Timestamp::now().as_second() + 600,
        999999999999999999,
    ] {
        let mut stale = Request::new(&secret);
        stale.set("x-people-timestamp", timestamp.to_string().as_bytes());
        stale.sign(&secret);
        expect(
            !stale.accepted(&handler).await,
            "past, future and 18-digit out-of-window timestamps refuse",
        )?;
    }
    for signature in [
        "a".repeat(63),
        "a".repeat(65),
        "A".repeat(64),
        "g".repeat(64),
        "0".repeat(64),
    ] {
        let mut invalid = Request::new(&secret);
        invalid.set("x-people-signature", signature.as_bytes());
        expect(
            !invalid.accepted(&handler).await,
            "signature shape and full HMAC tag are enforced",
        )?;
    }
    let mut get = Request::new(&secret);
    get.method = "GET";
    expect(!get.accepted(&handler).await, "non-POST method refuses")?;
    let mut invalid_utf8 = Request::new(&secret);
    invalid_utf8.set("x-people-nonce", &[255]);
    invalid_utf8.sign(&secret);
    expect(
        !invalid_utf8.accepted(&handler).await,
        "invalid UTF-8 header refuses",
    )?;
    let mut tampered = Request::new(&secret);
    tampered.body.push(b'\n');
    expect(
        !tampered.accepted(&handler).await,
        "body bytes cannot be changed after signing",
    )?;
    for field in [
        "x-people-tenant",
        "x-people-company",
        "x-people-timestamp",
        "x-people-nonce",
        "x-people-delivery",
    ] {
        let mut tampered = Request::new(&secret);
        let value = if field.contains("timestamp") {
            Timestamp::now().as_second().saturating_add(1).to_string()
        } else {
            "2".into()
        };
        tampered.set(field, value.as_bytes());
        expect(
            !tampered.accepted(&handler).await,
            "routing and attempt fields cannot be changed after signing",
        )?;
    }
    for body in [
        b"not json".as_slice(),
        b"[]",
        b"{}",
        br#"{"event":"other"}"#,
        br#"{"event":"directory.changed","extra":true}"#,
    ] {
        let mut invalid = Request::new(&secret);
        invalid.body = body.to_vec();
        invalid.sign(&secret);
        let raw = invalid.delivery();
        expect(
            handler.0.verify(&raw).await.is_ok(),
            "signature verification happens before JSON decoding",
        )?;
        expect(
            !invalid.accepted(&handler).await,
            "signed unsupported body refuses in handling",
        )?;
    }
    expect(
        counts(&db).await? == baseline,
        "refused requests leave no nonce or delivery rows",
    )?;
    expect(
        PORT_CALLS.load(Ordering::SeqCst) == port_calls,
        "machine verification and handling never read the People directory",
    )?;

    // Verify first, then change policy through the authorized facade. Handle
    // must re-read it under its lock, not trust the earlier proof.
    let rotation = Request::new(&secret);
    let raw = rotation.delivery();
    let proof = handler
        .0
        .verify(&raw)
        .await
        .map_err(|_| "verification failed")?;
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                secret: Some(replacement.clone()),
                ..Default::default()
            },
        )
        .await?;
    expect(
        handler.0.handle(&raw, proof).await.is_err(),
        "secret rotation between callbacks refuses the old signature",
    )?;
    let disabled = Request::new(&replacement);
    let raw = disabled.delivery();
    let proof = handler
        .0
        .verify(&raw)
        .await
        .map_err(|_| "verification failed")?;
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await?;
    expect(
        handler.0.handle(&raw, proof).await.is_err(),
        "disabling intake between callbacks refuses handling",
    )?;
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                secret: Some(secret.clone()),
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await?;
    expect(
        !request.accepted(&handler).await,
        "nonce history survives secret rotation",
    )?;

    let parallel = Request::new(&secret);
    let (a, b) = tokio::join!(parallel.accepted(&handler), parallel.accepted(&handler));
    expect(
        a != b,
        "concurrent requests with the same nonce serialize and only one succeeds",
    )?;
    let parallel = Request::new(&secret);
    let retry = parallel.retry(&secret);
    let before = counts(&db).await?;
    let (a, b) = tokio::join!(parallel.accepted(&handler), retry.accepted(&handler));
    expect(
        a && b && counts(&db).await? == (before.0 + 2, before.1 + 1),
        "concurrent delivery retries store one receipt and two nonces",
    )?;

    // Simulate a scoped action failure after receipt SQL and mutation capture.
    let rollback = Request::new(&secret);
    let before = counts(&db).await?;
    client.batch_execute("CREATE FUNCTION kc3_refuse_action() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event='people-connector.webhook' THEN RAISE EXCEPTION 'receipt audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER kc3_refuse_action BEFORE INSERT ON base_audit_actions FOR EACH ROW EXECUTE FUNCTION kc3_refuse_action()").await?;
    expect(
        !rollback.accepted(&handler).await && counts(&db).await? == before,
        "audit failure rolls back the nonce and receipt",
    )?;
    client.batch_execute("DROP TRIGGER kc3_refuse_action ON base_audit_actions; DROP FUNCTION kc3_refuse_action()").await?;
    expect(
        rollback.accepted(&handler).await,
        "retry succeeds after audit recovers with no spent nonce",
    )?;
    let nonce_hash = digest(rollback.value("x-people-nonce"));
    let row=client.query_one("SELECT count(*) FROM people_connector_webhook_nonces WHERE tenant_id=1 AND nonce_hash=$1",&[&nonce_hash]).await?;
    expect(
        row.get::<_, i64>(0) == 1,
        "retry stores exactly one hashed nonce",
    )?;

    let actions=client.query("SELECT actor_type,actor_id,payload FROM base_audit_actions WHERE tenant_id=1 AND event='people-connector.webhook'",&[]).await?;
    expect(
        actions.iter().all(|r| {
            r.get::<_, String>(0) == "guest"
                && r.get::<_, i64>(1) == 0
                && r.get::<_, Value>(2)["platform_company_id"] == 1
        }),
        "receipt actions use the company and guest actor zero",
    )?;
    expect(
        actions
            .iter()
            .any(|r| r.get::<_, Value>(2)["outcome"] == "duplicate")
            && actions
                .iter()
                .any(|r| r.get::<_, Value>(2)["outcome"] == "received"),
        "scoped audit distinguishes received and duplicate",
    )?;
    let leaked:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM base_audit_mutations WHERE old_values::text LIKE '%' || $1 || '%' OR new_values::text LIKE '%' || $1 || '%') OR EXISTS(SELECT 1 FROM base_audit_actions WHERE payload::text LIKE '%' || $1 || '%')",&[&secret]).await?.get(0);
    expect(!leaked, "audit contains no signing secret")?;
    let captured=client.query("SELECT new_values FROM base_audit_mutations WHERE auditable_type='people_connector_connection' AND new_values ? 'webhook_secret'",&[]).await?;
    expect(
        !captured.is_empty()
            && captured
                .iter()
                .all(|r| r.get::<_, Value>(0)["webhook_secret"] == "[redacted]"),
        "owner audit redaction also hides ciphertext",
    )?;
    expect(
        client
            .query_one(
                "SELECT count(*) FROM base_settings WHERE key LIKE 'people-connector.webhook.%'",
                &[],
            )
            .await?
            .get::<_, i64>(0)
            == 0,
        "webhook policy does not use login-scoped Settings overrides",
    )?;

    // Both setting bounds work through the facade and the registered handler.
    for (size, skew) in [(32, 1), (4096, 86400)] {
        let bounded = secret.repeat(64)[..size].to_owned();
        connector
            .put_webhook_settings(
                &manager,
                1,
                WebhookSettingsUpdate {
                    secret: Some(bounded.clone()),
                    max_skew_seconds: Some(skew),
                    ..Default::default()
                },
            )
            .await?;
        expect(
            Request::new(&bounded).accepted(&handler).await,
            "minimum and maximum secret/skew settings accept a signed notification",
        )?;
    }
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                secret: Some(secret.clone()),
                max_skew_seconds: Some(300),
                ..Default::default()
            },
        )
        .await?;

    // Connection lifecycle must clear policy without erasing replay history.
    connector
        .configure_connection(&manager, 1, "fixture.other")
        .await?;
    let summary = connector.webhook_summary(&manager, 1).await?;
    expect(
        !summary.enabled && !summary.secret_stored && summary.max_skew_seconds == 300,
        "provider change clears all webhook policy values",
    )?;
    expect(
        matches!(
            connector
                .put_webhook_settings(&manager, 1, WebhookSettingsUpdate::default())
                .await,
            Err(ConnectorError::Unsupported)
        ),
        "foreign provider cannot configure native intake",
    )?;
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    connector.set_enabled(&manager, 1, true).await?;
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                secret: Some(secret.clone()),
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await?;
    MAPPING.store(2000, Ordering::SeqCst);
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    let summary = connector.webhook_summary(&manager, 1).await?;
    expect(
        !summary.enabled && !summary.secret_stored,
        "mapping change clears signing policy",
    )?;
    connector.remove_connection(&manager, 1).await?;
    expect(
        counts(&db).await? == (0, 0),
        "removing a connection captures and clears all replay history",
    )?;
    connector
        .configure_connection(&manager, 1, NATIVE_PROVIDER_ID)
        .await?;
    connector.set_enabled(&manager, 1, true).await?;
    expect(
        !Request::new(&secret).accepted(&handler).await,
        "recreated connection requires a new configured policy",
    )?;
    connector
        .put_webhook_settings(
            &manager,
            1,
            WebhookSettingsUpdate {
                secret: Some(secret.clone()),
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await?;

    if let Ok(port) = std::env::var("WEBHOOK_PROOF_PORT") {
        let port = port.parse()?;
        let request = Request::new(&secret);
        expect(
            request.http(port)? == 202,
            "running host accepts a signed machine delivery",
        )?;
        expect(
            request.http(port)? == 403,
            "running host refuses a spent nonce",
        )?;
        expect(
            request.retry(&secret).http(port)? == 202,
            "running host acknowledges a signed retry",
        )?;
        let mut tampered = Request::new(&secret);
        tampered.body.push(b' ');
        expect(
            tampered.http(port)? == 403,
            "running host refuses tampered raw bytes",
        )?;
        let mut conflict = request.retry(&secret);
        conflict.body.push(b' ');
        conflict.sign(&secret);
        expect(
            conflict.http(port)? == 403,
            "running host refuses different bytes for a delivery key",
        )?;
    }
    expect(
        connector
            .webhook_summary(&manager, 1)
            .await?
            .last_received_at
            .is_some()
            == std::env::var("WEBHOOK_PROOF_PORT").is_ok(),
        "summary reports the last first receipt",
    )?;
    println!("KC3 webhook walkthrough complete");
    Ok(())
}
