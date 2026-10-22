//! Bilimbi Connector.Webhooks: authenticate exact bytes, then record a receipt.
//! The sender has no login identity. Policy lives on our company connection;
//! Settings supplies owner-secret encryption, never a fabricated account scope.
use crate::{
    authorize, connections, people, require_mapping, require_writable, webhook_store, Connection,
    Connector, ConnectorError, Result, Status, NATIVE_PROVIDER_ID,
};
use bilibi_audit::{record_action, Attribution};
use bilibi_company::organisation;
use bilibi_database::{Context, Error, Principal};
use bilibi_integration::webhooks::{Delivery, Refused, Verified, WebhookFuture, WebhookHandler};
use bilibi_tenancy::Scope;
use hmac::{Hmac, KeyInit, Mac};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

pub const WEBHOOK_IDENTIFIER: &str = "people-native";
pub const WEBHOOK_SERVICE: &str = "people_connector/connector.native-webhook";
const SECRET_OWNER: &str = "people_connector/connector.webhook";

/// The connections page's policy and most recent first receipt. Never a secret.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WebhookSummary {
    pub enabled: bool,
    pub secret_stored: bool,
    pub max_skew_seconds: i32,
    #[serde(with = "crate::sync_types::optional_instant")]
    pub last_received_at: Option<Timestamp>,
}

/// Omitted fields keep their current values. An empty secret clears it;
/// clearing while intake remains enabled is refused. No Debug/Serialize.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookSettingsUpdate {
    pub enabled: Option<bool>,
    pub secret: Option<String>,
    pub max_skew_seconds: Option<i32>,
}

impl Connector {
    /// Follows status's Workforce read policy and exposes no secret value.
    pub async fn webhook_summary(
        &self,
        scope: &Scope,
        platform_company_id: i64,
    ) -> Result<WebhookSummary> {
        let client = self.db.get().await.map_err(Error::from)?;
        self.status_on(&client, scope, platform_company_id).await?;
        let connection =
            connections::find(&**client, scope.tenant(), platform_company_id, false).await?;
        self.webhook_summary_on(&**client, connection.as_ref())
            .await
    }

    /// Manager-only policy write, authorized again inside the company lock.
    /// This deliberately uses our row, not company Settings overrides: inbound
    /// verification has no account Scope with which to resolve those overrides.
    pub async fn put_webhook_settings(
        &self,
        scope: &Scope,
        platform_company_id: i64,
        update: WebhookSettingsUpdate,
    ) -> Result<WebhookSummary> {
        let mut client = self.db.get().await.map_err(Error::from)?;
        let company = authorize(&**client, scope, platform_company_id).await?;
        if update
            .max_skew_seconds
            .is_some_and(|v| !(1..=86400).contains(&v))
            || update
                .secret
                .as_deref()
                .is_some_and(|v| !v.is_empty() && !valid_secret(v))
        {
            return Err(ConnectorError::InvalidWebhookSettings);
        }
        let mut tx = self
            .begin(&mut client, scope, "people-connector.put_webhook_settings")
            .await?;
        require_writable(&tx, scope, company).await?;
        let (connection, _) = self.connection_provider(tx.sql(), scope, company).await?;
        if connection.provider_id != NATIVE_PROVIDER_ID {
            return Err(ConnectorError::Unsupported);
        }
        let current =
            Status::from_workforce(people::company(&self.ports, &tx, scope, company).await?)?;
        require_mapping(&connection, &current)?;
        let before = webhook_store::policy(tx.sql(), &connection).await?;
        let enabled = update.enabled.unwrap_or(before.enabled);
        let secret = match update.secret.as_deref() {
            Some("") => None,
            Some(secret) => {
                // Match Settings::put: a syntactically valid but wrong
                // installation key must not overwrite existing ciphertext.
                if let Some(stored) = before.secret.as_ref() {
                    self.settings.open_owned(
                        SECRET_OWNER,
                        connection.tenant_id,
                        connection.id,
                        stored,
                    )?;
                }
                Some(self.settings.seal_owned(
                    SECRET_OWNER,
                    connection.tenant_id,
                    connection.id,
                    &json!(secret),
                )?)
            }
            None => before.secret.clone(),
        };
        if enabled && secret.is_none() {
            return Err(ConnectorError::WebhookSecretMissing);
        }
        let after = webhook_store::Policy {
            secret,
            enabled,
            max_skew_seconds: update.max_skew_seconds.unwrap_or(before.max_skew_seconds),
        };
        webhook_store::save_policy(&mut tx, &connection, before, after).await?;
        let summary = self.webhook_summary_on(tx.sql(), Some(&connection)).await?;
        tx.commit().await?;
        Ok(summary)
    }

    async fn webhook_summary_on(
        &self,
        client: &impl GenericClient,
        connection: Option<&Connection>,
    ) -> Result<WebhookSummary> {
        let (policy, last_received_at) = match connection {
            Some(c) => (
                webhook_store::policy(client, c).await?,
                webhook_store::last_received(client, c).await?,
            ),
            None => (webhook_store::Policy::default(), None),
        };
        Ok(WebhookSummary {
            enabled: policy.enabled,
            secret_stored: policy.secret.is_some(),
            max_skew_seconds: policy.max_skew_seconds,
            last_received_at,
        })
    }

    /// No body parsing, mutations, directory reads or login account lookups.
    async fn verify_native(
        &self,
        client: &impl GenericClient,
        connection: &Connection,
        fields: &Fields<'_>,
        body: &[u8],
    ) -> std::result::Result<(), Refused> {
        if !connection.enabled || connection.provider_id != NATIVE_PROVIDER_ID {
            return Err(Refused);
        }
        let policy = webhook_store::policy(client, connection)
            .await
            .map_err(|_| Refused)?;
        if !policy.enabled
            || Timestamp::now().as_second().abs_diff(fields.timestamp)
                > policy.max_skew_seconds as u64
        {
            return Err(Refused);
        }
        let secret = self
            .settings
            .open_owned(
                SECRET_OWNER,
                connection.tenant_id,
                connection.id,
                policy.secret.as_ref().ok_or(Refused)?,
            )
            .map_err(|_| Refused)?;
        let secret = secret.as_str().filter(|s| valid_secret(s)).ok_or(Refused)?;
        let signature = data_encoding::HEXLOWER
            .decode(fields.signature.as_bytes())
            .map_err(|_| Refused)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| Refused)?;
        for line in [
            "people-native:v1",
            fields.tenant_text,
            fields.company_text,
            fields.timestamp_text,
            fields.nonce,
            fields.delivery,
        ] {
            mac.update(line.as_bytes());
            mac.update(b"\n");
        }
        mac.update(body);
        mac.verify_slice(&signature).map_err(|_| Refused)
    }
}

fn valid_secret(secret: &str) -> bool {
    (32..=4096).contains(&secret.len()) && !secret.trim().is_empty()
}

struct Claim {
    tenant: i64,
    company: i64,
    connection: i64,
}

impl WebhookHandler for Connector {
    fn verify<'a>(&'a self, delivery: &'a Delivery<'a>) -> WebhookFuture<'a, Verified> {
        Box::pin(async move {
            let fields = Fields::parse(delivery)?;
            let client = self.db.get().await.map_err(|_| Refused)?;
            require_tenant(&**client, fields.tenant, false).await?;
            organisation::require_writable_in(&**client, fields.tenant, fields.company, false)
                .await
                .map_err(|_| Refused)?;
            let connection = connections::find(&**client, fields.tenant, fields.company, false)
                .await
                .map_err(|_| Refused)?
                .ok_or(Refused)?;
            self.verify_native(&**client, &connection, &fields, delivery.body)
                .await?;
            Ok(Box::new(Claim {
                tenant: fields.tenant,
                company: fields.company,
                connection: connection.id,
            }) as Verified)
        })
    }

    fn handle<'a>(
        &'a self,
        delivery: &'a Delivery<'a>,
        verified: Verified,
    ) -> WebhookFuture<'a, ()> {
        Box::pin(async move {
            let claim = verified.downcast::<Claim>().map_err(|_| Refused)?;
            let fields = Fields::parse(delivery)?;
            if fields.tenant != claim.tenant || fields.company != claim.company {
                return Err(Refused);
            }
            let mut client = self.db.get().await.map_err(|_| Refused)?;
            let context = Context {
                tenant: fields.tenant,
                actor: Principal {
                    kind: "guest".into(),
                    id: "0".into(),
                },
                impersonator: None,
                action: "people-connector.webhook.receipt".into(),
                trace_id: uuid::Uuid::new_v4().to_string(),
                agent_self_report: None,
                client_identity: None,
                agent_connection: None,
            };
            let mut tx = self
                .mutations
                .begin(&mut client, context)
                .await
                .map_err(|_| Refused)?;
            require_tenant(tx.sql(), fields.tenant, true).await?;
            organisation::require_writable_in(tx.sql(), fields.tenant, fields.company, true)
                .await
                .map_err(|_| Refused)?;
            let connection = connections::find(tx.sql(), fields.tenant, fields.company, true)
                .await
                .map_err(|_| Refused)?
                .ok_or(Refused)?;
            if connection.id != claim.connection {
                return Err(Refused);
            }
            // Verify again only after obtaining the same locks as policy writes.
            // Rotation, disable, removal and stale timestamps all refuse here.
            self.verify_native(tx.sql(), &connection, &fields, delivery.body)
                .await?;
            serde_json::from_slice::<Notification>(delivery.body).map_err(|_| Refused)?;
            let nonce_hash = digest(fields.nonce.as_bytes());
            let delivery_hash = digest(fields.delivery.as_bytes());
            let body_hash = digest(delivery.body);
            if webhook_store::nonce_used(tx.sql(), &connection, &nonce_hash)
                .await
                .map_err(|_| Refused)?
            {
                return Err(Refused);
            }
            let existing = webhook_store::delivery_body(tx.sql(), &connection, &delivery_hash)
                .await
                .map_err(|_| Refused)?;
            if existing.as_ref().is_some_and(|hash| hash != &body_hash) {
                return Err(Refused);
            }
            let first = existing.is_none();
            webhook_store::receive(
                &mut tx,
                &connection,
                nonce_hash,
                delivery_hash,
                body_hash,
                first,
            )
            .await
            .map_err(|_| Refused)?;
            let who = Attribution::from_context(tx.context()).map_err(|_| Refused)?;
            record_action(tx.sql(),&who,None,"people-connector.webhook",json!({"result":"succeeded",
                "outcome":if first {"received"} else {"duplicate"},"platform_company_id":fields.company}),false).await.map_err(|_| Refused)?;
            tx.commit().await.map_err(|_| Refused)
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Notification {
    #[serde(rename = "event")]
    _event: DirectoryChanged,
}
#[derive(Deserialize)]
enum DirectoryChanged {
    #[serde(rename = "directory.changed")]
    Changed,
}

/// A narrowly bounded tenant liveness read at the machine boundary. There is
/// deliberately no login Scope, account impersonation or tenant authority
/// inferred from a header: the HMAC subsequently authenticates every field.
async fn require_tenant(
    client: &impl GenericClient,
    tenant: i64,
    lock: bool,
) -> std::result::Result<(), Refused> {
    let query = if lock {
        "SELECT id FROM tenants WHERE id=$1 AND status='active' AND deleted_at IS NULL FOR SHARE"
    } else {
        "SELECT id FROM tenants WHERE id=$1 AND status='active' AND deleted_at IS NULL"
    };
    client
        .query_opt(query, &[&tenant])
        .await
        .map_err(|_| Refused)?
        .ok_or(Refused)?;
    Ok(())
}

struct Fields<'a> {
    tenant_text: &'a str,
    company_text: &'a str,
    timestamp_text: &'a str,
    tenant: i64,
    company: i64,
    timestamp: i64,
    nonce: &'a str,
    delivery: &'a str,
    signature: &'a str,
}
impl<'a> Fields<'a> {
    fn parse(request: &'a Delivery<'_>) -> std::result::Result<Self, Refused> {
        if request.method != "POST" {
            return Err(Refused);
        }
        let tenant_text = request.header_str("x-people-tenant").ok_or(Refused)?;
        let company_text = request.header_str("x-people-company").ok_or(Refused)?;
        let timestamp_text = request.header_str("x-people-timestamp").ok_or(Refused)?;
        let nonce = request
            .header_str("x-people-nonce")
            .filter(|v| valid_key(v))
            .ok_or(Refused)?;
        let delivery = request
            .header_str("x-people-delivery")
            .filter(|v| valid_key(v))
            .ok_or(Refused)?;
        let signature = request
            .header_str("x-people-signature")
            .filter(|v| {
                v.len() == 64
                    && v.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            .ok_or(Refused)?;
        Ok(Self {
            tenant: positive_integer(tenant_text)?,
            company: positive_integer(company_text)?,
            timestamp: positive_integer(timestamp_text)?,
            tenant_text,
            company_text,
            timestamp_text,
            nonce,
            delivery,
            signature,
        })
    }
}
fn positive_integer(text: &str) -> std::result::Result<i64, Refused> {
    if !(1..=18).contains(&text.len())
        || text.starts_with('0')
        || !text.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Refused);
    }
    text.parse().map_err(|_| Refused)
}
fn valid_key(text: &str) -> bool {
    (1..=100).contains(&text.len())
        && text.as_bytes()[0].is_ascii_alphanumeric()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}
fn digest(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(bytes))
}
