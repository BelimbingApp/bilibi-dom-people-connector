//! Run with DATABASE_URL on an empty disposable database and BILIBI_SETTINGS_KEY.
//! Optional WEBHOOK_PROOF_PORT also exercises the running host by HTTP.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bilibi_people_connector_walkthrough::run_webhooks(&std::env::var("DATABASE_URL")?).await
}
