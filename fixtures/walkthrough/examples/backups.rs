//! KC6 disposable database proof. DATABASE_URL and BILIBI_BACKUPS_PROOF_ROOT are required.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bilibi_people_connector_walkthrough::backup_walkthrough().await
}
