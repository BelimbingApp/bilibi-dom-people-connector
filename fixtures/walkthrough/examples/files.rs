//! KC4: DATABASE_URL=... BILIBI_FILES_PROOF_ROOT=/private/directory cargo run -p bilibi-people-connector-walkthrough --example files
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bilibi_people_connector_walkthrough::file_walkthrough().await
}
