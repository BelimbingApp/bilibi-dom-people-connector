//! KC5 proof: DATABASE_URL=... PGPASSFILE=... BILIBI_OPERATIONS_PROOF_ROOT=... cargo run -p bilibi-people-connector-walkthrough --example operations
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bilibi_people_connector_walkthrough::operations_walkthrough().await
}
