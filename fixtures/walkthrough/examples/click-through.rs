//! KU1 live page proofs; see connector/sweep.md for the disposable setup.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bilibi_people_connector_walkthrough::click_through(&std::env::var("DATABASE_URL")?).await
}
