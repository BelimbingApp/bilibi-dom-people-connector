//! KV1 composed-system sweep; see connector/sweep.md. No fixture adapter registrations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bilibi_people_connector_walkthrough::sweep(
        &std::env::var("DATABASE_URL")?,
        &std::env::args().nth(1).unwrap_or_else(|| "mounted".into()),
    )
    .await
}
