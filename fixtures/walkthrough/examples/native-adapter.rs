//! KA1 disposable-database proof. No People crate, SQL or product fixtures.
//! Run with `--live` after People's `workforce-settings -- setup` to exercise
//! the mounted directory. Without it, install the Platform fixture and inject
//! directory-port answers to exercise mapping, interrupted reads and refusals.
#[tokio::main]
async fn main() -> bilibi_database::Result<()> {
    bilibi_people_connector_walkthrough::native_adapter(
        &std::env::var("DATABASE_URL").map_err(|_| bilibi_database::Error::Invalid)?,
        std::env::args().any(|s| s == "--live"),
    )
    .await
}
