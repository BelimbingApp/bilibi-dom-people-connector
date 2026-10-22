//! KC4 browser proof in the connector-only composition, using a fixture-only directory module.
//! DATABASE_URL=... BIND_ADDR=127.0.0.1:3484 APP_ORIGIN=http://localhost:3484 cargo run -p bilibi-people-connector-walkthrough --example files-server
use bilibi_module_registry::{Contribution, Layer, Module, Provenance};
use bilibi_people_connector_connector::DIRECTORY_PORT;
use bilibi_people_connector_walkthrough::FileCompanyPort;
use bilibi_ports::{Port, PORTS};
inventory::submit! {Contribution::new(PORTS,DIRECTORY_PORT,Provenance{module:"people/workforce",source:file!(),line:line!()},&Port{provider:&FileCompanyPort})}
// This descriptor exists only in the fixture executable. The product never
// contributes a People module or port, and composes without either.
inventory::submit! {Module{id:"people/workforce",layer:Layer::Domains,dependencies:&["base/ports"],migrations:&[],services:&[]}}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = bilibi_database::pool(&std::env::var("DATABASE_URL")?)?;
    let origin = std::env::var("APP_ORIGIN")?;
    let app = bilibi_web::App::new(db, &origin)?;
    let listener = tokio::net::TcpListener::bind(std::env::var("BIND_ADDR")?).await?;
    axum::serve(listener, bilibi_web::router(app)).await?;
    Ok(())
}
