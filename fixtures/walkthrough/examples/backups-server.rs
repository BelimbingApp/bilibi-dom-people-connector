//! KC6 browser proof: real web host with a fixture-only directory port.
//! Run without People mounted; the fixture port deliberately separates the company axes.
use bilibi_module_registry::{Contribution, Layer, Module, Provenance};
use bilibi_people_connector_connector::DIRECTORY_PORT;
use bilibi_people_connector_walkthrough::BackupCompanyPort;
use bilibi_ports::{Port, PORTS};
inventory::submit! {Contribution::new(PORTS,DIRECTORY_PORT,Provenance{module:"people/workforce",source:file!(),line:line!()},&Port{provider:&BackupCompanyPort})}
inventory::submit! {Module{id:"people/workforce",layer:Layer::Domains,dependencies:&["base/ports"],migrations:&[],services:&[]}}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = bilibi_database::pool(&std::env::var("DATABASE_URL")?)?;
    let app = bilibi_web::App::new(db, &std::env::var("APP_ORIGIN")?)?;
    let listener = tokio::net::TcpListener::bind(std::env::var("BIND_ADDR")?).await?;
    axum::serve(listener, bilibi_web::router(app)).await?;
    Ok(())
}
