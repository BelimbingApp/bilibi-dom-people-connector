//! KC2 browser proof; fixture contributions are never linked into product builds.
use bilibi_module_registry::{Contribution, Layer, Module, Provenance};
use bilibi_people_connector_connector::{
    Adapter, ADAPTERS, DIRECTORY_PORT, MODULE, NATIVE_PROVIDER_ID,
};
use bilibi_people_connector_walkthrough::{BackupCompanyPort, ConnectionsDirectory};
use bilibi_ports::{Port, PORTS};
inventory::submit! {Module{id:"people/workforce",layer:Layer::Domains,dependencies:&["base/ports"],migrations:&[],services:&[]}}
inventory::submit! {Contribution::new(PORTS,DIRECTORY_PORT,Provenance{module:"people/workforce",source:file!(),line:line!()},&Port{provider:&BackupCompanyPort})}
inventory::submit! {Contribution::new(ADAPTERS,NATIVE_PROVIDER_ID,Provenance{module:MODULE,source:file!(),line:line!()},&Adapter{provider_id:NATIVE_PROVIDER_ID,port:&ConnectionsDirectory})}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = bilibi_database::pool(&std::env::var("DATABASE_URL")?)?;
    let app = bilibi_web::App::new(db, &std::env::var("APP_ORIGIN")?)?;
    let listener = tokio::net::TcpListener::bind(std::env::var("BIND_ADDR")?).await?;
    axum::serve(listener, bilibi_web::router(app)).await?;
    Ok(())
}
