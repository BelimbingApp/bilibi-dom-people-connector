//! Bilimbi `PeopleConnector.NativePeopleAdapter`, served through the optional
//! `people/workforce.directory` port. No People crate, SQL or startup registry.
mod cursor;
mod directory;

use bilibi_module_registry::{Contribution, Layer, Module, Provenance};
use bilibi_people_connector_connector::{
    contracts::{EMPLOYEE_DIRECTORY, ORGANIZATION_DIRECTORY},
    Adapter, Direction, Freshness, Page, PortAuthorization, PortContext, PortRequest, ReadFuture,
    ReadPort, ReadRefusal, ADAPTERS, NATIVE_PROVIDER_ID, NATIVE_SOURCE_ID,
};
use jiff::Timestamp;

pub const MODULE: &str = "people_connector/native_people_adapter";

/// The native provider's company/employee and organisation read adapter.
/// The Connector supplies the scope, client and explicit company mapping.
pub struct NativePeopleAdapter;

inventory::submit! {
    Module {
        id: MODULE,
        layer: Layer::Domains,
        dependencies: &[
            "base/database", "base/module_registry", "base/ports", "base/tenancy",
            "people_connector/connector",
        ],
        migrations: &[],
        services: &[],
    }
}
inventory::submit! {
    Contribution::new(ADAPTERS, NATIVE_PROVIDER_ID,
        Provenance { module: MODULE, source: file!(), line: line!() },
        &Adapter { provider_id: NATIVE_PROVIDER_ID, port: &NativePeopleAdapter })
}

impl ReadPort for NativePeopleAdapter {
    fn read<'a>(
        &'a self,
        context: PortContext<'a>,
        authorization: &'a PortAuthorization<'a>,
        request: &'a PortRequest,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            let a = authorization;
            if a.provider_id != NATIVE_PROVIDER_ID
                || a.workforce_source_id != NATIVE_SOURCE_ID
                || a.direction != Direction::Read
                || ![EMPLOYEE_DIRECTORY, ORGANIZATION_DIRECTORY].contains(&a.capability)
                || a.platform_company_id <= 0
                || a.workforce_company_id <= 0
            {
                return Err(ReadRefusal::InvalidAuthorization);
            }
            if request.limit == 0 {
                return Err(ReadRefusal::InvalidRequest);
            }
            let cursor = cursor::Cursor::read(a, request)?;
            let at = cursor.as_of()?;
            match directory::read(context, a, request, cursor).await {
                Ok(page) => Ok(page),
                Err(directory::Failure::Refused(error)) => Err(error),
                Err(directory::Failure::NotCurrent(freshness)) => Ok(Page {
                    entries: vec![],
                    next_cursor: None,
                    resume_cursor: None,
                    as_of: at,
                    freshness,
                    snapshot: false,
                }),
            }
        })
    }
}

fn now() -> Timestamp {
    Timestamp::from_microsecond(Timestamp::now().as_microsecond())
        .expect("microseconds of a valid timestamp")
}

fn current(entries: Vec<serde_json::Value>, at: Timestamp, next_cursor: Option<String>) -> Page {
    Page {
        entries,
        next_cursor,
        resume_cursor: Some(at.to_string()),
        as_of: at,
        freshness: Freshness::Current,
        snapshot: true,
    }
}
