//! Adapter cursor around Workforce's opaque cursors. Mirrors Bilimbi's
//! watermark cursor, with explicit tenant, both company axes and stream bounds.
use bilibi_people_connector_connector::{PortAuthorization, PortRequest, ReadRefusal};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cursor {
    version: u8,
    tenant: i64,
    platform: i64,
    workforce: i64,
    stream: String,
    limit: usize,
    watermark: String,
    pub inner: Option<String>,
}
impl Cursor {
    pub fn read(a: &PortAuthorization<'_>, r: &PortRequest) -> Result<Self, ReadRefusal> {
        let Some(encoded) = r.cursor.as_ref() else {
            return Ok(Self {
                version: 1,
                tenant: a.scope.tenant(),
                platform: a.platform_company_id,
                workforce: a.workforce_company_id,
                stream: a.capability.into(),
                limit: r.limit,
                watermark: crate::now().to_string(),
                inner: None,
            });
        };
        if encoded.len() > 8192 {
            return Err(ReadRefusal::InvalidCursor);
        }
        let cursor: Self = serde_json::from_str(encoded).map_err(|_| ReadRefusal::InvalidCursor)?;
        if cursor.version != 1
            || cursor.tenant != a.scope.tenant()
            || cursor.platform != a.platform_company_id
            || cursor.workforce != a.workforce_company_id
            || cursor.stream != a.capability
            || cursor.limit != r.limit
        {
            return Err(ReadRefusal::InvalidCursor);
        }
        cursor.as_of()?;
        Ok(cursor)
    }
    pub fn as_of(&self) -> Result<Timestamp, ReadRefusal> {
        self.watermark
            .parse()
            .map_err(|_| ReadRefusal::InvalidCursor)
    }
    pub fn continued(mut self, inner: Option<String>) -> Result<String, ReadRefusal> {
        self.inner = inner;
        serde_json::to_string(&self).map_err(|_| ReadRefusal::UnexpectedResult)
    }
}
