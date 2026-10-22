//! Bilimbi Connector.FileExchange: immutable v1/v2 wire formats, including duplicate-key refusal.
use crate::{ConnectorError, RecordKind, Result, Status, WorkforceRecord};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{json, Map, Value};
use std::{collections::BTreeSet, fmt};
const BASE: &[&str] = &[
    "kind",
    "source_id",
    "stable_id",
    "workforce_company_id",
    "name",
    "code",
    "email",
    "supervisor_stable_id",
    "observed_at",
    "active",
];
const POSITION: &[&str] = &[
    "parent_stable_id",
    "version",
    "vacant",
    "assignments_incomplete",
    "assignments",
];

pub(crate) fn encode(tenant: i64, status: &Status, records: &[WorkforceRecord]) -> Result<Vec<u8>> {
    let positions = records.iter().any(|r| r.kind == RecordKind::Position);
    let records: Vec<_> = records
        .iter()
        .map(|r| {
            let mut value = serde_json::to_value(r).expect("serializable directory fact");
            if r.kind != RecordKind::Position {
                for key in POSITION {
                    value.as_object_mut().unwrap().remove(*key);
                }
            }
            value
        })
        .collect();
    serde_json::to_vec(&json!({"format":if positions {"people-directory-v2"} else {"people-directory-v1"}, "tenant_id":tenant,
        "platform_company_id":status.platform_company_id,"workforce_source_id":status.workforce_source_id,
        "workforce_company_id":status.workforce_company_id,"records":records})).map_err(|_| ConnectorError::InvalidFile)
}

pub(crate) fn validate(
    bytes: &[u8],
    tenant: i64,
    status: &Status,
    max_records: i64,
) -> Result<usize> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = Unique::deserialize(&mut deserializer)
        .map_err(|_| ConnectorError::InvalidFile)?
        .0;
    deserializer
        .end()
        .map_err(|_| ConnectorError::InvalidFile)?;
    let invalid = || ConnectorError::InvalidFile;
    let envelope = value.as_object().ok_or_else(invalid)?;
    let keys = [
        "format",
        "tenant_id",
        "platform_company_id",
        "workforce_source_id",
        "workforce_company_id",
        "records",
    ];
    if envelope.len() != keys.len()
        || !keys.iter().all(|key| envelope.contains_key(*key))
        || value["tenant_id"].as_i64() != Some(tenant)
        || value["platform_company_id"].as_i64() != Some(status.platform_company_id)
        || value["workforce_source_id"].as_str() != Some(&status.workforce_source_id)
        || value["workforce_company_id"].as_i64() != Some(status.workforce_company_id)
    {
        return Err(invalid());
    }
    let v2 = match value["format"].as_str() {
        Some("people-directory-v1") => false,
        Some("people-directory-v2") => true,
        _ => return Err(invalid()),
    };
    let records = value["records"].as_array().ok_or_else(invalid)?;
    if records.len() > max_records as usize {
        return Err(invalid());
    }
    let mut identities = BTreeSet::new();
    for value in records {
        let record = value.as_object().ok_or_else(invalid)?;
        let position = value["kind"] == "position";
        if position && !v2
            || !BASE.iter().all(|k| record.contains_key(*k))
            || record
                .keys()
                .any(|k| !BASE.contains(&k.as_str()) && !(v2 && POSITION.contains(&k.as_str())))
        {
            return Err(invalid());
        }
        let parsed: WorkforceRecord =
            serde_json::from_value(value.clone()).map_err(|_| invalid())?;
        if !parsed.valid()
            || parsed.source_id != status.workforce_source_id
            || parsed.workforce_company_id != status.workforce_company_id
            || !identities.insert((parsed.kind, parsed.stable_id))
        {
            return Err(invalid());
        }
    }
    Ok(records.len())
}

// serde_json::Value otherwise silently keeps the final occurrence of an object key.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Json;
        impl<'de> Visitor<'de> for Json {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut access: M,
            ) -> std::result::Result<Unique, M::Error> {
                let mut map = Map::new();
                while let Some((key, Unique(value))) = access.next_entry::<String, Unique>()? {
                    if map.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate key"));
                    }
                }
                Ok(Unique(Value::Object(map)))
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> std::result::Result<Unique, S::Error> {
                let mut values = Vec::new();
                while let Some(Unique(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(Unique(Value::Array(values)))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
        }
        d.deserialize_any(Json)
    }
}
