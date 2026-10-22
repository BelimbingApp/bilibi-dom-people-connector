//! Bilimbi Connector.FileExchange: private review documents, never a directory write transport.
use crate::{authorize, connections, file_format, file_store as store, people, require_mapping, require_writable, ConnectionState, Connector, ConnectorError, Result, Status, NATIVE_PROVIDER_ID};
use bilibi_artifacts::{Operation, Owner, Reference};
use bilibi_database::{Db, Error};
use bilibi_party_ids::OrganisationId;
use bilibi_settings::{Settings, Target};
use bilibi_tenancy::{ProofClient, ProofFuture, Scope};
use jiff::Timestamp;
use serde::{Deserialize,Serialize};
use serde_json::{json,Value};
use sha2::{Digest,Sha256};
use std::sync::Arc;
use tokio_postgres::GenericClient;

pub(crate) const KEYS:[&str;5]=["people-connector.files.enabled","people-connector.files.json_enabled","people-connector.files.max_bytes","people-connector.files.max_records","people-connector.files.stale_minutes"];
#[derive(Clone,Copy,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct FilePolicy {pub enabled:bool,pub json_enabled:bool,pub max_bytes:i64,pub max_records:i64,pub stale_minutes:i64}
impl FilePolicy {
    fn valid(self)->bool {(1..=10_485_760).contains(&self.max_bytes)&&(1..=100_000).contains(&self.max_records)&&(1..=1440).contains(&self.stale_minutes)}
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct FileReceipt {pub id:String,pub direction:String,pub record_count:i64,pub state:String,pub inserted_at:String,pub artifact_id:Option<String>}
impl FileReceipt {
    pub fn state_label(&self)->&str {match self.state.as_str(){"ready"=>"Completed","pending"=>"In progress","stale"=>"Abandoned","expired"=>"Expired",_=>"Failed"}}
}
#[derive(Clone,Debug)]
pub struct FileSummary {pub policy:FilePolicy,pub records:Vec<FileReceipt>}
#[derive(Clone,Debug)]
pub struct FileExchange {pub receipt:FileReceipt,pub replayed:bool}
fn instant(value:&Value,key:&str)->Option<Timestamp>{value[key].as_str()?.parse().ok()}
fn readable(row:&Value)->bool {row["state"]=="ready" && instant(row,"expires_at").is_some_and(|time|time>Timestamp::now())}
fn stale(row:&Value,minutes:i64)->bool {row["state"]=="pending" && instant(row,"updated_at").is_some_and(|time|Timestamp::now().as_second()-time.as_second()>minutes*60)}
fn present(row:Value,minutes:i64)->Result<FileReceipt>{
    let state=if row["state"]=="ready"&&!readable(&row){"expired"}else if row["failure_reason"]=="stale"||stale(&row,minutes){"stale"}else{row["state"].as_str().unwrap_or("failed")}.to_owned();
    let mut receipt:FileReceipt=serde_json::from_value(row).map_err(|_|ConnectorError::Failed(Error::Internal))?;receipt.state=state;Ok(receipt)
}
async fn policy(settings:&Settings,client:&(impl GenericClient+Sync),scope:&Scope,company:i64)->Result<FilePolicy>{
    let values=settings.resolve_many(client,scope,Target::Company(OrganisationId::new(company)?),&KEYS).await?;
    let get=|key:&str|values.get(key).map(|r|r.value.clone()).unwrap_or(Value::Null);
    serde_json::from_value(json!({"enabled":get(KEYS[0]),"json_enabled":get(KEYS[1]),"max_bytes":get(KEYS[2]),"max_records":get(KEYS[3]),"stale_minutes":get(KEYS[4])})).map_err(|_|ConnectorError::InvalidFile)
}
impl Connector {
    pub async fn file_summary(&self,scope:&Scope,company:i64)->Result<FileSummary>{
        let client=self.db.get().await.map_err(Error::from)?;authorize(&**client,scope,company).await?;
        let policy=policy(&self.settings,&**client,scope,company).await?;
        let records=store::recent(&**client,scope.tenant(),company).await?.into_iter().map(|r|present(r,policy.stale_minutes)).collect::<Result<_>>()?;
        Ok(FileSummary{policy,records})
    }
    pub async fn put_file_policy(&self,scope:&Scope,company:i64,values:FilePolicy)->Result<FilePolicy>{
        let mut client=self.db.get().await.map_err(Error::from)?;authorize(&**client,scope,company).await?;
        if !values.valid(){return Err(ConnectorError::InvalidFile);}
        let mut tx=self.begin(&mut client,scope,"people-connector.files.policy").await?;require_writable(&tx,scope,company).await?;
        connections::find(tx.sql(),scope.tenant(),company,true).await?;
        for (key,value) in KEYS.into_iter().zip([json!(values.enabled),json!(values.json_enabled),json!(values.max_bytes),json!(values.max_records),json!(values.stale_minutes)]){
            self.settings.put(&mut tx,scope,Target::Company(OrganisationId::new(company)?),key,value).await?;
        }
        tx.commit().await?;Ok(values)
    }
    async fn file_gate(&self,client:&(impl GenericClient+Sync),scope:&Scope,company:i64)->Result<(Status,FilePolicy)>{
        authorize(client,scope,company).await?;
        let status=Status::from_workforce(people::company(&self.ports,client,scope,company).await?)?;
        let c=connections::find(client,scope.tenant(),company,false).await?.ok_or(ConnectorError::Disconnected)?;
        require_mapping(&c,&status)?;
        let policy=policy(&self.settings,client,scope,company).await?;
        if !c.enabled || c.provider_id!=NATIVE_PROVIDER_ID || !policy.enabled || !policy.json_enabled {return Err(ConnectorError::FileExchangeDisabled);}
        Ok((Status{state:ConnectionState::Enabled,provider_id:Some(c.provider_id),..status},policy))
    }
    pub async fn import_file(&self,scope:&Scope,company:i64,bytes:&[u8])->Result<FileExchange>{
        let (status,policy)={let client=self.db.get().await.map_err(Error::from)?;self.file_gate(&**client,scope,company).await?};
        check_size(bytes,policy)?;
        let count=file_format::validate(bytes,scope.tenant(),&status,policy.max_records)?;
        self.store_file(scope,status,"import",bytes,count).await
    }
    pub async fn export_file(&self,scope:&Scope,company:i64)->Result<FileExchange>{
        let (status,policy)={let client=self.db.get().await.map_err(Error::from)?;self.file_gate(&**client,scope,company).await?};
        let directory=self.workforce(scope,company).await?;
        if directory.freshness!=crate::Freshness::Current{return Err(ConnectorError::DirectoryUnavailable);}
        let records=directory.value.ok_or(ConnectorError::DirectoryUnavailable)?;
        if records.len()>policy.max_records as usize{return Err(ConnectorError::InvalidFile);}
        let bytes=file_format::encode(scope.tenant(),&status,&records)?;check_size(&bytes,policy)?;
        self.store_file(scope,status,"export",&bytes,records.len()).await
    }
    fn file_owner(&self)->FileOwner {FileOwner{db:self.db.clone(),ports:self.ports.clone(),settings:self.settings.clone()}}
    async fn store_file(&self,scope:&Scope,status:Status,direction:&str,bytes:&[u8],count:usize)->Result<FileExchange>{
        let company=status.platform_company_id;
        let digest=format!("{:x}",Sha256::digest(bytes));
        let mut client=self.db.get().await.map_err(Error::from)?;
        let mut tx=self.begin(&mut client,scope,"people-connector.files.reserve").await?;
        require_writable(&tx,scope,company).await?;
        let c=connections::find(tx.sql(),scope.tenant(),company,true).await?.ok_or(ConnectorError::Disconnected)?;
        let (current,policy)=self.file_gate(tx.sql(),scope,company).await?;require_mapping(&c,&status)?;
        if current.workforce_source_id!=status.workforce_source_id || current.workforce_company_id!=status.workforce_company_id{return Err(ConnectorError::MappingChanged);}
        check_size(bytes,policy)?;
        if count>policy.max_records as usize{return Err(ConnectorError::InvalidFile);}
        let rows=store::matching(tx.sql(),scope.tenant(),c.id,direction,&digest).await?;
        let mut retry=None;
        for before in rows {
            if stale(&before,policy.stale_minutes){let mut after=before.clone();after["state"]=json!("failed");after["failure_reason"]=json!("stale");store::save(&mut tx,Some(before),after).await?;}
            else if readable(&before){tx.commit().await?;return Ok(FileExchange{receipt:present(before,policy.stale_minutes)?,replayed:true});}
            else if before["state"]=="pending" {return Err(ConnectorError::FileExchangeInProgress);}
            else if before["state"]=="failed" && before["failure_reason"].is_null(){retry=Some(before);}
        }
        let mut values=retry.clone().unwrap_or_else(||json!({"id":uuid::Uuid::new_v4().to_string(),"tenant_id":scope.tenant(),"platform_company_id":company,"connection_id":c.id,"workforce_source_id":status.workforce_source_id,"workforce_company_id":status.workforce_company_id,"direction":direction,"sha256":digest,"record_count":count}));
        values["state"]=json!("pending");
        let pending=store::save(&mut tx,retry,values).await?;tx.commit().await?;
        let owner=self.file_owner();let documents=self.artifacts.for_owner(scope,OrganisationId::new(company)?,&owner);
        let reference=Reference{subject:pending["id"].as_str().unwrap().to_owned(),kind:"directory-file".into()};
        let artifact=match documents.put(&mut client,reference,bytes,"application/json").await{
            Ok(artifact)=>artifact,Err(error)=>{self.fail_file(scope,&pending).await?;return Err(error.into());}
        };
        let publication=async {
            let mut tx=self.begin(&mut client,scope,&format!("people-connector.files.{direction}")).await?;
            require_writable(&tx,scope,company).await?;
            let c=connections::find(tx.sql(),scope.tenant(),company,true).await?.ok_or(ConnectorError::Disconnected)?;
            self.file_gate(tx.sql(),scope,company).await?;require_mapping(&c,&status)?;
            let before=store::fetch(tx.sql(),scope.tenant(),company,pending["id"].as_str().unwrap()).await?;
            if before["state"]!="pending" || before["connection_id"].as_i64()!=Some(c.id) || pending["connection_id"].as_i64()!=Some(c.id) || before["updated_at"]!=pending["updated_at"] {return Err(ConnectorError::FileExchangeAbandoned);}
            let mut after=before.clone();after["state"]=json!("ready");after["artifact_id"]=json!(artifact.id);after["expires_at"]=json!(artifact.expires_at);
            if after["expires_at"].is_null(){return Err(ConnectorError::InvalidFile);}
            let ready=store::save(&mut tx,Some(before),after).await?;tx.commit().await?;
            Ok(FileExchange{receipt:present(ready,policy.stale_minutes)?,replayed:false})
        }.await;
        if publication.is_err(){
            // Never remove bytes after an ambiguous publication commit. Read the durable receipt first.
            let stored=store::fetch(&**client,scope.tenant(),company,pending["id"].as_str().unwrap()).await?;
            if stored["artifact_id"]!=artifact.id {documents.delete(&mut client,&artifact.id).await?;self.fail_file(scope,&pending).await?;}
        }
        publication
    }
    async fn fail_file(&self,scope:&Scope,pending:&Value)->Result<()>{
        let company=pending["platform_company_id"].as_i64().unwrap();let mut client=self.db.get().await.map_err(Error::from)?;
        let mut tx=self.begin(&mut client,scope,"people-connector.files.failed").await?;require_writable(&tx,scope,company).await?;
        connections::find(tx.sql(),scope.tenant(),company,true).await?;
        let before=store::fetch(tx.sql(),scope.tenant(),company,pending["id"].as_str().unwrap()).await?;
        if before["state"]=="pending" && before["updated_at"]==pending["updated_at"] {let mut after=before.clone();after["state"]=json!("failed");store::save(&mut tx,Some(before),after).await?;}
        tx.commit().await?;Ok(())
    }
    pub async fn download_file(&self,scope:&Scope,company:i64,id:&str)->Result<bilibi_artifacts::Document>{
        let mut client=self.db.get().await.map_err(Error::from)?;authorize(&**client,scope,company).await?;
        let row=store::fetch(&**client,scope.tenant(),company,id).await?;
        if !readable(&row){return Err(ConnectorError::NotFound);}
        let owner=self.file_owner();Ok(self.artifacts.for_owner(scope,OrganisationId::new(company)?,&owner).read(&mut client,row["artifact_id"].as_str().ok_or(ConnectorError::NotFound)?).await?)
    }
    pub async fn purge_expired_files(&self,scope:&Scope,company:i64)->Result<bilibi_artifacts::Recovery>{
        let mut client=self.db.get().await.map_err(Error::from)?;authorize(&**client,scope,company).await?;
        let owner=self.file_owner();Ok(self.artifacts.for_owner(scope,OrganisationId::new(company)?,&owner).purge_expired(&mut client).await?)
    }
}
fn check_size(bytes:&[u8],policy:FilePolicy)->Result<()>{if bytes.is_empty()||bytes.len()>policy.max_bytes as usize {Err(ConnectorError::InvalidFile)}else{Ok(())}}

// Its stable ID AND concrete Rust type are persisted by Base. Never select it from a request.
struct FileOwner {db:Db,ports:Arc<bilibi_ports::Ports>,settings:Arc<Settings>}
impl Owner for FileOwner {
    fn owner_id(&self)->&'static str {crate::MODULE}
    fn authorize<'a>(&'a self,proof:&'a dyn ProofClient,scope:&'a Scope,company:OrganisationId,operation:Operation,reference:Option<&'a Reference>)->ProofFuture<'a,()>{
        Box::pin(async move{
            // Core's target authorizer takes GenericClient; the proof adapter is object safe.
            let client=self.db.get().await.map_err(Error::from)?;
            authorize(&**client,scope,company.get()).await.map_err(|_|Error::Forbidden)?;
            bilibi_company::organisation::require_writable(&**client,scope,company.get(),false).await.map_err(|_|Error::Forbidden)?;
            if operation==Operation::Purge && reference.is_none(){return Ok(());}
            let reference=reference.filter(|r|r.kind=="directory-file"&&uuid::Uuid::parse_str(&r.subject).is_ok()).ok_or(Error::Forbidden)?;
            let row=proof.query_opt("SELECT to_jsonb(f) FROM people_connector_file_exchanges f WHERE tenant_id=$1 AND platform_company_id=$2 AND id=$3::text::uuid",&[&scope.tenant(),&company.get(),&reference.subject]).await?.ok_or(Error::Forbidden)?;
            let receipt:Value=row.get(0);
            if operation==Operation::Delete {return Ok(());}
            if !matches!(operation,Operation::Create|Operation::Read){return Err(Error::Forbidden);}
            let current=people::company(&self.ports,&**client,scope,company.get()).await.map_err(|_|Error::Forbidden)?.require_current().map_err(|_|Error::Forbidden)?;
            let policy=policy(&self.settings,&**client,scope,company.get()).await.map_err(|_|Error::Forbidden)?;
            if !policy.enabled || !policy.json_enabled{return Err(Error::Forbidden);}
            let c=proof.query_opt("SELECT id,workforce_source_id,workforce_company_id FROM people_connector_connections WHERE tenant_id=$1 AND platform_company_id=$2 AND enabled AND provider_id=$3",&[&scope.tenant(),&company.get(),&NATIVE_PROVIDER_ID]).await?.ok_or(Error::Forbidden)?;
            let expected=if operation==Operation::Create{"pending"}else{"ready"};
            if receipt["state"]!=expected || receipt["connection_id"].as_i64()!=Some(c.get(0))
                || receipt["workforce_source_id"].as_str()!=Some(&current.reference.source_id)
                || receipt["workforce_company_id"].as_i64()!=Some(current.workforce_company_id)
                || c.get::<_,String>(1)!=current.reference.source_id || c.get::<_,i64>(2)!=current.workforce_company_id {return Err(Error::Forbidden);}
            Ok(())
        })
    }
}
