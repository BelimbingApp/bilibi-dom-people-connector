-- Bilimbi Connector.Migrations.CreateRetentionAttempts, fresh schema.
CREATE TABLE people_connector_retention_attempts (
 id bigserial PRIMARY KEY,
 tenant_id bigint NOT NULL,
 platform_company_id bigint NOT NULL,
 kind varchar(10) NOT NULL CHECK (kind IN ('sync','webhook','nonce','file')),
 record_id varchar(36) NOT NULL,
 attempted_at timestamptz NOT NULL,
 FOREIGN KEY (tenant_id,platform_company_id) REFERENCES companies(tenant_id,id),
 CONSTRAINT people_connector_retention_attempts_identity UNIQUE (platform_company_id,kind,record_id)
);
CREATE INDEX people_connector_sync_runs_retention ON people_connector_sync_runs(connection_id,finished_at);
CREATE INDEX people_connector_webhook_deliveries_retention ON people_connector_webhook_deliveries(connection_id,received_at);
CREATE INDEX people_connector_webhook_nonces_retention ON people_connector_webhook_nonces(connection_id,received_at);
CREATE INDEX people_connector_file_exchanges_retention ON people_connector_file_exchanges(tenant_id,platform_company_id,inserted_at);
