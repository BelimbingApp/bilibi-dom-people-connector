-- Bilimbi Connector.Migrations.CreateFileExchanges; private bytes belong to Artifacts.
CREATE TABLE people_connector_file_exchanges (
  id uuid PRIMARY KEY,
  audit_id bigserial UNIQUE NOT NULL,
  tenant_id bigint NOT NULL,
  platform_company_id bigint NOT NULL,
  connection_id bigint REFERENCES people_connector_connections(id) ON DELETE SET NULL,
  workforce_source_id varchar(100) NOT NULL,
  workforce_company_id bigint NOT NULL,
  direction varchar(10) NOT NULL,
  sha256 varchar(64) NOT NULL,
  record_count integer NOT NULL,
  state varchar(10) NOT NULL,
  artifact_id varchar(100),
  expires_at timestamptz,
  failure_reason varchar(20),
  inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (tenant_id, platform_company_id) REFERENCES companies(tenant_id,id),
  CONSTRAINT people_connector_file_exchange_values CHECK (
    direction IN ('import','export') AND state IN ('pending','ready','failed')
    AND record_count >= 0 AND workforce_company_id > 0
    AND sha256 ~ '^[0-9a-f]{64}$'
    AND (state <> 'ready' OR (artifact_id IS NOT NULL AND expires_at IS NOT NULL))
    AND (failure_reason IS NULL OR (state = 'failed' AND failure_reason = 'stale'))
  )
);
CREATE UNIQUE INDEX people_connector_file_exchanges_pending_unique
  ON people_connector_file_exchanges(connection_id,direction,sha256) WHERE state='pending';
CREATE INDEX people_connector_file_exchanges_replay
  ON people_connector_file_exchanges(connection_id,direction,sha256);
CREATE INDEX people_connector_file_exchanges_company
  ON people_connector_file_exchanges(tenant_id,platform_company_id,inserted_at DESC);
