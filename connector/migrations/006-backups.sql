-- Bilimbi Connector.Backup.Record; receipts survive removal of their connection.
CREATE TABLE people_connector_backups (
 id uuid PRIMARY KEY, audit_id bigserial UNIQUE NOT NULL,
 tenant_id bigint NOT NULL, platform_company_id bigint NOT NULL,
 connection_id bigint,
 sha256 varchar(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
 state varchar(10) NOT NULL CHECK (state IN ('pending','ready','failed','purged')),
 artifact_id text, expires_at timestamptz NOT NULL,
 preview_token_hash varchar(64), preview_state_hash varchar(64),
 preview_actor_id bigint, preview_impersonator_id bigint, preview_expires_at timestamptz,
 recovery_generation bigint CHECK (recovery_generation > 0), restored_at timestamptz,
 purge_attempts integer NOT NULL DEFAULT 0 CHECK (purge_attempts >= 0),
 purge_last_error varchar(255), purge_attempted_at timestamptz, purge_held_at timestamptz,
 inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK (state NOT IN ('ready','purged') OR artifact_id IS NOT NULL),
 FOREIGN KEY (tenant_id,connection_id) REFERENCES people_connector_connections(tenant_id,id) ON DELETE SET NULL (connection_id)
);
CREATE INDEX people_connector_backups_company_expiry ON people_connector_backups(tenant_id,platform_company_id,expires_at,id);
-- An empty restored checkpoint must still fence late results and recovery requests.
ALTER TABLE people_connector_sync_checkpoints ADD COLUMN bootstrap_required boolean NOT NULL DEFAULT false;
