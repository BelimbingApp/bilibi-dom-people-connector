-- Bilimbi Connector sync tables and organisation projection, fresh schema.
CREATE UNIQUE INDEX people_connector_connections_tenant_identity ON people_connector_connections (tenant_id, id);
CREATE TABLE people_connector_sync_checkpoints (
 id bigserial PRIMARY KEY, tenant_id bigint NOT NULL, connection_id bigint NOT NULL,
 version bigint NOT NULL CHECK (version > 0), resume_cursor varchar(1000), as_of_at timestamptz NOT NULL,
 inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (connection_id), FOREIGN KEY (tenant_id, connection_id) REFERENCES people_connector_connections (tenant_id, id) ON DELETE CASCADE
);
CREATE TABLE people_connector_sync_runs (
 id bigserial PRIMARY KEY, tenant_id bigint NOT NULL, connection_id bigint NOT NULL,
 platform_company_id bigint NOT NULL, provider_id varchar(100) NOT NULL,
 idempotency_key varchar(100) NOT NULL, pass varchar(20) NOT NULL CHECK (pass IN ('bootstrap','incremental')),
 state varchar(20) NOT NULL CHECK (state IN ('running','succeeded','stale','unavailable','refused','failed','unknown')),
 reason varchar(60), checkpoint_version bigint, as_of_at timestamptz,
 applied integer NOT NULL DEFAULT 0 CHECK (applied >= 0), unchanged integer NOT NULL DEFAULT 0 CHECK (unchanged >= 0),
 superseded integer NOT NULL DEFAULT 0 CHECK (superseded >= 0), deactivated integer NOT NULL DEFAULT 0 CHECK (deactivated >= 0), refused integer NOT NULL DEFAULT 0 CHECK (refused >= 0),
 started_at timestamptz NOT NULL, finished_at timestamptz,
 inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 CONSTRAINT people_connector_sync_runs_idempotency_unique UNIQUE (connection_id, idempotency_key),
 FOREIGN KEY (tenant_id, connection_id) REFERENCES people_connector_connections (tenant_id, id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX people_connector_sync_runs_one_running ON people_connector_sync_runs (connection_id) WHERE state = 'running';
CREATE TABLE people_connector_workforce_records (
 id bigserial PRIMARY KEY, tenant_id bigint NOT NULL, connection_id bigint NOT NULL,
 kind varchar(20) NOT NULL CHECK (kind IN ('company','employee','position')),
 source_id varchar(100) NOT NULL, stable_id varchar(100) NOT NULL, workforce_company_id bigint NOT NULL CHECK (workforce_company_id > 0),
 active boolean NOT NULL, name varchar(255) NOT NULL, code varchar(100) NOT NULL,
 email varchar(255), supervisor_stable_id varchar(100), parent_stable_id varchar(100), version bigint CHECK (version > 0),
 vacant boolean, assignments_incomplete boolean, assignments jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(assignments) = 'array'),
 content_hash varchar(64) NOT NULL, observed_at timestamptz NOT NULL, deactivated_at timestamptz,
 inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 CONSTRAINT people_connector_workforce_records_identity_unique UNIQUE (connection_id, kind, source_id, stable_id),
 FOREIGN KEY (tenant_id, connection_id) REFERENCES people_connector_connections (tenant_id, id) ON DELETE CASCADE
);
CREATE TABLE people_connector_reconciliation_issues (
 id bigserial PRIMARY KEY, tenant_id bigint NOT NULL, connection_id bigint NOT NULL,
 issue_key varchar(200) NOT NULL, kind varchar(40) NOT NULL, reason varchar(60) NOT NULL,
 severity varchar(10) NOT NULL CHECK (severity IN ('warning','error')), record_kind varchar(20), stable_id varchar(100),
 status varchar(10) NOT NULL CHECK (status IN ('open','resolved')), occurrences integer NOT NULL DEFAULT 1 CHECK (occurrences > 0),
 first_seen_at timestamptz NOT NULL, last_seen_at timestamptz NOT NULL, resolved_at timestamptz,
 inserted_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 CONSTRAINT people_connector_reconciliation_issues_key_unique UNIQUE (connection_id, issue_key),
 FOREIGN KEY (tenant_id, connection_id) REFERENCES people_connector_connections (tenant_id, id) ON DELETE CASCADE
);
