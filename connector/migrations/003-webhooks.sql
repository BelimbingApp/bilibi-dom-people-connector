-- Bilimbi Connector migration 20261002070003: hashed attempts and deliveries.
-- The inbound sender has no login Scope. The company policy therefore lives
-- on our connection, using Settings' owner-secret encryption, rather than
-- company Settings overrides (the deliberate Bilibi storage deviation).
ALTER TABLE people_connector_connections
  ADD COLUMN webhook_secret jsonb,
  ADD COLUMN webhook_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN webhook_max_skew_seconds integer NOT NULL DEFAULT 300,
  ADD CONSTRAINT people_connector_connections_webhook_skew_check
    CHECK (webhook_max_skew_seconds BETWEEN 1 AND 86400),
  ADD CONSTRAINT people_connector_connections_webhook_secret_check
    CHECK (NOT webhook_enabled OR webhook_secret IS NOT NULL),
  ADD CONSTRAINT people_connector_connections_tenant_identity_unique UNIQUE (tenant_id, id);

CREATE TABLE people_connector_webhook_nonces (
  id bigserial PRIMARY KEY,
  tenant_id bigint NOT NULL,
  connection_id bigint NOT NULL,
  nonce_hash varchar(64) NOT NULL CHECK (nonce_hash ~ '^[0-9a-f]{64}$'),
  received_at timestamp(6) NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  CONSTRAINT people_connector_webhook_nonces_connection_fk
    FOREIGN KEY (tenant_id, connection_id)
    REFERENCES people_connector_connections (tenant_id, id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX people_connector_webhook_nonces_identity_unique
  ON people_connector_webhook_nonces (connection_id, nonce_hash);

CREATE TABLE people_connector_webhook_deliveries (
  id bigserial PRIMARY KEY,
  tenant_id bigint NOT NULL,
  connection_id bigint NOT NULL,
  delivery_hash varchar(64) NOT NULL CHECK (delivery_hash ~ '^[0-9a-f]{64}$'),
  body_hash varchar(64) NOT NULL CHECK (body_hash ~ '^[0-9a-f]{64}$'),
  received_at timestamp(6) NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  CONSTRAINT people_connector_webhook_deliveries_connection_fk
    FOREIGN KEY (tenant_id, connection_id)
    REFERENCES people_connector_connections (tenant_id, id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX people_connector_webhook_deliveries_identity_unique
  ON people_connector_webhook_deliveries (connection_id, delivery_hash);
CREATE INDEX people_connector_webhook_deliveries_last_received
  ON people_connector_webhook_deliveries (tenant_id, connection_id, received_at DESC, id DESC);
