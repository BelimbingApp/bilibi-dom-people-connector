-- Connector 001: a company's provider connection, as Bilimbi's
-- `20261002070001_create_people_connector_connections` leaves it, plus the
-- scoped foreign key into Core Company that keeps `tenant_id` true for good.
--
-- One row is one connection. Both company axes are explicit:
-- `platform_company_id` is the Core Company in `tenant_id`, and
-- `workforce_source_id` with `workforce_company_id` is the People Workforce
-- identity the connection was mapped to when its provider was chosen. A
-- platform company has at most one connection, and one workforce company
-- identity backs at most one platform company in a tenant. The provider's
-- credential is not here: it is the encrypted company-scoped Base Setting
-- `people-connector.connection.credential`.

CREATE TABLE people_connector_connections (
  id bigserial PRIMARY KEY,
  tenant_id bigint NOT NULL,
  platform_company_id bigint NOT NULL,
  provider_id varchar(100) NOT NULL,
  provider_contract_version varchar(20) NOT NULL,
  workforce_source_id varchar(100) NOT NULL,
  workforce_company_id bigint NOT NULL,
  enabled boolean NOT NULL DEFAULT false,
  inserted_at timestamp(0) NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  updated_at timestamp(0) NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  CONSTRAINT people_connector_connections_platform_company_unique
    UNIQUE (platform_company_id),
  CONSTRAINT people_connector_connections_workforce_company_unique
    UNIQUE (tenant_id, workforce_source_id, workforce_company_id),
  -- The platform company is a company of this tenant.
  CONSTRAINT people_connector_connections_company_fk
    FOREIGN KEY (tenant_id, platform_company_id) REFERENCES companies (tenant_id, id)
);

CREATE INDEX people_connector_connections_tenant_id_index
  ON people_connector_connections (tenant_id);
