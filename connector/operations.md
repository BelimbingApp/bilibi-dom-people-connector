# Connection health and retention

Use `/integrations/people/operations` to check a company's connection and remove old operational records.

Choose a company, then select **Run health check**. The findings show connection configuration, provider contract, credential presence, adapter availability, directory freshness, the last sync, timed-out and unsuccessful passes, open issues, webhook intake, and private file storage and exchange counts. The check records `people-connector.doctor` in audit. It does not repair records, synchronise, accept a notification, or exchange files. A broken workforce mapping becomes a finding so you can diagnose it. Secret values, employee details and private storage paths never appear in the report or its audit action.

## Keep or remove records

The three retention periods start unset, which keeps all records. Edit a period in place to set the number of days to retain. Clear it to keep records again. Changes apply to the next purge.

| Setting | Initial value | Allowed values |
| --- | --- | --- |
| `people-connector.retention.sync_days` | Unset | 1–3650 days or unset |
| `people-connector.retention.webhook_days` | Unset | 1–3650 days or unset |
| `people-connector.retention.file_days` | Unset | 1–3650 days or unset |
| `people-connector.retention.batch_size` | 100 | 1–1000 per record type |
| `people-connector.retention.retry_minutes` | 60 | 1–1440 minutes |

Select **Purge eligible records** and confirm the company. **Purging removes replay history for old requests.** Later requests can no longer be deduplicated against deleted history.

The purge preserves running syncs, each connection's latest run and latest successful run, pending file exchanges, checkpoints, directory projections, reconciliation issues and audit history. A sync run must have finished before its cutoff. Webhook deliveries and nonces stay for the greater of the configured period and twice the connection's signing skew. File receipts must be old enough and their bytes expired; Artifacts must confirm cleanup before the receipt can be deleted, including after a connection is removed.

Each record has its own audited transaction. The purge checks authorization, company writability and eligibility again under the company and connection locks. A failed deletion rolls back that record and creates a retry hold in `people_connector_retention_attempts`. Later candidates still run. The page shows failed record types and IDs with fixed next steps; it never shows database or storage exception text. A held record becomes eligible for another attempt after the retry delay. Successful deletion removes its hold too.

## Facade calls

Call `Connector::run_doctor(scope, platform_company_id)`, `retention_policy`, `put_retention_policy` (partial JSON field map), or `purge`. Each requires `people-connector.connections.manage` and company reach. Policy updates reject unknown fields, non-integers and values outside the ranges above before writing anything. The page rechecks access on every write event, including opening the purge confirmation; the facade checks inside its mutation transaction again.

The Operations page and retention remain available when People is absent. The doctor reports unavailable company identity, while cleanup operates on the connector's existing company-owned history.

See [the connector contract](README.md), [file exchange](README.md#directory-file-exchange), and the [disposable walkthrough](../fixtures/walkthrough/src/operations.rs).
