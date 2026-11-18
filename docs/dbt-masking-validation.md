---
title: "Validate schema-wide dbt masking"
description: "Human-only QA checks for attachment reconciliation without returning warehouse PII"
status: draft
tier: 2
---

# Validate schema-wide dbt masking

These checks are for a human operator in an approved QA environment. The AI agent
does not execute them. Use the deployed branch's admin worker identity, verify
the database/environment first, and do not SELECT warehouse data values or paste
PII into logs. A Rails console's read-only banner does not protect Redshift.

## Prerequisites

- dbt has created the policies named in `config/dbt_masking.yaml`. Verify their
  signatures and expressions against the reviewed dbt definitions; Rails checks
  names and conservative column compatibility, not expression equivalence.
- `pii_viewers` exists with the approved membership. Direct-user unmask attachments
  are removed separately; this reconciliation does not remove them or prove that
  role membership is the only source of unmasked access.
- Choose **only `fraudops_qa_marts`** for this exercise. The scheduled dbt job
  explicitly runs both configured scopes; do not invoke that job for a QA-only test.

## Inventory and coverage (metadata only)

```sql
SELECT current_database();

SELECT policy_name, input_columns, policy_expression
FROM svv_masking_policy
WHERE policy_database = current_database()
  AND policy_name LIKE 'dbt_%'
ORDER BY policy_name;

SELECT table_name, column_name, data_type, character_maximum_length,
       numeric_precision, numeric_scale
FROM svv_columns
WHERE table_catalog = current_database()
  AND table_schema = 'fraudops_qa_marts'
ORDER BY table_name, ordinal_position;

SELECT schema_name, table_name, policy_name, output_columns,
       grantee, grantee_type, priority, grantor
FROM svv_attached_masking_policy
WHERE schema_name IN ('fraudops_qa_marts', 'fraudops_marts')
ORDER BY schema_name, table_name, output_columns, grantee_type, grantee, priority;
```

Save the production-schema metadata as a comparison baseline. Compare each QA
physical table's columns against the type mapping: each supported column requires
one PUBLIC mask at 0 and one `pii_viewers` role identity unmask at
`dbt_unmask_priority` (default 100). Check
materialized views separately in `svv_mv_info`; they fail preflight. Plain views
are excluded with a warning, not certified as protected. Unsupported types,
short strings, CHAR, SMALLINT, timezone-aware timestamps, and decimals other than
NUMERIC(18,0) fail preflight rather than being coerced.

## Synthetic orphan repair

With explicit operator approval, create a disposable, non-temporary QA table
with no dbt model and no PII:

```sql
CREATE TABLE fraudops_qa_marts.masking_reconciliation_probe (
  probe_text VARCHAR(256), probe_number INTEGER, probe_flag BOOLEAN
);
INSERT INTO fraudops_qa_marts.masking_reconciliation_probe
VALUES ('synthetic-only', 7, true);
```

From the deployed Rails admin console, run:

```ruby
RedshiftMaskingSync.new.sync_dbt_schema(schema: 'fraudops_qa_marts')
```

Verify six expected attachments for the probe, with the Rails identity as grantor.
Run the same method again: expect zero repair candidates and no ATTACH/DETACH/LOCK
statements. Run `sync_configured_columns` separately and confirm the probe's
attachments survive. Compare production-schema metadata with the baseline: no
changes from the QA-scoped invocation.

Using approved, separate non-admin sessions for a role member and a non-member,
inspect the effective policy names without returning data:

```sql
SELECT mask_get_policy_for_user_on_column(
  'fraudops_qa_marts', 'masking_reconciliation_probe', 'probe_text',
  '<approved-test-user>'
);
```

Expect the unmask policy for a role member and the mask policy for a non-member
without another override. Any actual read test must target the synthetic probe
only, never `decrypted_events` or another warehouse table containing PII.

## Concurrent dbt build and failure visibility

Use a disposable dbt model with synthetic values. Overlap its transactional
rebuild/post-hook with QA reconciliation. Confirm that healthy tables are not
explicitly locked and that a detected repair candidate is re-inspected after
LOCK, not repaired from the initial snapshot. Observe blocking with metadata
tools, not application row queries.

In another approved session, hold a transaction lock on the synthetic probe;
remove one probe attachment in advance so it becomes a repair candidate. Verify
the reconciliation raises on the bounded statement timeout and does not report
completion. Release the operator-held lock, then invoke reconciliation afresh.
The 5-second timeout is **per statement**, not a total transaction deadline.

Validate that a failed correction rolls back that table's detach/attach sequence
and releases its lock. Previously committed repairs on other tables may remain.
Verify failure is visible in GoodJob when running the scheduled job in an
approved environment. These Redshift DDL/locking guarantees require live QA;
mocked specs do not establish them.

The operator owns removal of disposable fixtures after approval. Rails does not
delete orphan tables or drop policies. Do not roll back by removing real masking
attachments; disable the new scheduled job through normal change control if
needed, leaving existing protections intact.
