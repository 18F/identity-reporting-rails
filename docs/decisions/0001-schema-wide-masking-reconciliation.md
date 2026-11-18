---
title: "Reuse dbt policies for schema-wide attachment reconciliation"
description: "Separate configured-column masking from periodic dbt-schema repairs"
status: draft
decision_status: proposed
tier: 2
date: "2026-11-18"
category: data-handling
nist_controls: ["AC-3", "AC-6", "SA-11", "AU-3"]
---

# Reuse dbt policies for schema-wide attachment reconciliation

## Context and problem statement

dbt's transactional post-hook protects built models, but unselected and orphaned
physical tables can retain missing attachments. Grantor-based preservation alone
does not repair them or recognize repairs created by the Rails admin identity.

## Decision drivers

- Cover all physical columns in explicitly configured dbt-owned schemas.
- Preserve configured source-column protections and avoid competing definitions.
- Keep role membership as the intended unmasked-access contract.
- Avoid explicitly locking healthy tables during a five-minute backstop.

## Considered options

1. Expand dbt's run-end process to discover every schema table.
2. Have Rails create a second set of masking policies.
3. Reuse existing dbt policies in a separate Rails attachment reconciliation.

## Proposed decision

Use option 3. `sync_configured_columns` manages schemas present in `mask.yaml`;
`sync_dbt_schema(schema:)` manages one schema from `config/dbt_masking.yaml`.
One separate scheduled dbt job explicitly invokes each configured schema.
Rails never creates, alters, or drops shared dbt policies. Expected attachments
are PUBLIC mask at 0 and ROLE `pii_viewers` identity unmask at `dbt_unmask_priority` (default 100), independent
of attachment grantor. Transitional direct-user attachments are left alone.

Discover drift first. Only repair candidates receive a short per-table transaction
and ACCESS EXCLUSIVE lock, followed by fresh discovery and correction. SQL errors
propagate. No orphan tables are deleted and no session cache is needed.

## Consequences

- Healthy tables incur metadata checks but no explicit repair lock.
- Repair candidates can briefly block reads/writes; concurrent DDL may fail.
- Configured-column sync does not detach marts repairs outside its schema scope.
- Missing policies and unsupported type/size mappings fail rather than silently
  skipping columns. Policy-expression equivalence is an operator prerequisite.
- GoodJob limits overlapping scheduled Rails jobs; it does not coordinate dbt or
  direct console invocations. Table-level locking is the proposed concurrency
  mechanism, subject to human-only live QA verification.
- The direct-user transition and policy-definition changes belong to dbt, not
  this Rails change. Human review is required before deployment.

## Links

- [QA validation](../dbt-masking-validation.md)
- [Job architecture](../jobs.md)
