---
title: "Audit Redshift roles against redshift_config.yaml inside RedshiftSync"
description: "Detect, and optionally revoke, role membership, inheritance and privilege drift"
status: draft
decision_status: proposed
tier: 2
date: "2026-11-18"
category: access-control
nist_controls: ["AC-2", "AC-3", "AC-6", "AU-6", "CM-3"]
---

# Audit Redshift roles against redshift_config.yaml inside RedshiftSync

## Context and problem statement

`RedshiftSync` makes configured roles hold the configured users, but it never
looks at who else holds them. `pii_viewers` unmasks PII, so an extra member, a
nested role or a role that inherits it is a real exposure. Roles whose feature
flag is off are skipped entirely, so their stale members keep access.

## Decision drivers

- Cover every role in the config, `pii_viewers` first.
- Redshift has no DENY: grants from users, groups and roles add up, so the only
  correction is `REVOKE` from a managed role.
- A human should review real findings before anything is revoked automatically.
- No new keys in `redshift_config.yaml`, no new job, no dependency upgrades.

## Considered options

1. A separate scheduled audit job.
2. Detect only, never revoke.
3. Audit inside `RedshiftSync`, always log, revoke behind a flag.

## Proposed decision

Use option 3. `RedshiftRoleAudit` runs from `sync_cluster` (cluster-wide views) and
`sync_database_grants` (per-database privilege views), on the existing 15-minute
admin-queue schedule, built from the same intended state the sync uses.
Findings are always logged. `IdentityConfig.store.redshift_role_audit_revoke`
(default `false`) gates the `REVOKE` statements. Revoke and catalog-read failures
go to `RedshiftSync#errors`, so the sync fails.

The audit never drops a role, changes `sys:` roles' own grants, users' direct
object privileges or groups, or anything for roles not in the config. A user who
reaches a role through a chain and an unmanaged role are report-only, as are
scoped permissions. If intended state cannot be computed, that role is skipped
and logged (fail closed).

## Consequences

- Disabled roles are now intended-empty, so their stale members are found and
  revoked once revoke is on. This is a behavior change.
- Alerting: with revoke off, an alarm on `findings_detected` re-fires every
  15 minutes while drift persists.
- Revoking an unexpected role-to-role edge can remove access the owner of that
  role relied on; the first run's logs must be reviewed before enabling revoke.
- The `svv_*` views show only what the connecting user can see; the admin worker
  connects as superuser. Column names come from AWS documentation and need live
  confirmation (see the validation doc).
- Human review is required before deployment and before enabling revoke.

## Links

- [Human validation](../role-audit-validation.md)
- [Job architecture](../jobs.md)
