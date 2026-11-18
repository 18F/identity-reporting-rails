---
title: "Validate the Redshift role audit"
description: "Human-only checks for RedshiftRoleAudit that return metadata, never data values"
status: draft
tier: 2
---

# Validate the Redshift role audit

These checks are for a human operator in an approved environment. The AI agent
does not run them. Verify the environment first, query metadata views only, and
do not SELECT warehouse data values or paste PII into tickets or logs. A Rails
console's read-only banner does not protect Redshift.

## Rollout order

1. Deploy with `redshift_role_audit_revoke` unset or `false`.
2. Read one `RedshiftSyncJob` run's logs for `"name":"RedshiftRoleAudit"` and
   work through every finding (see below).
3. Decide per finding: fix the config, or confirm the revoke is intended.
4. Set `redshift_role_audit_revoke: true` in the deployed `application.yml`.
5. Read the next run: it should log `"clean":true`, or `revoke_attempted` for
   anything left.

A `role_effective_user_unexpected`, `role_unmanaged`, scoped-permission or
`sys:` inheritor finding is never auto-revoked; a person handles those.

## Column and visibility checks

Run as the admin worker's identity (superuser).

```sql
SELECT current_database();

SELECT column_name, data_type
FROM svv_columns
WHERE table_name IN ('svv_user_grants', 'svv_role_grants', 'svv_roles',
                     'svv_relation_privileges', 'svv_schema_privileges',
                     'svv_database_privileges', 'svv_system_privileges')
ORDER BY table_name, ordinal_position;
```

Expect `svv_user_grants.admin_option` to be `boolean`, and `privilege_scope` on
`svv_schema_privileges` and `svv_database_privileges` only.

## Expected state of one role

```sql
SELECT user_name, admin_option FROM svv_user_grants WHERE role_name = 'pii_viewers' ORDER BY 1;
SELECT role_name AS grantee, granted_role_name FROM svv_role_grants
WHERE role_name = 'pii_viewers' OR granted_role_name = 'pii_viewers' ORDER BY 1, 2;
SELECT role_name FROM svv_roles WHERE role_name NOT LIKE 'sys:%' ORDER BY 1;
```

Compare with `member_users`, `aws_groups` and `assigned_roles` in
`config/redshift_config.yaml`, remembering that members of a disabled role are
expected to be empty.

## Object privileges on managed roles

Run in each database (`analytics`, `analytics_zetl`); the views are per database.

```sql
SELECT identity_name, namespace_name, relation_name, privilege_type
FROM svv_relation_privileges WHERE identity_type = 'role' ORDER BY 1, 2, 3, 4;
SELECT identity_name, namespace_name, privilege_type, privilege_scope
FROM svv_schema_privileges WHERE identity_type = 'role' ORDER BY 1, 2, 3;
SELECT identity_name, database_name, privilege_type, privilege_scope
FROM svv_database_privileges WHERE identity_type = 'role' ORDER BY 1, 2, 3;
SELECT identity_name, system_privilege
FROM svv_system_privileges WHERE identity_type = 'role' ORDER BY 1, 2;
```

The config declares no privileges for roles, so every row for a managed role is a
finding. Rows for `sys:` roles are expected and are not audited.

## Checking a revoke

After revoke is on, re-run the queries above for the affected role. The row must
be gone. If a user still reaches the role, check for another path with the
inheritor and effective-user queries; grants from groups and other roles add up.
