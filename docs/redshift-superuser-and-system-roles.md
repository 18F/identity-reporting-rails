# Redshift `superuser` and the `sys:*` System-Defined Roles

What the `superuser` account can actually do on the `analytics` cluster, which
`sys:*` system-defined role covers each privilege, and where the AWS
documentation disagrees with the live cluster.

Written to support the least-privilege admin-worker work on the
`1386-rails-superuser` branch — see [`db/redshift_roles/rails_superuser.sql`](../db/redshift_roles/rails_superuser.sql).

| | |
|---|---|
| **Verified against** | `analytics` database, `CURRENT_USER = superuser` |
| **Verified on** | 2026-09-09 UTC |
| **Machine-readable copy** | [`redshift-system-privileges-matrix.csv`](redshift-system-privileges-matrix.csv) |
| **Verification queries** | [`redshift-system-privileges-verify.sql`](redshift-system-privileges-verify.sql) |

> Everything below marked `live` was read from `SVV_SYSTEM_PRIVILEGES` and
> `pg_user_info`. Where the docs and the cluster disagree, **the cluster wins** —
> the doc claim is recorded as a DIVERGENCE so the discrepancy is traceable.

---

## AWS documentation

| Topic | URL |
|---|---|
| System-defined roles (the `sys:*` table) | [`dg/r_roles-default.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_roles-default.html) |
| System permissions for RBAC (what is grantable) | [`dg/r_roles-system-privileges.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_roles-system-privileges.html) |
| Role-based access control overview | [`dg/t_Roles.html`](https://docs.aws.amazon.com/redshift/latest/dg/t_Roles.html) |
| Visibility of data in system tables | [`dg/cm_chap_system-tables.html#c_visibility-of-data`](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_system-tables.html#c_visibility-of-data) |
| `SYSLOG ACCESS` parameter | [`dg/r_ALTER_USER.html#alter-user-syslog-access`](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_USER.html#alter-user-syslog-access) |
| `ALTER USER` | [`dg/r_ALTER_USER.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_USER.html) |
| `GRANT` | [`dg/r_GRANT.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) |
| `SVV_SYSTEM_PRIVILEGES` | [`dg/r_SVV_SYSTEM_PRIVILEGES.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_SYSTEM_PRIVILEGES.html) |
| `SVV_USER_GRANTS` | [`dg/r_SVV_USER_GRANTS.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_USER_GRANTS.html) |
| `SVV_ROLES` / `SVV_ROLE_GRANTS` | [`r_SVV_ROLES.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_ROLES.html) / [`r_SVV_ROLE_GRANTS.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_ROLE_GRANTS.html) |
| `SVV_MASKING_POLICY` / `SVV_ATTACHED_MASKING_POLICY` | [`r_SVV_MASKING_POLICY.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_MASKING_POLICY.html) / [`r_SVV_ATTACHED_MASKING_POLICY.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_ATTACHED_MASKING_POLICY.html) |
| Full SVV view index | [`dg/svv_views.html`](https://docs.aws.amazon.com/redshift/latest/dg/svv_views.html) |

Doc URLs are **case-sensitive**. `r_svv_system_privileges.html` silently returns
an empty page shell; `r_SVV_SYSTEM_PRIVILEGES.html` is correct.

---

## The `superuser` account

`superuser` is both the *name* of the cluster's master account and a *boolean
user attribute*. These are unrelated, and the name proves nothing — but on this
cluster the attribute is set:

```sql
SELECT usename, usesuper, usecreatedb, useconnlimit
FROM pg_user_info
WHERE usename = CURRENT_USER;
-- superuser | true | true | UNLIMITED
```

Only two accounts hold `usesuper = true`: `superuser` and `rdsdb` (Redshift's
internal service account). All eight application users — `rails_worker`,
`pii_reader`, `security_audit`, `quicksight_connector`, `marts`, `qa_marts`,
`fraudops_marts`, `fraudops_qa_marts` — are `false`.

`superuser` also holds one role, `dw_ingestion`, which `RedshiftSync` grants per
`config/redshift_config.yaml` `user_roles`. That role carries *object*
privileges, not system privileges, so **`superuser`'s administrative power comes
entirely from the `usesuper` attribute** — there is no system-privilege role
definition to copy.

A superuser bypasses privilege checks rather than holding enumerated privileges.
That is why `SVV_SYSTEM_PRIVILEGES` lists nothing for it and why the `superuser`
column below is derived from documented rules rather than observed grants.

---

## The five system-defined roles

All five exist on every cluster, owned by `rdsdb`, with contiguous IDs assigned
at bootstrap. Descriptions are verbatim from
[`r_roles-default.html`](https://docs.aws.amazon.com/redshift/latest/dg/r_roles-default.html):

| Role | Documented description |
|---|---|
| `sys:monitor` | "This role has the permission to access catalog or system tables." |
| `sys:operator` | "...access catalog or system tables, analyze, vacuum, or cancel queries." |
| `sys:dba` | "...create schemas, create tables, drop schemas, drop tables, and truncate tables... create or replace stored procedures, drop procedures, create or replace functions, create or replace external functions, create views, and drop views. **Also, this role inherits all the permissions from the `sys:operator` role.**" |
| `sys:secadmin` | "...create users, alter users, drop users, create roles, drop roles, and grant roles... turn RLS ON or OFF... manage RLS and DDM policies (CREATE, DROP, ATTACH, DETACH, and ALTER)... **This role can have access to user tables only when the permission is explicitly granted to the role.**" |
| `sys:superuser` | "This role has all the supported system permissions defined in System permissions for RBAC." |

Redshift ships exactly one role-to-role edge, which is the `sys:dba` inheritance
clause above:

```sql
SELECT role_name, granted_role_name FROM svv_role_grants;
-- sys:dba | sys:operator
```

`SVV_ROLE_GRANTS` is **role-to-role only** — it says nothing about which roles a
*user* holds. For that, use `SVV_USER_GRANTS`.

---

## Privilege matrix

### Legend

| Marker | Meaning |
|---|---|
| `X` | Direct grant, observed in `SVV_SYSTEM_PRIVILEGES` |
| `i` | Effective via the `sys:dba` → `sys:operator` edge; **no direct row in the view** |
| `?` | Not observable through the view — needs a functional test |
| blank | Not held |
| `Grantable` | Whether the capability can be conferred by granting a role at all |

The `superuser` column reflects a true superuser (`usesuper = true`, confirmed
above). Role columns `sys:operator` through `sys:superuser` are abbreviated to
`operator` / `monitor` / `dba` / `secadmin` / `sys:superuser` for width.

### All 57 capabilities, all six identities

One table so a single column can be scanned end to end. **Section A** is what
`SVV_SYSTEM_PRIVILEGES` reports — use those exact strings in `GRANT`, several
differ from the doc headings. **Section B** is what no role grant can reach.

| Privilege | superuser | operator | monitor | dba | secadmin | sys:superuser | Grantable | Notes |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|---|
| **SECTION A — the 42 privileges `SVV_SYSTEM_PRIVILEGES` reports** |  |  |  |  |  |  |  |  |
| `ACCESS CATALOG` | X |   |   |   |   | X | Yes | **DIVERGENCE:** docs say `sys:monitor` and `sys:operator` can "access catalog or system tables" but neither holds this. Among `sys:` roles only `sys:superuser` does. |
| `ACCESS SYSTEM TABLE` | X | X | X | i |   | X | Yes | `sys:dba` has no direct row; effective through the `sys:operator` edge. NOT held by `sys:secadmin`. |
| `ALTER DATASHARE` | X |   |   |   |   | X | Yes |  |
| `ALTER DEFAULT PRIVILEGES` | X |   |   |   |   | X | Yes | No `sys:` role except `sys:superuser`. `RedshiftSync` uses `ALTER DEFAULT PRIVILEGES FOR USER` per DBT schema. |
| `ALTER MATERIALIZED VIEW ROW LEVEL SECURITY` | X |   |   |   | X |   | Yes | `sys:secadmin` only. NOT held by `sys:superuser`. |
| `ALTER TABLE` | X |   |   | X |   | X | Yes | **RESOLVED:** `sys:dba` does hold it despite being omitted from the `sys:dba` doc description. |
| `ALTER TABLE ENABLE ROW LEVEL SECURITY` | X |   |   |   | X |   | Yes | `sys:secadmin` only. NOT held by `sys:superuser`. |
| `ALTER USER` | X |   |   |   | X | X | Yes | Grantees cannot promote or demote superusers. Several `ALTER USER` options stay superuser-only — see Section B. |
| `ANALYZE` | X | X |   | i |   | X | Yes | `sys:dba` effective via the `sys:operator` edge. |
| `ATTACH RLS POLICY` | X |   |   |   | X |   | Yes | `sys:secadmin` only. NOT held by `sys:superuser`. |
| `CANCEL` | X | X |   | i |   | X | Yes | Any user may cancel their own query without this. |
| `CREATE DATASHARE` | X |   |   |   |   | X | Yes |  |
| `CREATE LIBRARY` | X |   |   |   |   | X | Yes | Python UDF support ends after 2026-06-30 per the AWS deprecation notice. |
| `CREATE OR REPLACE EXTERNAL FUNCTION` | X |   |   |   |   | X | Yes |  |
| `CREATE OR REPLACE FUNCTION` | X |   |   | X |   | X | Yes |  |
| `CREATE OR REPLACE STORED PROCEDURES` | X |   |   | X |   | X | Yes | **NAME DIFFERS** from the doc heading `CREATE OR REPLACE PROCEDURE`. Use the live plural form. |
| `CREATE OR REPLACE VIEW` | X |   |   | X |   | X | Yes |  |
| `CREATE RLS POLICY` | X |   |   |   | X |   | Yes | `sys:secadmin` only. NOT held by `sys:superuser`. |
| `CREATE ROLE` | X |   |   |   | X | X | Yes |  |
| `CREATE SCHEMA` | X |   |   | X |   | X | Yes | Needed by `RedshiftSyncJob` and `IdpZeroEtlBindingViewSyncJob`. |
| `CREATE TABLE` | X |   |   | X |   | X | Yes | Also satisfied by `CREATE` on the schema. |
| `CREATE USER` | X |   |   |   | X | X | Yes | Grantees explicitly cannot create superusers — escalation is blocked by design. |
| `DETACH RLS POLICY` | X |   |   |   | X |   | Yes | `sys:secadmin` only. NOT held by `sys:superuser`. |
| `DROP DATASHARE` | X |   |   |   |   | X | Yes |  |
| `DROP FUNCTION` | X |   |   | X |   | X | Yes | **RESOLVED:** `sys:dba` does hold it despite the doc description listing only "drop procedures". |
| `DROP LIBRARY` | X |   |   |   |   | X | Yes |  |
| `DROP MODEL` | X |   |   |   |   | X | Yes |  |
| `DROP PROCEDURE` | X |   |   | X |   | X | Yes |  |
| `DROP RLS POLICY` | X |   |   |   | X |   | Yes | `sys:secadmin` only. NOT held by `sys:superuser`. |
| `DROP ROLE` | X |   |   |   | X | X | Yes | **DIVERGENCE:** the docs grantee list omits "users with the DROP ROLE permission", but it is a real grantable privilege. |
| `DROP SCHEMA` | X |   |   | X |   | X | Yes | Schema owner can also drop without the system permission. |
| `DROP TABLE` | X |   |   | X |   | X | Yes | Table owner with `USAGE` on the schema can also drop. |
| `DROP USER` | X |   |   |   | X | X | Yes |  |
| `DROP VIEW` | X |   |   | X |   | X | Yes |  |
| `EXPLAIN MASKING` | X |   |   |   | X |   | Yes | **DIVERGENCE:** `sys:superuser` does NOT hold this. `sys:secadmin` only. |
| `EXPLAIN RLS` | X |   |   |   | X | X | Yes |  |
| `GRANT ROLE` | X |   |   |   | X | X | Yes | **RESOLVED:** this IS a real grantable system privilege, not only an ownership rule. Needed by `RedshiftSyncJob` for `dw_ingestion`. |
| `IGNORE RLS` | X |   |   |   | X | X | Yes |  |
| `SYSTEM CREATE MODEL` | X |   |   |   |   | X | Yes | **NAME DIFFERS** from the doc heading `CREATE MODEL`. |
| `TRUNCATE TABLE` | X |   |   | X |   | X | Yes | Table owner can also truncate. |
| `UNKNOWN` | ? |   |   |   | X |   | ? | Held only by `sys:secadmin`. Prime suspect for the DDM masking-policy privilege the docs attribute to secadmin but the view cannot name. |
| `VACUUM` | X | X |   | i |   | X | Yes | `sys:dba` effective via the `sys:operator` edge. |
| **SECTION B — capabilities absent from `SVV_SYSTEM_PRIVILEGES`** |  |  |  |  |  |  |  |  |
| `CREATE` / `DROP` / `ALTER` / `ATTACH` / `DETACH MASKING POLICY` | X |   |   |   | ? |   | ? | Docs say `sys:secadmin` manages DDM policies, but no such privilege appears in the view — unlike the RLS equivalents, which all do. Either the `UNKNOWN` row above, or unreachable via roles. **Blocks `RedshiftMaskingJob` until tested.** |
| `SET SESSION AUTHORIZATION` | X |   |   |   |   |   | No | **CONFIRMED superuser-only:** absent from both the docs table and the view. Blocks fully retiring the `superuser` account — see the "NOT granted" header note in `rails_superuser.sql`. |
| `CREATE USER ... CREATEUSER` | X |   |   |   |   |   | No | `CREATE USER` grantees explicitly cannot create superusers. |
| `ALTER USER ... CREATEUSER` / `NOCREATEUSER` | X |   |   |   |   |   | No | `ALTER USER` grantees explicitly cannot change users to or from superuser. |
| `ALTER USER ... PASSWORD DISABLE` / enable | X |   |   |   | ? |   | No | "Only a superuser can enable or disable passwords." A superuser's own password cannot be disabled. |
| `ALTER USER ... VALID UNTIL` | X |   |   |   |   |   | No | "Only superusers can use this parameter." |
| `ALTER USER ... RESET SESSION TIMEOUT` | X |   |   |   |   |   | No | "You must be a database superuser to run this command." |
| `ALTER USER ... SYSLOG ACCESS` | X |   |   |   | ? |   | No | Docs do not explicitly restrict this to superusers, so an `ALTER USER` holder may be able to set it. `RedshiftSync` sets it via `syslog_access` in `redshift_config.yaml`. |
| `SELECT` on superuser-visible system tables | X |   |   |   |   |   | No | "Only superusers can see superuser-visible tables." `SYSLOG ACCESS UNRESTRICTED` does NOT cross this boundary. Workaround is a per-table `GRANT SELECT`. |
| See other users' rows in user-visible system tables | X |   |   |   |   |   | No (user attribute) | Controlled by `SYSLOG ACCESS UNRESTRICTED`, not any role privilege. Does not apply to metadata views. |
| `CREATE` / `ALTER` / `DROP GROUP` | X |   |   |   |   |   | No | **CONFIRMED absent** from both the docs table and the view. The `GRANT CREATE/ALTER/DROP GROUP` statements were **removed** from `rails_superuser.sql` — do not reintroduce them. |
| `ALTER ROLE` | X |   |   |   |   |   | No | **CONFIRMED absent** from both (only `CREATE ROLE`, `DROP ROLE`, `GRANT ROLE` exist). The `GRANT ALTER ROLE` statement was **removed** from `rails_superuser.sql` — do not reintroduce it. |
| `ALTER SCHEMA ... OWNER TO` | X |   |   |   |   |   | No | Superuser or current owner. `superuser` owns all eight app schemas, including the four DBT schemas. |
| `GRANT` / `REVOKE` on objects not owned | X |   |   |   |   |   | No | A non-superuser needs ownership or `WITH GRANT OPTION`. This is the structural reason `RedshiftSyncJob` has needed a superuser. |
| `ALTER USER rdsdb` |   |   |   |   |   |   | No | "You can't alter the user named `rdsdb`." The one operation even a true superuser cannot perform. |

`superuser` holds **55 of the 56** determinable capabilities. The single
exception is the last row. The one indeterminate is `UNKNOWN`.

---

## Two visibility axes

A recurring source of confusion, worth stating explicitly. From
[`c_visibility-of-data`](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_system-tables.html#c_visibility-of-data):

1. **Which tables** — user-visible vs superuser-visible. *"Only users with
   superuser privileges can see the data in those tables that are in the
   superuser-visible category... To give a regular user access to
   superuser-visible tables, grant SELECT privilege on that table."*
2. **Whose rows within them** — *"in most user-visible tables, rows generated by
   another user are invisible to a regular user. If a regular user is given
   SYSLOG ACCESS UNRESTRICTED, that user can see all rows in user-visible
   tables."*

`SYSLOG ACCESS UNRESTRICTED` moves **only axis 2**. Per `ALTER USER`:
*"UNRESTRICTED doesn't give a regular user access to superuser-visible tables."*
And it does not apply to metadata views at all.

So for a non-superuser to read a system table it needs the right combination of
`ACCESS SYSTEM TABLE` / `ACCESS CATALOG` / per-table `GRANT SELECT` (axis 1) and
`SYSLOG ACCESS UNRESTRICTED` (axis 2). Each view's own doc page states its class.

---

## Where the docs and the cluster disagree

| # | Doc claim | Live reality |
|---|---|---|
| 1 | `sys:superuser` has "all the supported system permissions" | It has 34, and is **not a superset of `sys:secadmin`**. Eight privileges belong to secadmin alone. |
| 2 | `sys:monitor` / `sys:operator` can "access catalog or system tables" | Neither holds `ACCESS CATALOG`. That phrase maps only to `ACCESS SYSTEM TABLE`. |
| 3 | `sys:secadmin` manages DDM policies (CREATE/DROP/ATTACH/DETACH/ALTER) | No masking-policy privilege appears in the view for any role, though all six RLS equivalents do. |
| 4 | `sys:dba` description omits `ALTER TABLE` and `DROP FUNCTION` | It holds both. Doc omission only. |
| 5 | `DROP ROLE` grantee list omits "users with the DROP ROLE permission" | It is a real grantable privilege. |
| 6 | Headings `CREATE OR REPLACE PROCEDURE`, `CREATE MODEL` | Live names are `CREATE OR REPLACE STORED PROCEDURES` (plural) and `SYSTEM CREATE MODEL`. |
| 7 | `GRANT ROLE` appears only as an ownership rule | It is an enumerated system privilege held by `sys:secadmin` and `sys:superuser`. |
| 8 | — | `SVV_SYSTEM_PRIVILEGES` has **no `admin_option` column** on this cluster version. |

The eight privileges from row 1 — the reason no single role replaces `superuser`:

```text
ALTER MATERIALIZED VIEW ROW LEVEL SECURITY   CREATE RLS POLICY
ALTER TABLE ENABLE ROW LEVEL SECURITY        DETACH RLS POLICY
ATTACH RLS POLICY                            DROP RLS POLICY
EXPLAIN MASKING                              UNKNOWN
```

---

## Verification SQL

Full set with recorded answers in
[`redshift-system-privileges-verify.sql`](redshift-system-privileges-verify.sql).
All read-only. The four that matter most:

**Is the connected user a true superuser?**

```sql
SELECT usename, usesuper, usecreatedb, syslogaccess, useconnlimit
FROM pg_user_info
WHERE usename = CURRENT_USER;
```

**Reproduce Section A from live state** — this pivot emits the matrix directly:

```sql
SELECT system_privilege,
       MAX(CASE WHEN identity_name = 'sys:operator'  THEN 'X' END) AS "sys:operator",
       MAX(CASE WHEN identity_name = 'sys:monitor'   THEN 'X' END) AS "sys:monitor",
       MAX(CASE WHEN identity_name = 'sys:dba'       THEN 'X' END) AS "sys:dba",
       MAX(CASE WHEN identity_name = 'sys:secadmin'  THEN 'X' END) AS "sys:secadmin",
       MAX(CASE WHEN identity_name = 'sys:superuser' THEN 'X' END) AS "sys:superuser"
FROM svv_system_privileges
WHERE identity_name LIKE 'sys:%'
GROUP BY system_privilege
ORDER BY system_privilege;
```

> `SVV_SYSTEM_PRIVILEGES` reports **direct grants only**. `sys:dba` shows no row
> for `ACCESS SYSTEM TABLE` / `ANALYZE` / `VACUUM` / `CANCEL` even though the
> `sys:dba` → `sys:operator` edge exists. Computing effective privileges from
> this view alone under-counts `sys:dba` by four.

**What `sys:secadmin` has that `sys:superuser` lacks:**

```sql
SELECT system_privilege
FROM svv_system_privileges
WHERE identity_name = 'sys:secadmin'
  AND system_privilege NOT IN (SELECT system_privilege
                               FROM svv_system_privileges
                               WHERE identity_name = 'sys:superuser')
ORDER BY system_privilege;
```

**Schema ownership** — decides whether a non-superuser can `GRANT`/`REVOKE`:

```sql
SELECT nspname AS schema_name, pg_get_userbyid(nspowner) AS owner
FROM pg_namespace
WHERE nspname IN ('idp', 'idp_core', 'logs', 'system_tables', 'fraudops',
                  'marts', 'qa_marts', 'fraudops_marts', 'fraudops_qa_marts')
ORDER BY nspname;
```

Result: all eight existing schemas owned by `superuser`. `idp_core` returned no
row, so it does not exist in this database.

---

## Implications for the admin queue

Jobs on `queue_as :admin` and what each needs:

| Job | Needs | Covered by |
|---|---|---|
| `RedshiftSyncJob` | `CREATE`/`ALTER`/`DROP USER`, `CREATE`/`DROP`/`GRANT ROLE` | `sys:secadmin` |
| `RedshiftSyncJob` | `GRANT`/`REVOKE` on managed schemas and tables | **Ownership only** — no role grants this |
| `RedshiftMaskingJob` | `CREATE MASKING POLICY` | **Unresolved** — see divergence 3 |
| `RedshiftMaskingJob` | `ATTACH`/`DETACH MASKING POLICY` | **Unresolved, and fails silently** — see below |
| `RedshiftUserLoginDetectionJob` | `SYS_CONNECTION_LOG` | `ACCESS SYSTEM TABLE` (`sys:operator` / `sys:monitor`) |
| `RedshiftUnloadLogCheckerJob` | `STL_UNLOAD_LOG` | `ACCESS SYSTEM TABLE`, pending the axis-1 class check |
| `RedshiftSystemTableSyncJob` | `pg_table_def`, `svv_columns` | **Already `PUBLIC`** / `ACCESS SYSTEM TABLE` |
| `RedshiftUnexpectedUserDetectionJob` | `pg_user` | **Already `PUBLIC`** — no grant needed |
| `IdpZeroEtlBindingViewSyncJob` | `CREATE SCHEMA`, `CREATE OR REPLACE VIEW` | `sys:dba` |

A least-privilege `data_warehouse_admin` role therefore needs `sys:secadmin` +
`sys:operator` + `sys:dba` (or the individual DDL privileges) + catalog access +
ownership of **eight** schemas — with masking unresolved and
`SET SESSION AUTHORIZATION` permanently out of reach.

### Catalog access: no grant is needed at all

`ACCESS CATALOG` grants unrestricted read of the *entire* catalog and, among
`sys:` roles, only `sys:superuser` carries it. The obvious alternative was a
scoped, config-driven grant — already proven on this cluster, and differing from
`ACCESS CATALOG` by three orders of magnitude:

| Identity | `tables:` in config | pg_catalog relations granted |
|---|---|---|
| `rails_worker` | `[pg_user]` | **1** |
| `security_audit` | `[pg_user]` | **1** |
| `marts` / `qa_marts` | *omitted* | **2540** each |
| `fraudops_marts` / `fraudops_qa_marts` | *omitted* | **1846** each |
| `PUBLIC` | — (built-in) | 448 SELECT + **1 UPDATE** |

The mechanism is `RedshiftSync#create_system_user_privileges`
(`app/services/redshift_sync.rb:427-434`): when `tables:` is blank the grant
becomes `GRANT <priv> ON ALL TABLES IN SCHEMA pg_catalog`; when present it
enumerates `schema.table` explicitly. So the DBT users' blanket access is the
config asking for it, not drift.

**But the answer turned out to be simpler than any grant: grant nothing.**
`PUBLIC` already holds `SELECT` on all three relations the admin jobs read
(query 11e):

| Relation | `relkind` | Held by |
|---|---|---|
| `pg_group` | `r` table | `PUBLIC` + 4 DBT users |
| `pg_table_def` | `v` view | `PUBLIC` + 4 DBT users |
| `pg_user` | `v` view | `PUBLIC` + 4 DBT users + `rails_worker`, `security_audit` |

Three consequences:

1. **The staged `tables: [pg_user]` blocks are no-ops.** The uncommitted
   `pg_catalog` additions for `rails_worker` and `security_audit` grant a read
   `PUBLIC` already confers. They can be dropped from `redshift_config.yaml`.
2. **`rails_superuser` needs no catalog grant at all** — not `ACCESS CATALOG`,
   and not a scoped `tables:` block either. The `GRANT ACCESS CATALOG` statement
   was removed from `rails_superuser.sql` with nothing added in its place, which
   is the smallest correct change.
3. **Catalog visibility was never why these jobs are on `:admin`.** For
   `RedshiftSystemTableSyncJob` the real constraint is `search_path`, not
   privilege — see below.

Two side observations, both independent of this work:

- **`GRANT ON ALL TABLES` is a point-in-time snapshot.** It does not cover
  relations created later and does not self-heal. The 694-relation gap between
  the `marts` pair (2540) and the `fraudops` pair (1846) means they were last
  granted at different times — most likely because the fraudops users are gated
  on `fraud_ops_tracker_enabled` + `dw_fraudops_email_enabled`, so `RedshiftSync`
  skips them while those flags are off and `pg_catalog` keeps growing.
- **`PUBLIC` holds `UPDATE` on `pg_settings` — benign, do not revoke.** This is
  inherited PostgreSQL behavior: `pg_settings` carries an `ON UPDATE DO INSTEAD`
  rule that calls `set_config()`, so updating it is just another spelling of
  `SET`. It affects only the issuing session's own GUCs and grants nothing a user
  couldn't already do. Documented here so it stops being rediscovered.

Separately, the `data_warehouse` migrations that run `CREATE OR REPLACE EXTERNAL
FUNCTION` for the Lambda UDFs do **not** require a true superuser:
`CREATE OR REPLACE EXTERNAL FUNCTION` is an enumerated, grantable system
privilege (Section A). A single `GRANT` covers it.

### Confirmed defect: `convert_source_char_columns` has never run on Redshift

Found while checking whether the catalog work affected
`RedshiftSystemTableSyncJob`. **Pre-existing and unrelated to `rails_superuser`** —
recorded here because the investigation surfaced it.

The job reads columns two ways
(`app/jobs/redshift_system_table_sync_job.rb:164-186`):

| Call | Query | Result |
|---|---|---|
| `fetch_source_columns` | `pg_table_def WHERE schemaname = 'pg_catalog'` | **Works** — `pg_catalog` is implicitly on every `search_path` |
| `fetch_target_columns` | `pg_table_def WHERE schemaname = 'system_tables'` | **Returns nothing** — `system_tables` is not on `search_path` |

`search_path` is `'$user', public`, and `PG_TABLE_DEF` filters by it — a filter no
privilege lifts, superuser included. `fetch_target_columns` has exactly one
caller, at `:196`:

```ruby
col = fetch_target_columns.find { |c| source_char_type?(c['type']) }
break unless col
```

Zero rows means `col` is always `nil`, the loop breaks on its first iteration, and
the `CHAR(n)` → `VARCHAR(n)` conversion never happens. Nothing raises.

**Corroborated independently by the masking policies.** Query 9 shows
`mask_system_tables_stv_recents_query` still has input type `character(600)`.
Converting exactly that column to `VARCHAR(600)` is this method's entire purpose
(`source_char_type?` at `:215`, `redshift_data_type` `/^char/` at `:305`), so its
survival proves the body has never executed.

Tests can't catch it: the method returns early `unless dw_redshift?`, and
local/test runs on Postgres take the `information_schema` branch. The only path
where the method does anything is the only path that is broken.

**Fix:** use `svv_columns` for the target rather than widening `search_path`. It
is not `search_path`-dependent, and `missing_system_table_columns` (`:115-141`)
already uses it against this same target schema — so the job is internally
inconsistent about which view it trusts.

### Masking is a silent failure mode, not just an unresolved one

`RedshiftMaskingSync` calls `create_masking_policies` on **every** sync
(`redshift_masking_sync.rb:54`), not only when policies are absent. The
statements use `CREATE MASKING POLICY ... IF NOT EXISTS`
(`redshift_masking/sql_executor.rb:14`), so the 24 existing policies make the
run a no-op in *effect* — but the statement is still issued every time. Whether
Redshift short-circuits `IF NOT EXISTS` before the privilege check, or checks
the privilege first, decides whether a non-superuser can run this job at all.
That is not documented; it needs the probe.

The more dangerous half is attachment. The two paths fail differently:

| Path | Code | On privilege failure |
|---|---|---|
| `CREATE MASKING POLICY` | `sql_executor.rb:38` — bare `connection.execute` | Raises. Job fails loudly. |
| `ATTACH` / `DETACH MASKING POLICY` | `sql_executor.rb:69-75` — `rescue ActiveRecord::StatementInvalid` → `logger.warn` | **Swallowed.** Job logs `sync completed` and exits 0. |

So if `rails_superuser` can create policies but cannot attach them, the sync
reports success while leaving PII columns unmasked. Every subsequent run
re-detects the same drift, re-fails, and re-warns. Nothing escalates.

**Before granting this job to a non-superuser, verify attachment positively** —
query 9b counts live attachments per grantee. Do not infer success from the job's
exit status or from `sync completed` in the logs.

### Corrections applied to `rails_superuser.sql`

All of these were folded into the file on 2026-09-09. Recorded by *statement*
rather than by line number, because the rewrite moved every line and stale line
references are worse than no references — the old table pointed at line 77 for
`GRANT ACCESS CATALOG`, which is now `GRANT DROP ROLE`.

| Statement | Finding | Outcome |
|---|---|---|
| `GRANT CREATE / ALTER / DROP GROUP` | No `GROUP` privilege exists in Redshift | **Removed** — would have failed |
| `GRANT ALTER ROLE` | Only `CREATE`/`DROP`/`GRANT ROLE` exist | **Removed** — would have failed |
| `GRANT CREATE MASKING POLICY` | Not an enumerable privilege for any role | **Removed**, replaced by the `sys:secadmin` route plus a probe |
| `GRANT ACCESS CATALOG` | Valid, but unnecessary — `PUBLIC` already reads all three relations (query 11e) | **Removed**, nothing added |
| `GRANT ROLE "sys:secadmin"` | Described as a "supported fallback"; it is **required** — masking and RLS management exist nowhere else | Reworded and granted, with the RLS over-reach stated |
| `ALTER SCHEMA ... OWNER TO` | Premise was false: all eight schemas are `superuser`-owned, not DBT-user-owned | Premise corrected; still reassigns only four, other four commented pending the `ALTER DEFAULT PRIVILEGES` check |
| `GRANT ROLE ... TO rails_superuser` | The user does not exist — never added to `system_users` in `redshift_config.yaml`, so `RedshiftSync` never creates it | Prerequisite documented in the file. **The underlying gap is still open.** |

Three grants were also **added**, all confirmed grantable: `GRANT ROLE` (without
it `RedshiftSyncJob` could not sync `user_roles` at all), `CREATE OR REPLACE
VIEW`, and `CREATE OR REPLACE EXTERNAL FUNCTION`.

---

## Open questions

1. **Can `sys:secadmin` create and attach masking policies?** The blocker, and
   two separate questions — `CREATE` and `ATTACH` are granted, and fail,
   independently. Only a functional test settles either; query 9 in the `.sql`
   file has a throwaway-user recipe. Answer `ATTACH` first: it is the one that
   fails silently. If no, `RedshiftMaskingJob` keeps needing a superuser and the
   least-privilege split is partial at best.
2. **What is the `UNKNOWN` privilege on `sys:secadmin`?** Likely the DDM
   privilege the view cannot name. Worth re-checking after a cluster version
   bump.
3. **Is `STL_UNLOAD_LOG` user-visible or superuser-visible?** **Not answerable by
   observation on this cluster.** Query 10 returned `NULL / NULL / 0` — the table
   is entirely empty, so no UNLOAD has run inside the 7-day STL retention window.
   There is no baseline to compare a restricted user against. Either trigger an
   UNLOAD first, or grant `ACCESS SYSTEM TABLE` on the documentation alone and
   accept that `RedshiftUnloadLogCheckerJob` is unverified.
4. **Can an `ALTER USER` holder set `SYSLOG ACCESS`?** Determines whether
   `RedshiftSync` can keep managing `syslog_access` without a superuser.
5. **Does reassigning the DBT schemas break DBT?** The stated rationale was
   false, but the conclusion may still hold. Needs its own check against how
   `RedshiftSync` applies `ALTER DEFAULT PRIVILEGES FOR USER`.
6. ~~Does `RedshiftSystemTableSyncJob` return any columns today?~~
   **ANSWERED — it is a confirmed defect**, see the section above. `search_path`
   is `'$user', public`; `pg_table_def` returns 0 rows for `system_tables`;
   `convert_source_char_columns` is a permanent no-op on Redshift. Pre-existing,
   fixable in one line, tracked separately from this branch.
