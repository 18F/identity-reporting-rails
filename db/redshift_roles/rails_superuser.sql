-- =============================================================================
-- data_warehouse_admin role  ->  rails_superuser
-- =============================================================================
-- Replaces the need for the admin worker to connect as a Redshift superuser.
-- The role grants only the explicit system + object privileges required by the
-- jobs on the :admin queue:
--
--   * RedshiftSyncJob                    (CREATE/ALTER/DROP USER, GRANT/REVOKE)
--   * RedshiftMaskingJob                 (CREATE/ATTACH MASKING POLICY)
--   * RedshiftSystemTableSyncJob         (svv_columns, pg_table_def)
--   * RedshiftUserLoginDetectionJob      (SYS_CONNECTION_LOG)
--   * RedshiftUnloadLogCheckerJob        (STL_UNLOAD_LOG)
--   * RedshiftUnexpectedUserDetectionJob (pg_user)
--   * IdpZeroEtlBindingViewSyncJob       (CREATE SCHEMA, CREATE OR REPLACE VIEW)
--
-- NOT granted: SET SESSION AUTHORIZATION. CONFIRMED superuser-only -- absent
-- from both the RBAC permission table and SVV_SYSTEM_PRIVILEGES. It stays with
-- the `superuser` account (used only by the already-completed FraudOps
-- bootstrap, not by any recurring job).
--
-- Run as a superuser, once per cluster:
--
--   psql ... -f db/redshift_roles/rails_superuser.sql
--
-- Re-running: Redshift has no `CREATE ROLE IF NOT EXISTS`. To re-apply, run
-- `DROP ROLE data_warehouse_admin;` first (this revokes it from members too).
--
-- -----------------------------------------------------------------------------
-- VALIDATION STATUS: 4a is RESOLVED. Every statement below was checked against
-- SVV_SYSTEM_PRIVILEGES on a live cluster (2026-09-09 UTC). The privilege names
-- used here are the exact strings the view reports, which differ from the AWS doc
-- headings in two places (`CREATE OR REPLACE STORED PROCEDURES` is plural;
-- `CREATE MODEL` is really `SYSTEM CREATE MODEL`).
--
-- Full analysis, matrix and verification queries:
--   docs/redshift-superuser-and-system-roles.md
--   docs/redshift-system-privileges-matrix.csv
--   docs/redshift-system-privileges-verify.sql
--
-- Four statements from the previous revision were REMOVED as invalid -- the
-- privileges do not exist in Redshift and the GRANTs would have failed:
--   GRANT CREATE GROUP / ALTER GROUP / DROP GROUP   (no GROUP privileges at all)
--   GRANT ALTER ROLE                (only CREATE ROLE, DROP ROLE, GRANT ROLE)
--   GRANT CREATE MASKING POLICY     (no masking privilege is enumerable -- see
--                                    the MASKING section below)
--   GRANT ACCESS CATALOG            (valid, but unnecessary: PUBLIC already holds
--                                    SELECT on pg_user, pg_table_def, pg_group)
--
-- STILL BLOCKED: masking. See the MASKING section -- it is the one thing that
-- prevents this role from fully replacing `superuser` for the :admin queue.
-- =============================================================================

CREATE ROLE data_warehouse_admin;

-- ---------------------------------------------------------------------------
-- System-level permissions (Redshift RBAC)
-- ---------------------------------------------------------------------------
-- All verified present in SVV_SYSTEM_PRIVILEGES and grantable to a role.

-- User management: RedshiftSyncJob creates and reconciles the system users.
-- Note Redshift blocks escalation by design -- a CREATE USER / ALTER USER
-- grantee cannot create a superuser or promote a user to one.
GRANT CREATE USER TO ROLE data_warehouse_admin;
GRANT ALTER USER  TO ROLE data_warehouse_admin;
GRANT DROP USER   TO ROLE data_warehouse_admin;

-- Role management. GRANT ROLE is required for RedshiftSyncJob to sync the
-- `user_roles` section of config/redshift_config.yaml (currently dw_ingestion
-- and quicksight_access). It is a real system privilege despite being absent
-- from the grantee list in the AWS GRANT documentation.
-- CAUTION on the third statement: the privilege really is named "GRANT ROLE", so
-- the statement reads `GRANT GRANT ROLE TO ROLE ...`. Confirmed as a privilege
-- name in SVV_SYSTEM_PRIVILEGES, but the doubled keyword has not been parsed by a
-- live cluster. If Redshift rejects it, drop the line -- sys:secadmin (granted
-- below) already carries GRANT ROLE, so nothing is lost but the documentation.
GRANT CREATE ROLE TO ROLE data_warehouse_admin;
GRANT DROP ROLE   TO ROLE data_warehouse_admin;
GRANT GRANT ROLE  TO ROLE data_warehouse_admin;

-- Schema and view DDL: RedshiftSyncJob creates DBT schemas, and
-- IdpZeroEtlBindingViewSyncJob creates the binding-view schema and its views.
GRANT CREATE SCHEMA           TO ROLE data_warehouse_admin;
GRANT CREATE OR REPLACE VIEW  TO ROLE data_warehouse_admin;

-- The data_warehouse migrations create the Lambda UDFs. This privilege is
-- grantable, so those migrations do NOT require a true superuser -- which was
-- previously assumed to be a hard blocker on single-identity.
GRANT CREATE OR REPLACE EXTERNAL FUNCTION TO ROLE data_warehouse_admin;

-- System table reads: STL_/STV_/SVV_/SVL_/SYS_ views. Required by
-- RedshiftUnloadLogCheckerJob (STL_UNLOAD_LOG), RedshiftUserLoginDetectionJob
-- (SYS_CONNECTION_LOG) and RedshiftSystemTableSyncJob (SVV_COLUMNS).
--
-- ACCESS CATALOG is deliberately NOT granted. It would confer unrestricted read
-- of the entire catalog, and among the sys: roles only sys:superuser carries it.
-- It is also unnecessary: PUBLIC already holds SELECT on every pg_catalog
-- relation these jobs touch (pg_user, pg_table_def, pg_group). If a future job
-- needs a catalog relation PUBLIC cannot read, add a scoped `tables:` entry
-- under a pg_catalog schema block in config/redshift_config.yaml rather than
-- reaching for ACCESS CATALOG -- see the `rails_worker` precedent there.
GRANT ACCESS SYSTEM TABLE TO ROLE data_warehouse_admin;

-- ---------------------------------------------------------------------------
-- MASKING -- BLOCKED, and the reason this role is not yet a drop-in replacement
-- ---------------------------------------------------------------------------
-- RedshiftMaskingJob creates masking policies and attaches them to columns.
-- There is NO enumerable system privilege for either operation: masking-policy
-- management appears nowhere in SVV_SYSTEM_PRIVILEGES for ANY role, even though
-- all six row-level-security equivalents do. AWS documents sys:secadmin as
-- managing DDM policies, so sys:secadmin is the only candidate path -- which
-- makes it REQUIRED here, not the optional fallback the previous revision
-- described. The unnamed `UNKNOWN` privilege held only by sys:secadmin is the
-- prime suspect for the missing DDM privilege.
--
-- sys:secadmin also confers the six RLS privileges plus IGNORE RLS, which
-- exceeds what any current job needs. That is the cost of the only available
-- route to masking. It additionally supersets the user- and role-management
-- grants above; those are kept enumerated so this file still documents the
-- actual requirement, and so revoking sys:secadmin (if the probe below fails)
-- does not silently strip user management too.
GRANT ROLE "sys:secadmin" TO ROLE data_warehouse_admin;

-- SUFFICIENCY IS UNPROVEN. Before trusting RedshiftMaskingJob to this role, run
-- the probe as a throwaway user holding ONLY sys:secadmin, and test ATTACH as
-- well as CREATE -- they are granted, and fail, independently:
--
--   -- as superuser -- scratch table so the probe never touches real PII:
--   CREATE SCHEMA IF NOT EXISTS probe_scratch;
--   CREATE TABLE probe_scratch.t (v VARCHAR(64));
--   CREATE USER secadmin_probe PASSWORD DISABLE;
--   GRANT ROLE "sys:secadmin" TO secadmin_probe;
--   GRANT USAGE ON SCHEMA probe_scratch TO secadmin_probe;
--   -- as secadmin_probe -- these two are the actual test:
--   CREATE MASKING POLICY probe_policy WITH (v VARCHAR(64)) USING ('***');
--   ATTACH MASKING POLICY probe_policy ON probe_scratch.t (v)
--     TO PUBLIC PRIORITY 99;
--   -- cleanup as superuser:
--   DROP SCHEMA probe_scratch CASCADE;
--   DROP MASKING POLICY probe_policy;
--   DROP USER secadmin_probe;
--
-- ATTACH matters more than CREATE. RedshiftMasking::SqlExecutor rescues
-- ActiveRecord::StatementInvalid around attach/detach and only logs a warning
-- (app/services/redshift_masking/sql_executor.rb:69-75), so a role that cannot
-- attach makes the sync report success while leaving PII columns unmasked. CREATE
-- is unrescued (:38) and fails loudly. Verify attachment positively -- query 9b
-- in docs/redshift-system-privileges-verify.sql counts live attachments per
-- grantee -- and do NOT infer success from the job's exit status or from
-- `sync completed` in the logs.
--
-- If the probe fails, keep RedshiftMaskingJob on the `superuser` connection and
-- treat this role as covering the rest of the :admin queue only.

-- ---------------------------------------------------------------------------
-- Object-level privileges
-- ---------------------------------------------------------------------------
-- RedshiftSyncJob and RedshiftMaskingJob must GRANT/REVOKE on the managed
-- schemas and their tables. A non-superuser can only do that on objects it
-- owns, and no system privilege substitutes for ownership -- this is the
-- structural reason these jobs have needed a superuser.
--
-- CORRECTION to the previous revision: it claimed the four DBT schemas are
-- "owned and populated by their matching DBT users". That is false. On a live
-- cluster ALL EIGHT app schemas are owned by `superuser`, the DBT schemas
-- included, so ownership of the four below leaves RedshiftSyncJob unable to
-- GRANT/REVOKE on the other four.
--
-- The four DBT schemas are still NOT reassigned here, but for a different and
-- unverified reason: RedshiftSync issues
-- `ALTER DEFAULT PRIVILEGES FOR USER <schema> IN SCHEMA <schema>` for DBT
-- schemas (app/services/redshift_sync.rb:420-424), and whether that still
-- behaves correctly once the schema owner changes has not been tested. Until it
-- is, RedshiftSyncJob cannot manage those four as this role -- see open
-- question 5 in docs/redshift-superuser-and-system-roles.md.

ALTER SCHEMA idp           OWNER TO ROLE data_warehouse_admin;
ALTER SCHEMA logs          OWNER TO ROLE data_warehouse_admin;
ALTER SCHEMA system_tables OWNER TO ROLE data_warehouse_admin;
ALTER SCHEMA fraudops      OWNER TO ROLE data_warehouse_admin;

-- Pending the ALTER DEFAULT PRIVILEGES check above, then uncomment:
-- ALTER SCHEMA marts             OWNER TO ROLE data_warehouse_admin;
-- ALTER SCHEMA qa_marts          OWNER TO ROLE data_warehouse_admin;
-- ALTER SCHEMA fraudops_marts    OWNER TO ROLE data_warehouse_admin;
-- ALTER SCHEMA fraudops_qa_marts OWNER TO ROLE data_warehouse_admin;

-- Existing tables in those schemas must also be reassigned so the role can
-- GRANT/REVOKE on them, and so RedshiftSystemTableSyncJob can run ALTER TABLE
-- against the system_tables copies. Ownership covers that DDL, which is why no
-- CREATE TABLE / ALTER TABLE system privilege is granted above. Generate the
-- statements with, e.g.:
--
--   SELECT 'ALTER TABLE ' || schemaname || '.' || tablename ||
--          ' OWNER TO ROLE data_warehouse_admin;'
--   FROM pg_tables
--   WHERE schemaname IN ('idp', 'logs', 'system_tables', 'fraudops');
--
-- then review and run the output.

-- ---------------------------------------------------------------------------
-- Assign the role to the admin worker's DB user
-- ---------------------------------------------------------------------------
-- PREREQUISITE: `rails_superuser` must exist before this statement runs, and
-- nothing creates it yet. Two things are needed, in order:
--
--   1. terraform creates the Secrets Manager secret
--      `redshift/<env>-analytics-rails-superuser`. Terraform cannot run SQL
--      against Redshift, so it stops there.
--   2. `rails_superuser` must be added to the `system_users:` list in
--      config/redshift_config.yaml. RedshiftSync#create_system_user reads the
--      secret and issues the actual CREATE USER. It is currently absent from
--      that list, so the user is never created and this GRANT will fail with
--      "user does not exist".

GRANT ROLE data_warehouse_admin TO rails_superuser;
