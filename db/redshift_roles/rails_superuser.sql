-- =============================================================================
-- data_warehouse_admin role  ->  rails_superuser
-- =============================================================================
-- Replaces the need for the admin worker to connect as a Redshift superuser.
-- The role grants only the explicit system + object privileges required by the
-- jobs on the :admin queue:
--
--   * RedshiftSyncJob                    (CREATE/ALTER/DROP USER, GRANT/REVOKE)
--   * RedshiftMaskingJob                 (CREATE/ATTACH MASKING POLICY)
--   * RedshiftSystemTableSyncJob         (pg_table_def / svv_columns visibility)
--   * RedshiftUserLoginDetectionJob      (SYS_CONNECTION_LOG)
--   * RedshiftUnloadLogCheckerJob        (STL_UNLOAD_LOG)
--   * RedshiftUnexpectedUserDetectionJob (pg_user visibility)
--
-- NOT granted: SET SESSION AUTHORIZATION. Redshift reserves that for true
-- superusers, so it stays with the `superuser` account (used only by the
-- already-completed FraudOps bootstrap, not by any recurring job).
--
-- Run as a superuser, once per cluster:
--
--   psql ... -f db/redshift_roles/rails_superuser.sql
--
-- Assumes the database user `rails_superuser` already exists (created
-- out-of-band, with its password stored in Secrets Manager under
-- `redshift/<env>-analytics-rails-superuser` so the admin worker picks it up
-- via IdentityConfig).
--
-- Re-running: Redshift has no `CREATE ROLE IF NOT EXISTS`. To re-apply, run
-- `DROP ROLE data_warehouse_admin;` first (this revokes it from members too).
--
-- SANDBOX-PENDING (4a): the individual `GRANT <system privilege> TO ROLE`
-- statements below have not yet been validated against a live Redshift cluster.
-- If Redshift rejects any of them, the supported fallback is to assign the
-- built-in system-defined roles instead:
--
--   GRANT ROLE "sys:secadmin" TO ROLE data_warehouse_admin;  -- users/roles/masking
--   GRANT ROLE "sys:operator" TO ROLE data_warehouse_admin;  -- system table reads
--
-- =============================================================================

CREATE ROLE data_warehouse_admin;

-- ---------------------------------------------------------------------------
-- System-level permissions (Redshift RBAC)
-- ---------------------------------------------------------------------------

-- User management: CREATE USER, ALTER USER, DROP USER for RedshiftSyncJob.
GRANT CREATE USER TO ROLE data_warehouse_admin;
GRANT ALTER USER  TO ROLE data_warehouse_admin;
GRANT DROP USER   TO ROLE data_warehouse_admin;

-- Group management: RedshiftSyncJob creates/alters the lg_* groups.
GRANT CREATE GROUP TO ROLE data_warehouse_admin;
GRANT ALTER GROUP  TO ROLE data_warehouse_admin;
GRANT DROP GROUP   TO ROLE data_warehouse_admin;

-- Role management: required so this role can GRANT roles to the users it
-- provisions (RedshiftSyncJob syncs role membership via user_roles).
GRANT CREATE ROLE TO ROLE data_warehouse_admin;
GRANT ALTER ROLE  TO ROLE data_warehouse_admin;
GRANT DROP ROLE   TO ROLE data_warehouse_admin;

-- Schema management: RedshiftSyncJob and the binding-view job run CREATE SCHEMA.
GRANT CREATE SCHEMA TO ROLE data_warehouse_admin;

-- Dynamic data masking: RedshiftMaskingJob creates and attaches policies.
GRANT CREATE MASKING POLICY TO ROLE data_warehouse_admin;

-- System catalog visibility:
--   * ACCESS CATALOG      -> unrestricted reads of pg_catalog (pg_user,
--                            pg_group, pg_table_def, svv_columns, etc.),
--                            required by RedshiftUnexpectedUserDetectionJob and
--                            RedshiftSystemTableSyncJob.
--   * ACCESS SYSTEM TABLE -> unrestricted reads of STL_/STV_/SVV_/SYS_ views,
--                            required by RedshiftUnloadLogCheckerJob and
--                            RedshiftUserLoginDetectionJob.
GRANT ACCESS CATALOG      TO ROLE data_warehouse_admin;
GRANT ACCESS SYSTEM TABLE TO ROLE data_warehouse_admin;

-- ---------------------------------------------------------------------------
-- Object-level privileges
-- ---------------------------------------------------------------------------
-- RedshiftSyncJob and RedshiftMaskingJob must GRANT/REVOKE on the managed
-- schemas and their tables. A non-superuser can only do that on objects it
-- owns, so transfer ownership of the app-controlled schemas to the role.
--
-- 4b DECISION: the four DBT schemas (marts, qa_marts, fraudops_marts,
-- fraudops_qa_marts) are intentionally NOT reassigned. They are owned and
-- populated by their matching DBT users, and RedshiftSync assumes each DBT user
-- owns its own schema (ALTER DEFAULT PRIVILEGES FOR USER <schema> ...).
-- Reassigning them would break table creation for those users.
--
-- Run once per cluster; extend the list only for app-controlled schemas:

ALTER SCHEMA idp           OWNER TO ROLE data_warehouse_admin;
ALTER SCHEMA logs          OWNER TO ROLE data_warehouse_admin;
ALTER SCHEMA system_tables OWNER TO ROLE data_warehouse_admin;
ALTER SCHEMA fraudops      OWNER TO ROLE data_warehouse_admin;

-- Existing tables in those schemas must also be reassigned so the role can
-- GRANT/REVOKE on them. Generate the statements with, e.g.:
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
-- `rails_superuser` is the DB user the admin worker connects as; its password
-- is sourced from Secrets Manager `redshift/<env>-analytics-rails-superuser`.

GRANT ROLE data_warehouse_admin TO rails_superuser;
