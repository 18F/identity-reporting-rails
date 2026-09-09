-- =============================================================================
-- Verify docs/redshift-system-privileges-matrix.csv against a live cluster
-- =============================================================================
-- The CSV is built from the AWS documentation. This file reproduces the same
-- matrix from actual cluster state so you can diff the two and resolve every
-- '?' cell. All statements are READ-ONLY.
--
-- Run against the {env}-analytics database. Query 1 must be run as the account
-- you are auditing; queries 2-6 need a superuser or sys:secadmin to see all
-- rows (SVV_ROLES and SVV_*_GRANTS are visibility-filtered).
--
-- Docs:
--   https://docs.aws.amazon.com/redshift/latest/dg/r_roles-default.html
--   https://docs.aws.amazon.com/redshift/latest/dg/r_roles-system-privileges.html
--   https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_system-tables.html#c_visibility-of-data
-- =============================================================================


-- 1. Fills the CSV's `superuser` column. usesuper = 't' means the documented
--    baseline applies as written; 'f' means the whole column is a hypothesis.
--    syslogaccess covers the "other users' rows" row in Section B.
SELECT current_database(),
       current_user,
       usename,
       usesuper,
       usecreatedb,
       syslogaccess,
       useconnlimit,
       sessiontimeout
FROM pg_user_info
WHERE usename = CURRENT_USER;


-- 2. Every system user's superuser attribute and syslog access in one pass.
--    Expect exactly one usesuper = 't' row (`superuser`); anything else is a
--    finding. Cross-check the usernames against redshift_config.yaml.
SELECT usename,
       usesuper,
       syslogaccess,
       usecreatedb
FROM pg_user_info
WHERE usename NOT LIKE 'IAM%'
ORDER BY usesuper DESC, usename;


-- 3. THE pivot: reproduces the CSV's sys: columns from live state. Diff this
--    output against Section A of the CSV. Any privilege appearing here that the
--    CSV marks blank -- or vice versa -- means the docs and your cluster
--    version disagree, and the cluster wins.
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


-- 4. Resolves the biggest open question in the CSV: does sys:superuser actually
--    carry only the RBAC permission table, or more?
--    NOTE: svv_system_privileges has NO admin_option column on this cluster
--    version -- system_privilege and identity_* are all that is available.
--    ANSWERED 2026-09-09 UTC (analytics database): 34 privileges, and it is NOT a
--    superset of sys:secadmin. All six RLS POLICY privileges plus EXPLAIN
--    MASKING and UNKNOWN belong to sys:secadmin alone.
SELECT system_privilege
FROM svv_system_privileges
WHERE identity_name = 'sys:superuser'
ORDER BY system_privilege;


-- 4b. The 8 privileges sys:secadmin holds that sys:superuser does not. This set
--     is why no single sys: role can serve the admin queue.
SELECT system_privilege
FROM svv_system_privileges
WHERE identity_name = 'sys:secadmin'
  AND system_privilege NOT IN (SELECT system_privilege
                               FROM svv_system_privileges
                               WHERE identity_name = 'sys:superuser')
ORDER BY system_privilege;


-- 5. Role membership for the connected user.
--    ANSWERED 2026-09-09: `superuser` is a member of dw_ingestion (admin_option
--    false), which RedshiftSync grants per redshift_config.yaml user_roles.
--    dw_ingestion carries object privileges, not system privileges, so
--    `superuser`'s administrative power still comes solely from usesuper = true
--    and there is no system-privilege role to copy for rails_superuser.
SELECT *
FROM svv_user_grants
WHERE user_name = CURRENT_USER;


-- 6. Role-to-role nesting. Redshift ships exactly one built-in edge
--    (sys:dba -> sys:operator). Extra rows mean someone granted a sys: role
--    into a custom role.
SELECT role_name, granted_role_name
FROM svv_role_grants
ORDER BY role_name, granted_role_name;


-- 7. Section B verification: which system tables the admin-queue jobs read, and
--    whether the account can actually see them. Run as the candidate user
--    (e.g. rails_superuser), not as `superuser`. An error or zero rows where
--    `superuser` sees rows means the table is superuser-visible and needs an
--    explicit GRANT SELECT -- SYSLOG ACCESS UNRESTRICTED will not help.
SELECT 'STL_UNLOAD_LOG'    AS system_table, COUNT(*) AS visible_rows FROM stl_unload_log
UNION ALL
SELECT 'SYS_CONNECTION_LOG',                COUNT(*)                 FROM sys_connection_log
UNION ALL
SELECT 'PG_TABLE_DEF',                      COUNT(*)                 FROM pg_table_def
UNION ALL
SELECT 'PG_USER',                           COUNT(*)                 FROM pg_user;


-- 8. Object ownership for the app-controlled schemas. A non-superuser can only
--    GRANT/REVOKE on objects it owns, so this determines whether the ownership
--    reassignment in the ALTER SCHEMA ... OWNER TO block of
--    db/redshift_roles/rails_superuser.sql is still required and which schemas
--    are safe to move.
--    ANSWERED 2026-09-09: all eight existing schemas are owned by `superuser`,
--    INCLUDING marts / qa_marts / fraudops_marts / fraudops_qa_marts. That
--    contradicted the old `4b DECISION` comment, which assumed each DBT user
--    owns its matching schema; the premise has since been corrected in that
--    file. (idp_core returned no row, so it does not exist in this database.)
SELECT nspname AS schema_name,
       pg_get_userbyid(nspowner) AS owner
FROM pg_namespace
WHERE nspname IN ('idp', 'idp_core', 'logs', 'system_tables', 'fraudops',
                  'marts', 'qa_marts', 'fraudops_marts', 'fraudops_qa_marts')
ORDER BY nspname;


-- =============================================================================
-- Follow-ups opened by the 2026-09-09 run
-- =============================================================================

-- 9. DECISIVE masking test. The docs say sys:secadmin manages DDM policies, but
--    no masking-policy privilege appears in svv_system_privileges for any role
--    -- while every RLS equivalent does. Only a functional test settles whether
--    RedshiftMaskingJob can run without a true superuser.
--    Test ATTACH as well as CREATE -- they are granted, and fail, independently,
--    and ATTACH is the one that fails silently (see 9b). Use a scratch table so
--    the probe never touches real PII:
--
--      -- as superuser:
--      CREATE SCHEMA IF NOT EXISTS probe_scratch;
--      CREATE TABLE probe_scratch.t (v VARCHAR(64));
--      CREATE USER secadmin_probe PASSWORD DISABLE;
--      GRANT ROLE "sys:secadmin" TO secadmin_probe;
--      GRANT USAGE ON SCHEMA probe_scratch TO secadmin_probe;
--      -- as secadmin_probe -- these two statements ARE the test:
--      CREATE MASKING POLICY probe_policy WITH (v VARCHAR(64)) USING ('***');
--      ATTACH MASKING POLICY probe_policy ON probe_scratch.t (v)
--        TO PUBLIC PRIORITY 99;
--      -- cleanup as superuser:
--      DROP SCHEMA probe_scratch CASCADE;
--      DROP MASKING POLICY probe_policy;
--      DROP USER secadmin_probe;
--
--    STILL UNANSWERED as of 2026-09-09 -- the reference SELECT below was run but
--    the CREATE MASKING POLICY probe was NOT. The privilege question is open.
--
--    Existing policies, for reference:
--    ANSWERED 2026-09-09: 24 policies = 8 masked columns x 3 permission types,
--    exactly matching RedshiftMasking::SqlExecutor::POLICY_USING_CLAUSES
--    ('masked' -> mask_* 'XXXX', 'allowed' -> unmask_* passthrough, 'denied' ->
--    deny_* NULL). Every policy takes a single parameter named `value`, matching
--    the WITH(value %<type>s) template. Masked columns are
--    fraudops.frd_email_addresses{,_zetl}.{email,encrypted_email},
--    fraudops.frd_{,encrypted_}events.message (message is SUPER),
--    system_tables.stl_query.querytxt, system_tables.stv_recents.query (CHAR(600)).
SELECT policy_name, input_columns, policy_expression
FROM svv_masking_policy
ORDER BY policy_name;


-- 9b. The ATTACH half of the question, which matters MORE than CREATE because
--     RedshiftMasking::SqlExecutor#execute_correction rescues
--     ActiveRecord::StatementInvalid and only logs a warning (sql_executor.rb:72).
--     A user lacking ATTACH/DETACH MASKING POLICY makes the job report success
--     while applying nothing -- silent PII exposure. CREATE, by contrast, is
--     unrescued (sql_executor.rb:38) and fails loudly.
SELECT grantee, COUNT(*) AS attachments, MIN(priority) AS min_pri, MAX(priority) AS max_pri
FROM svv_attached_masking_policy
GROUP BY grantee
ORDER BY grantee;


-- 10. STL_UNLOAD_LOG returned 0 rows as `superuser`, so query 7 is inconclusive
--     for that table -- there is nothing to compare a restricted user against.
--     STL views retain only 7 days, so confirm whether UNLOAD activity exists at
--     all before treating RedshiftUnloadLogCheckerJob's access as verified.
--     ANSWERED 2026-09-09: NULL / NULL / 0 -- the table is entirely EMPTY, not
--     merely filtered. No UNLOAD has run inside the retention window, so this
--     job's access CANNOT be verified by observation on this cluster. Either
--     trigger an UNLOAD first, or accept ACCESS SYSTEM TABLE on the docs alone.
SELECT MIN(start_time) AS oldest, MAX(start_time) AS newest, COUNT(*) AS rows
FROM stl_unload_log;


-- 11. Table-level catalog grants already in place. Among sys: roles only
--     sys:superuser holds ACCESS CATALOG, so per-table GRANT SELECT on
--     pg_catalog is the established workaround -- redshift_config.yaml already
--     does this for security_audit and rails_worker.
--     NOTE: the unfiltered form returned 9223 rows (every pg_catalog relation x
--     every identity, dominated by PUBLIC). Use the two narrowed forms below.

-- 11a. Collapse to one row per identity to see who holds pg_catalog grants at all.
--      ANSWERED 2026-09-09:
--        marts             SELECT  2540      qa_marts          SELECT  2540
--        fraudops_marts    SELECT  1846      fraudops_qa_marts SELECT  1846
--        public            SELECT   448      public            UPDATE     1
--        rails_worker      SELECT     1      security_audit    SELECT     1
--
--      NOT drift -- this is exactly what the config asks for. RedshiftSync
--      #create_system_user_privileges (redshift_sync.rb:427-434) branches on the
--      `tables:` key: when it is blank the grant becomes
--      "GRANT <priv> ON ALL TABLES IN SCHEMA pg_catalog". The four DBT users omit
--      `tables:`, so each gets blanket catalog SELECT. rails_worker and
--      security_audit declare `tables: [pg_user]` and get exactly one relation.
--
--      Two things to note:
--      (1) 2540 vs 1846 -- GRANT ON ALL TABLES is a POINT-IN-TIME snapshot; it
--          does not cover relations created afterwards and does not self-heal.
--          The 694-relation gap means the two pairs were last granted at
--          different times. Likely cause: fraudops_marts/fraudops_qa_marts are
--          gated on fraud_ops_tracker_enabled + dw_fraudops_email_enabled, so
--          RedshiftSync skips them whenever those flags are off, freezing their
--          grants while pg_catalog keeps growing across version upgrades.
--          Confirm by checking those two flags in this environment.
--      (2) rails_worker/security_audit already show 1 relation each even though
--          the pg_catalog blocks granting it are still UNCOMMITTED (staged) on
--          this branch -- so this cluster has run this branch's config. Treat it
--          as a sandbox/dev environment, not prod.
SELECT identity_name, identity_type, privilege_type, COUNT(*) AS relations
FROM svv_relation_privileges
WHERE namespace_name = 'pg_catalog'
GROUP BY identity_name, identity_type, privilege_type
ORDER BY relations DESC, identity_name;

-- 11b. Just the app-managed users. Expect pg_user for security_audit and
--      rails_worker; anything broader than redshift_config.yaml declares is drift.
--      ANSWERED 2026-09-09: 8774 rows = 2540+2540+1846+1846+1+1 exactly, so the
--      per-user totals in 11a account for every row and nothing else holds
--      pg_catalog grants. Neither rails_superuser nor data_warehouse_admin
--      appears -- consistent with neither existing yet (query 5 / system_users).
--      Too wide to read directly; use 11a, or add a relation_name filter.
SELECT identity_name, relation_name, privilege_type
FROM svv_relation_privileges
WHERE namespace_name = 'pg_catalog'
  AND identity_name IN ('rails_worker', 'security_audit', 'pii_reader',
                        'quicksight_connector', 'idp_connector', 'marts',
                        'qa_marts', 'fraudops_marts', 'fraudops_qa_marts',
                        'rails_superuser', 'data_warehouse_admin')
ORDER BY identity_name, relation_name;

-- 11c. PUBLIC holds UPDATE on exactly one pg_catalog relation. Identify it --
--      a writable catalog relation exposed to PUBLIC deserves an explicit
--      explanation, even if it turns out to be a Redshift built-in default.
--      ANSWERED 2026-09-09: pg_settings. BENIGN and NOT actionable -- this is
--      inherited PostgreSQL behavior, not a misconfiguration. pg_settings is a
--      view carrying an ON UPDATE DO INSTEAD rule that calls set_config(), so
--      "UPDATE pg_settings" is just another spelling of SET. It changes only the
--      issuing session's own GUCs and confers nothing a user could not already
--      do with SET. Do not revoke it; leave this row documented so it stops
--      being rediscovered as a finding.
SELECT relation_name, privilege_type, identity_name
FROM svv_relation_privileges
WHERE namespace_name = 'pg_catalog'
  AND identity_name = 'public'
  AND privilege_type <> 'SELECT';

-- 11d. Can the catalog relations the admin jobs need be granted individually?
--      The scoped `tables:` path is proven for pg_user (1 relation, above), but
--      RedshiftSystemTableSyncJob reads pg_table_def, which is a VIEW in
--      pg_catalog and may not accept a per-relation GRANT. Check that it is
--      grantable BEFORE relying on scoped grants instead of ACCESS CATALOG.
--      relkind: 'r' = ordinary table, 'v' = view. A relation missing from
--      pg_class entirely is leader-node-only and cannot be granted per-relation.
--      ANSWERED 2026-09-09 -- RESOLVED YES, by existing example:
--        pg_group      r  (table)
--        pg_table_def  v  (view)
--        pg_user       v  (view)
--      pg_user is ALSO a view, and rails_worker/security_audit already hold a
--      working per-relation grant on it (exactly 1 relation each in 11a). So
--      pg_catalog VIEWS are grantable per-relation, and pg_table_def is the same
--      relkind, present in pg_class, hence grantable the same way. The scoped
--      `tables:` path covers every catalog relation the admin jobs need, and
--      GRANT ACCESS CATALOG can be dropped from
--      db/redshift_roles/rails_superuser.sql. (Superseded by 11e below: no
--      catalog grant is needed at all, scoped or otherwise.)
SELECT n.nspname AS schema_name, c.relname AS relation_name, c.relkind
FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog'
  AND c.relname IN ('pg_table_def', 'pg_user', 'pg_group')
ORDER BY c.relname;

--      If you want belt-and-braces confirmation, attempt the grant directly:
--        CREATE USER catalog_probe PASSWORD DISABLE;
--        GRANT USAGE ON SCHEMA pg_catalog TO catalog_probe;
--        GRANT SELECT ON pg_catalog.pg_table_def TO catalog_probe;
--        DROP USER catalog_probe;


-- 11e. Is the explicit pg_user grant even load-bearing? PUBLIC holds SELECT on
--      448 pg_catalog relations. If pg_user is among them, the
--      `tables: [pg_user]` block for rails_worker/security_audit is a no-op and
--      the real reason those jobs need :admin lies elsewhere.
--      ANSWERED 2026-09-09: PUBLIC holds SELECT on ALL THREE -- pg_user,
--      pg_table_def AND pg_group. Consequences:
--      (1) The `tables: [pg_user]` pg_catalog blocks added for rails_worker and
--          security_audit (still STAGED, uncommitted on this branch) are NO-OPS.
--          PUBLIC already grants that read. They can be dropped.
--      (2) rails_superuser needs NO catalog grant at all for these relations --
--          not ACCESS CATALOG, and not a scoped `tables:` block either. This
--          supersedes the scoped-grant recommendation in 11d: the smallest
--          correct change is to grant nothing and delete the
--          GRANT ACCESS CATALOG statement from
--          db/redshift_roles/rails_superuser.sql. (Done 2026-09-09.)
--      (3) Whatever makes RedshiftSystemTableSyncJob and
--          RedshiftUnexpectedUserDetectionJob need :admin, it is NOT pg_user or
--          pg_table_def visibility. See query 12 -- for the sync job it is
--          search_path, not privilege at all.
SELECT relation_name, identity_name, privilege_type
FROM svv_relation_privileges
WHERE namespace_name = 'pg_catalog'
  AND relation_name IN ('pg_user', 'pg_table_def', 'pg_group')
ORDER BY relation_name, identity_name;


-- 12. CONFIRMED LATENT DEFECT, pre-existing and unrelated to rails_superuser.
--     RedshiftSystemTableSyncJob#convert_source_char_columns is a permanent
--     silent no-op on Redshift.
--
--     Mechanism. The job reads columns two ways
--     (app/jobs/redshift_system_table_sync_job.rb:164-186):
--       * fetch_source_columns -> pg_table_def WHERE schemaname = 'pg_catalog'
--         WORKS. pg_catalog is implicitly on every search_path, always.
--       * fetch_target_columns -> pg_table_def WHERE schemaname = 'system_tables'
--         RETURNS NOTHING. system_tables is NOT on search_path, and AWS documents
--         PG_TABLE_DEF as filtering by search_path -- a filter no privilege
--         lifts, superuser included.
--     fetch_target_columns has exactly one caller, line 196:
--       col = fetch_target_columns.find { |c| source_char_type?(c['type']) }
--       break unless col
--     With zero rows, col is always nil, the loop breaks on its first iteration,
--     and the CHAR(n) -> VARCHAR(n) conversion never runs. Nothing raises.
--
--     Corroborated independently by query 9: the live masking policy
--     mask_system_tables_stv_recents_query still has input type character(600).
--     Converting exactly that CHAR column to VARCHAR(600) is the method's whole
--     purpose (source_char_type? at :215, redshift_data_type /^char/ at :305), so
--     its survival proves the body has never executed.
--
--     Not caught by tests: the method returns early unless dw_redshift?, and
--     local/test runs on Postgres take the information_schema branch instead. The
--     only path where it does anything is the only path that is broken.
--
--     Fix is one line, and search_path is the wrong lever -- prefer svv_columns,
--     which is not search_path-dependent and which missing_system_table_columns
--     (:115-141) ALREADY uses for this same target schema.
--
--     ANSWERED 2026-09-09: search_path = '$user, public'. pg_table_def returns 0
--     rows for idp/logs/system_tables/fraudops. Note the job never reads
--     idp/logs/fraudops -- source_schema is pg_catalog for all 13 entries in
--     config/redshift_system_tables.yml -- so system_tables is the only one of
--     the four that matters here.
SHOW search_path;

-- Only system_tables is relevant to this job; pg_catalog is the control.
SELECT 'system_tables' AS schema_name, COUNT(*) AS visible_columns
FROM pg_table_def WHERE schemaname = 'system_tables'
UNION ALL
SELECT 'pg_catalog', COUNT(*)
FROM pg_table_def WHERE schemaname = 'pg_catalog';

-- Does the same target resolve through svv_columns (the proposed fix)?
SELECT COUNT(*) AS svv_visible_columns
FROM svv_columns
WHERE table_schema = 'system_tables';

-- The smoking gun: any CHAR/BPCHAR left in system_tables is a column
-- convert_source_char_columns was supposed to have converted.
SELECT table_name, column_name, data_type, character_maximum_length
FROM svv_columns
WHERE table_schema = 'system_tables'
  AND data_type ILIKE '%char%'
  AND data_type NOT ILIKE '%varying%'
ORDER BY table_name, column_name;
