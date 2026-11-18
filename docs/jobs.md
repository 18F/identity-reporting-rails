# Jobs

## Logs extraction

`ExtractorRowCheckerEnqueueJob` starts at the configured
`extractor_row_checker_enqueue_cron` schedule. Every invocation enqueues
`PiiRowCheckerJob` for each table in the `logs` schema, even if work from a
previous invocation is still pending. Checks for target tables enqueue
`LogsColumnExtractorJob`. Per-table concurrency limits apply independently to
the checker and extractor jobs.

Extractor status messages include `job_id` for correlation. An empty source logs
`success: true` with `No source records to extract; skipping`. Caught extraction
errors log `success: false`, which matches the dedicated extractor failure
metric filter. These caught errors return normally, so inspect the application
logs rather than relying on GoodJob completion status alone.

## User Syncs (Redshift & QuickSight)

This app owns the **execution** logic for syncing users into the data
warehouse. The **configuration** (which humans exist and which AWS groups they
belong to) stays in `identity-devops`.

### Config vs. execution split

- **Config (in `identity-devops`):**
  - `terraform/master/global/users.yaml` — the source of truth for users and
    their `aws_groups`. Read locally via
    `IdentityConfig.identity_devops_users_yaml_path` (no runtime GitHub fetch).
- **Execution (in this repo):**
  - `config/redshift_config.yaml` — single source of truth for the DW role
    model shared by both syncs: `enabled_aws_groups` (which DW groups are active
    per environment), `aws_role_map` (`aws_group` → normalized DW role), and
    `role_priority` (which role wins when a user has several).
  - `config/quicksight_config.yaml` — QuickSight-specific mappings only
    (`quicksight_aws_role`, `quicksight_group`, `protected_accounts`,
    `non_human_accounts`, `default_email_domain`).
  - `quicksight_multi_account_allowlist` (an `IdentityConfig` value, set via the
    deployed `application.yml`) — `users.yaml` username → extra QS roles to
    create alongside a user's highest-priority account. It lives in deployed
    config rather than this public repo so individual usernames are not
    committed here. Defaults to `{}`.

### Redshift sync

- Service: `app/services/redshift_sync.rb`
- Job: `app/jobs/redshift_sync_job.rb` (`RedshiftSyncJob`, every 15 minutes)

### QuickSight sync

- Service: `app/services/quicksight_sync.rb`
- Job: `app/jobs/quicksight_sync_job.rb` (`QuicksightSyncJob`, every 15 minutes)

`QuicksightSync#sync`:

1. Reads `users.yaml` locally and filters users by
   `enabled_aws_groups[env_type]` (from `redshift_config.yaml`).
2. Maps each user's `aws_groups` to the highest-priority DW role and derives a
   QuickSight group (`QSAdmin`/`QSPowerUser`/`QSUser`) and QS username
   (`{role}/{email-localpart}`).
3. Diffs against the QuickSight API (`list_users`) and `register_user` +
   `create_group_membership` for new users, `delete_user` for removed ones
   (excluding protected accounts and `FullAdministrator/*`).
4. Logs a warning for any users with `*_PRO` roles (cost guard).

Each run is a full diff, so transient per-user failures self-heal on the next
run. Failures are aggregated per-user and the job raises at the end so GoodJob
marks it failed and the `reportingRails-*-failed` alert fires.

`env_type` derives `prod` for env ∈ `[prod, dm, staging]` and `sandbox`
otherwise (consistent with the Redshift sync), so `*nonprod` groups never reach
role mapping in prod.

#### Running / testing locally

QuickSight sync requires AWS credentials with QuickSight + STS access, so it is
not typically exercised end-to-end locally. To run the unit specs:

```sh
bundle exec rspec spec/services/quicksight_sync_spec.rb
bundle exec rspec spec/jobs/quicksight_sync_job_spec.rb
```

The service specs stub `Aws::QuickSight::Client`, `Aws::STS::Client`, and
`users_yaml`, so no live AWS or GitHub access is needed.

### Known follow-ups / tech debt

- **Unify the DW role model in one place.** The `aws_group` → DW role
  relationship is currently defined twice: declaratively in
  `config/redshift_config.yaml` (`aws_role_map`, consumed by
  `QuicksightSync`) and imperatively as the hardcoded
  `RedshiftMasking::UserResolver::IAM_ROLE_GROUPS` constant
  (`app/services/redshift_masking/user_resolver.rb`). These encode the same
  knowledge in two forms and can drift apart. A future change should drive
  `UserResolver` from `redshift_config.yaml`'s `aws_role_map` (or a shared
  loader) so there is a single source of truth. Verify all `UserResolver`
  consumers and the redshift masking specs when doing this.

### Redshift role audit

`RedshiftSync` compares role state with `config/redshift_config.yaml` at the end
of each pass (`RedshiftRoleAudit`, no separate job). `sync_cluster` checks the
cluster-wide views (`svv_roles`, `svv_user_grants`, `svv_role_grants`);
`sync_database_grants` checks object privileges per database, because
`svv_*_privileges` only show the current database. Findings: unmanaged roles,
unexpected members, `ADMIN OPTION`, nested roles, inheritors, users who reach a
managed role through a chain (report only), and any object privilege on a
managed role.

- Findings are always logged as JSON with `name: "RedshiftRoleAudit"`. A summary
  line carries `findings_detected`, which is what alerting matches.
- Redshift has no DENY and grants add up, so the audit can only `REVOKE` from a
  managed role. That is off unless `redshift_role_audit_revoke` is `true`
  (default `false`, set in deployed config).
- A failed revoke, or a failed catalog read, is added to `RedshiftSync#errors`
  and fails the sync like any other statement failure.
- A role whose feature flag is off is intended to have no members and no nested
  roles; its stale members are now found (and revoked when revoke is on). A role
  whose intended state cannot be computed (`SyncError`) is skipped and logged.
- It never drops a role, changes `sys:` roles' own grants, users' direct
  privileges or groups, or anything for roles outside the config.

Rollout: deploy with revoke off, read one run's `RedshiftRoleAudit` logs, decide,
then set `redshift_role_audit_revoke: true`. See the [decision
record](decisions/0002-redshift-role-audit.md) and the [human validation
queries](role-audit-validation.md).

## Backfilling flattened frd_events columns

`FraudOpsPiiDecryptJob` populates the flattened event columns (`event_type`,
`success`, `device_id`, `user_ip_address`, `agency_uuid`, `unique_session_id`)
for events ingested going forward. To populate historical rows from the retained
`message` blob, run the one-time, batched, idempotent backfill:

    bundle exec rails frd_events:backfill_columns          # default batch size 1000
    bundle exec rails frd_events:backfill_columns[5000]    # custom batch size

It only updates rows where `event_type IS NULL`, so it is safe to re-run and to
run while ingestion continues. No ingestion pause is required.

## Redshift masking reconciliation

Two admin-queue jobs run every five minutes behind the existing fraudops feature
gate (either fraudops flag enabled):

- `RedshiftMaskingJob` calls `sync_configured_columns(user_filter: nil)` using
  identity-devops `mask.yaml`. Actual attachments are compared only in schemas
  listed in that configuration, preserving `idp`, `fraudops`, and `system_tables`
  while leaving dbt marts outside that scope. `sync` remains a compatibility wrapper.
- `RedshiftDbtMaskingJob` explicitly invokes `sync_dbt_schema(schema:)` for each
  schema in `config/dbt_masking.yaml`. A manual single-schema invocation never
  expands to both schemas. The mapping references existing dbt-created policies;
  Rails performs no policy-definition DDL.

The dbt path discovers every physical table and column, including orphan tables,
and expects PUBLIC masking at priority 0 plus ROLE `pii_viewers` unmasking at `dbt_unmask_priority` (default 100).
Direct-user attachments are left untouched during their separate transition.
Views are excluded with warnings; unsupported stored relations or type/size
mappings fail preflight. Existing policy names and the role must be visible.

Only tables with discovered drift receive an explicit lock. Inside a per-table
transaction, reconciliation sets a bounded per-statement timeout, locks the table,
rechecks metadata, then applies fresh corrections. Healthy tables are not explicitly
locked. Repairs remain expected on subsequent runs regardless of their grantor.
Failures propagate instead of logging successful completion; earlier committed
table repairs may remain after a later failure. GoodJob limits overlapping
scheduled executions of each job class.

See [human-only QA validation](dbt-masking-validation.md) before deploying; live
Redshift catalog decoding, compatibility, and transaction/locking behavior have
not been established by the mocked specs.
