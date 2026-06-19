# QuickSight User Sync Migration to Rails

**Issue:** [#1348](https://gitlab.login.gov/lg-teams/Team-Data/data-warehouse-ag/-/issues/1348) — QS: Move QS Sync to rails (as done for Redshift)
**Date:** 2026-06-16
**Status:** Design — awaiting review

## Problem

The QuickSight user sync currently runs as a Python AWS Lambda living in
`identity-devops`
(`terraform/modules/quicksight/lambda/quicksight_user_sync/src/quicksight_user_sync.py`,
269 lines). It runs hourly, fetches `users.yaml` from GitHub at runtime (via
PyGithub + a Secrets Manager token), and syncs users + group memberships into
AWS QuickSight.

We want to migrate the execution logic into `identity-reporting-rails`,
following the same pattern used for the Redshift usersync migration (#1207), to
standardize sync logic, improve testability, and enable faster iteration.

This is blocked by #1332 (QuickSight production release) and should not ship
until that is in production.

## Goals / Acceptance Criteria

- [ ] QuickSight user sync execution logic implemented in this repository.
- [ ] Configuration remains in `identity-devops` (`users.yaml` read from the
      local devops path; no runtime GitHub fetch).
- [ ] Behavior matches or improves upon the existing Lambda.
- [ ] Documentation describes the config-vs-execution split and how to run/test
      QuickSight usersync locally.

## Non-Goals

- Migrating the QuickSight Terraform infra (groups, IAM, alarms) — that stays in
  `identity-devops`.
- Touching the separate GitLab Go user-sync (`bin/users/sync.go`).
- Refactoring the existing Redshift sync code beyond reading shared config keys
  (`enabled_aws_groups`, `aws_role_map`, `role_priority`).

## Architecture

Mirror the Redshift sync pattern one-to-one:

| New file | Mirrors | Purpose |
|---|---|---|
| `app/services/quicksight_sync.rb` | `app/services/redshift_sync.rb` | `QuicksightSync#sync` — the ported sync logic |
| `app/jobs/quicksight_sync_job.rb` | `app/jobs/redshift_sync_job.rb` | GoodJob wrapper, structured logging, concurrency limit 1 |
| `config/quicksight_config.yaml` | `config/redshift_config.yaml` | QS-specific role/group mappings |
| `spec/services/quicksight_sync_spec.rb` | `spec/services/redshift_sync_spec.rb` | RSpec, stubbed `Aws::QuickSight::Client` |
| `spec/jobs/quicksight_sync_job_spec.rb` | `spec/jobs/redshift_sync_job_spec.rb` | job-level spec |
| `Gemfile` (+ cron entry) | — | add `aws-sdk-quicksight`; schedule `quicksight_sync_job` |

### Deliberate divergences from the Python Lambda

1. **No GitHub fetch / Secrets Manager token.** `users.yaml` is read locally via
   `IdentityConfig.identity_devops_users_yaml_path`, exactly like `RedshiftSync`.
   This satisfies AC #2 and sidesteps the usersync caching concern raised in the
   [Slack thread](https://gsa-tts.slack.com/archives/C093VFMQL5T/p1775577832353379).
2. **No PyGithub/PyYAML dependencies.** Only `aws-sdk-quicksight` plus Ruby
   stdlib `yaml`.

## Data Flow

```
users.yaml (local devops path)
  → filter users by enabled_aws_groups[env_type]   (read from redshift_config.yaml)
  → map each user's aws_groups → highest-priority DW role
  → derive QS group (QSAdmin/QSPowerUser/QSUser) + QS username ({role}/{email_localpart})
  → diff against list_users from the QuickSight API
  → register_user + create_group_membership for new users
  → delete_user for removed users (excluding protected accounts)
  → flag_users_with_pro_roles (log-only cost guard)
```

## Configuration

`enabled_aws_groups` is **not** duplicated. It already lives in
`config/redshift_config.yaml` (lines 2–13) as the single source of truth for
"which DW groups are active per environment", and `QuicksightSync` reads that
key directly.

**Verified (2026-06-16)** against the Lambda's `enabled_aws_groups()`
(lines 221–232): the per-env group sets match exactly —
prod `{dwuser, dwpoweruser, dwadmin}`, sandbox adds the three `*nonprod`
variants. (Order differs but it is a membership test.)

**Env-type mapping difference (intentional):** the Lambda keys on an `ENV_TYPE`
env var (`prod`/`sandbox`). `QuicksightSync` instead reuses
`RedshiftSync#env_type`, which derives `prod` for env ∈ `[prod, dm, staging]`
and `sandbox` otherwise. This means `dm` and `staging` are treated as `prod` in
the Rails version, consistent with the sibling Redshift sync.

The shared DW role model lives in `config/redshift_config.yaml` (alongside
`enabled_aws_groups`), since it is not QuickSight-specific and is also the basis
for the Redshift role mapping:

```yaml
# users.yaml aws_group -> normalized DW role
aws_role_map:
  dwadmin: DWAdmin
  dwadminnonprod: DWAdmin
  dwpoweruser: DWPowerUser
  dwpowerusernonprod: DWPowerUser
  dwuser: DWUser
  dwusernonprod: DWUser

# DW role -> priority (higher wins when a user has multiple roles)
role_priority:
  DWAdmin: 3
  DWPowerUser: 2
  DWUser: 1
```

`config/quicksight_config.yaml` holds only QuickSight-specific mappings:

```yaml
# DW role -> QuickSight API role (register_user)
quicksight_aws_role:
  DWAdmin: ADMIN
  DWPowerUser: AUTHOR
  DWUser: AUTHOR
  default: READER

# DW role -> QuickSight group (create_group_membership)
quicksight_group:
  DWAdmin: QSAdmin
  DWPowerUser: QSPowerUser
  DWUser: QSUser
  default: QSUser

# Accounts never deleted by drop_users (matched against the QS user's Email).
# FullAdministrator/* matched by prefix in code, not listed here.
protected_accounts:
  - identity-devops@login.gov

# Accounts in users.yaml that are not human users and are skipped entirely.
non_human_accounts:
  - root
  - project_21_bot

# users.yaml username -> extra QS roles to create in addition to the user's
# highest-priority role. Lets a single user have multiple accounts on one email
# (e.g. both DWAdmin/x and FullAdministrator/x) for special cases such as
# troubleshooting. Two *different* users.yaml entries can never share an email;
# the first wins and the rest are skipped (with a warning).
multi_account_allowlist: {}

# Default email domain: used both to build a default email for users with no
# explicit email in users.yaml ("<user>@gsa.gov") and to strip from the email
# when building the QS username. Verified hardcoded to gsa.gov in the Lambda
# (strip_email_domain, line 34; default email, line 215).
default_email_domain: gsa.gov
```

`*nonprod` groups are excluded from prod via the `enabled_aws_groups` filter, so
they never reach role mapping in prod (matching the Lambda's
`normalize_aws_role` returning `None` for nonprod in prod).

### QS username construction (verified)

QS usernames are `"#{dw_role}/#{email_without_gsa_domain}"`. The Lambda strips
**only** `@gsa.gov` (`strip_email_domain`, line 34): a non-gsa.gov email keeps
its full domain in the username. The default email for a user without an
explicit `email` in `users.yaml` is `"<username>@gsa.gov"` (line 215).

## Service Internals (`QuicksightSync`)

Public method `#sync` orchestrates, matching the Lambda's `lambda_handler`:

```
list existing QS users  →  compute expected users from users.yaml
create_users (new only)  →  drop_users (removed, minus protected)
flag_users_with_pro_roles (log-only cost guard)
```

Private helpers (ported from the Python source, mappings driven by config):

| Ruby method | Python origin | Notes |
|---|---|---|
| `env_name` / `env_type` | (copied from `RedshiftSync`) | prod/dm/staging → `prod`, else `sandbox` |
| `users_yaml` | `load_users_yaml` | local file read, no GitHub |
| `enabled_aws_groups` | `enabled_aws_groups` | from `redshift_config.yaml` |
| `normalize_aws_role` | lines 74–87 | from `aws_role_map` (in `redshift_config.yaml`) |
| `role_priority` | `get_role_priority` | from `role_priority` (in `redshift_config.yaml`) |
| `quicksight_aws_role` | lines 111–121 | from `quicksight_aws_role` |
| `quicksight_group` | lines 123–132 | from `quicksight_group` |
| `build_qs_username` | line 38 | `"#{role}/#{email.sub('@gsa.gov', '')}"` — strips only `@gsa.gov` |
| `highest_aws_role` | lines 102–108 | max valid role by `role_priority`; `nil` if none |
| `filtered_yaml_email_mapping` | lines 202–218 | skip `non_human_accounts` + users with no `aws_groups`; default email `"<user>@gsa.gov"`; duplicate emails resolved first-wins with a warning |
| `multi_account_allowlist` | — (Rails addition) | per-user extra roles always created alongside the highest-priority account (e.g. `FullAdministrator`); exempt from the one-account-per-user collapse |
| `create_users` | lines 135–187 | #1318 logic: one account per user, highest-priority role, upgrade only if no higher-priority account exists |
| `drop_users` | lines 189–199 | skip `protected_accounts` (by Email) and `FullAdministrator/*` (by UserName prefix) |
| `flag_users_with_pro_roles` | lines 235–238 | log warn on `*_PRO` roles |

AWS client (mirrors `RedshiftSync#secrets_manager_client`):

```ruby
def quicksight_client
  @quicksight_client ||= Aws::QuickSight::Client.new(region: Identity::Hostdata.config.aws_region)
end
```

Four QuickSight API calls to wrap: `list_users`, `register_user`
(IdentityType `IAM`), `delete_user`, `create_group_membership` — all require
`aws_account_id` and `namespace: 'default'`.

**Verified (2026-06-16):** `aws_account_id` comes from
`Aws::STS::Client#get_caller_identity` (Lambda line 242:
`boto3.client("sts").get_caller_identity()["Account"]`), and all four calls use
`Namespace="default"` (lines 31, 51, 59, 69).

## Error Handling

> **Deliberate improvement over the Lambda (not a faithful port).** The current
> Lambda wraps its entire body in `try/except Exception` that only
> `logger.exception(...)` and returns — it **never re-raises** (lines 247–269).
> So production QS sync failures are effectively invisible today: the Lambda
> always "succeeds". The Rails version intentionally changes this to surface
> failures via the existing `reportingRails-*-failed` alerting. AC #3 permits
> this ("matches **or improves**").

Per-user catch-and-log with a guaranteed raise at the end if anything failed —
matching the existing `PiiRetentionEnforcementJob` pattern
(`app/jobs/pii_retention_enforcement_job.rb:31`):

```
errors = []
for each user to create/delete:
  begin
    perform the API call
  rescue Aws::QuickSight::Errors::ServiceError => e
    log per-user failure; errors << { user:, error: e }
  end
raise "QuickSight sync failed for #{errors.size} user(s): ..." if errors.any?
```

Rationale:
- One bad user does not block the rest of the sync (max progress each run).
- The job still raises at the end, so GoodJob marks it failed and the existing
  `reportingRails-*-failed` alerting fires.
- Each run is a full diff, so transient failures self-heal on the next run.
- `rescue` is scoped to `Aws::QuickSight::Errors::ServiceError` only —
  programming errors and config errors still fail loudly and immediately.

## Job & Scheduling

`QuicksightSyncJob` mirrors `RedshiftSyncJob`: `GoodJob` concurrency limit of 1,
structured JSON logging via `IdentityJobLogSubscriber`, re-raises on failure.

Scheduled every 15 minutes (`cron_15m`) in
`config/initializers/job_configurations.rb`, consistent with
`redshift_sync_job` (upgraded from the Lambda's hourly cadence).

## Testing

- `spec/services/quicksight_sync_spec.rb`: stub `Aws::QuickSight::Client` (and
  STS) with `instance_double`, stub `users_yaml`, assert the correct
  create/delete/group-membership calls for a representative `users.yaml`. Cover:
  new user, removed user, protected-account skip, role upgrade, role demotion,
  highest-priority selection, `*nonprod`-in-prod exclusion, duplicate-email
  first-wins, the `multi_account_allowlist`, and per-user error aggregation.
- `spec/jobs/quicksight_sync_job_spec.rb`: assert the service is invoked and
  success/failure logging behaves like `RedshiftSyncJob`.
- Follow the existing `redshift_sync_spec.rb` stubbing conventions
  (RSpec + WebMock already in the suite).

## Documentation

Update backend docs to describe the config-vs-execution split (config in
`identity-devops` `users.yaml` + `redshift_config.yaml`; execution in this repo)
and how to run/test QuickSight usersync locally.

## Verified Against Lambda Source (2026-06-16)

All pre-implementation unknowns have been confirmed against
`quicksight_user_sync.py`:

1. `enabled_aws_groups` per-env lists match `redshift_config.yaml` lines 2–13
   (Lambda lines 221–232). ✅
2. `aws_account_id` source is STS `get_caller_identity` (line 242). ✅
3. QuickSight `namespace` is `default` on all four API calls
   (lines 31, 51, 59, 69). ✅
4. QS username strips only `@gsa.gov` (line 34); default email is
   `"<user>@gsa.gov"` (line 215). ✅
5. `non_human_accounts` = `["root", "project_21_bot"]` (line 245). ✅
6. Lambda currently swallows all errors and never re-raises (lines 247–269);
   the Rails version intentionally raises to enable alerting. ✅

## Dependencies

- #1332 (QuickSight production release) — **released**, no longer blocking.
- Soft: #1338 may simplify scope; #1322 related. Neither is a hard dependency.
