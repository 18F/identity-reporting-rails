# Back-end Architecture

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
  - `config/redshift_config.yaml` — single source of truth for
    `enabled_aws_groups` (which DW groups are active per environment), shared by
    both syncs.
  - `config/quicksight_config.yaml` — QuickSight-specific role/group mappings.

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
