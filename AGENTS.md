# AGENTS.md

Guidance for AI coding agents working in the Login.gov Identity Reporting Rails
repository.

## Overview

This is the backend reporting and management application for the Login.gov data
warehouse. Its primary responsibilities are:

- Administrative access within the VPC (managing Redshift users/roles via SQL).
- Running background jobs/workers (data processing, report generation).
- Managing data warehouse migrations in source code.

Ruby on Rails 8.1 app. Ruby version is pinned in `.ruby-version` (currently
3.4.5). Background jobs use ActiveJob + GoodJob.

## Dev Environment (devenv / Nix)

The canonical development environment is managed by
[devenv](https://devenv.sh/) (Nix-based) and auto-activated by
[direnv](https://direnv.net/):

- `.envrc` runs `use devenv`, so entering the repo directory activates the
  environment automatically (`direnv allow` on first use). Without direnv, run
  `devenv shell` manually.
- `devenv.nix` provisions Ruby (from `.ruby-version`), Bundler, PostgreSQL 16,
  Redis, and CLI tools (`glab`, `gnumake`, `detect-secrets`). A `bundle install`
  task runs on shell entry.
- A **`detect-secrets` pre-commit git hook** is configured in `devenv.nix`. It
  blocks commits containing high-entropy strings (likely secrets), checked
  against `.secrets.baseline`. Commits made by agents will run this hook. The
  hook's `detect-secrets-hook` executable is only on `PATH` inside the devenv
  shell, so run `git commit` from within `devenv shell` (or the hook fails with
  "Executable `detect-secrets-hook` not found").
- PostgreSQL and Redis are provided by devenv as services, not system installs,
  when using this path. Start them with `devenv up` (see Running services).

A manual Homebrew + rbenv path is also documented in
`docs/local-development.md` (uses the `Brewfile` and `make setup`). Prefer the
devenv path unless you have a reason not to.

### Running services (PostgreSQL + Redis)

The Postgres and Redis services declared in `devenv.nix` do **not** start on
shell entry — start them with `devenv up`. Both are required before running the
test suite or `bin/setup`.

- `devenv` / `direnv` live on `PATH` only via the Nix profile. If commands like
  `devenv` are "command not found" (e.g. a non-interactive/sandbox shell that
  didn't source direnv), prepend it:
  `export PATH="$HOME/.nix-profile/bin:$PATH"`. Then run everything through
  `devenv shell -- <cmd>` (or enter `devenv shell`) so Ruby/Bundler resolve.
- The default `devenv up` uses a full-screen TUI that needs a real terminal; in
  a headless/sandbox environment it fails with `open /dev/tty: no such device`.
  Use `devenv up -d` (detached) there, then `devenv processes stop` to stop.
- If Postgres was killed uncleanly it leaves a stale
  `.devenv/state/postgres/postmaster.pid` and refuses to restart (`lock file
  "postmaster.pid" already exists` / connection refused). Fix: stop everything
  (`devenv processes stop`, `pkill -f "postgres -D"`), remove the stale pid
  (`rm -f .devenv/state/postgres/postmaster.pid`), then `devenv up -d` again.
- Transient `FATAL: role "agent" does not exist` lines in the postgres log while
  the service comes up are harmless startup probes — the cluster is
  `postgres`-owned by design (see `services.postgres`).
- Data lives under `.devenv/state/` (e.g. `.devenv/state/postgres`); it does not
  exist until the service has been started at least once.

### Running the test suite from a clean checkout

`devenv.nix`'s `enterShell` auto-creates `config/application.yml` from the
default and defaults `POSTGRES_USER=postgres` (which `config/database.yml` reads
in test), so no manual config copy is needed. The remaining steps are:

```sh
export PATH="$HOME/.nix-profile/bin:$PATH"   # only if devenv isn't on PATH
devenv up -d                                  # start Postgres + Redis (detached)
devenv shell -- bash -c 'RAILS_ENV=test bin/rails db:prepare'  # first run only
devenv shell -- bash -c 'make test'
```

A running Redis is required for the **whole** suite: `spec/rails_helper.rb`
flushes Redis in a `before(:each)` hook, so without it every spec fails with
`Redis::CannotConnectError`.

Leave the devenv services (Postgres + Redis) **running** after a test run —
do not stop them and do not ask whether to stop them. Only run
`devenv processes stop` if the user explicitly asks.

`RedshiftUnexpectedUserDetectionJob` specs assume the local Postgres/Redshift
user is `postgres`; `devenv.nix` bootstraps the cluster with a `postgres`
superuser (see [Running services](#running-services-postgresql--redis)) so this
holds regardless of the OS user name (e.g. `agent`). If you have an existing
`.devenv/state/postgres` created before that change, delete it so the cluster is
re-initialized with the right user.

## Setup & Common Commands

All common tasks are exposed through the `Makefile`. Prefer these over raw
commands.

- `make setup` — Run setup scripts (`bin/setup`): packages, dependencies, databases, config files.
- `make fast_setup` — Abbreviated setup that skips linking some files.
- `make run` — Start the development server (runs the `Procfile`: `web` =
  `rackup config.ru`, `worker` = `good_job start`).
- `make test` — Run the full local RSpec suite (`RAILS_ENV=test`, `bundle exec rspec`).
- `make test_serial` — Run RSpec serially (non-parallel).
- `make fast_test` — RSpec without accessibility specs.
- `make lint` — Run all linters (rubocop, brakeman, lockfile/readme/migration checks).
- `make lintfix` — Auto-fix rubocop + normalize YAML.
- `make brakeman` — Security scan.
- `make audit` — `bundler-audit` dependency vulnerability check.
- `make check` — Runs `lint` then `test`.
- `make update` — `bundle install` + `rails db:migrate` (after a git pull).

Run `make help` to list all available targets.

## Testing

- Framework: **RSpec** (`rspec-rails`), with FactoryBot, Shoulda Matchers,
  WebMock, and SimpleCov.
- Specs live in `spec/`. Factories live in `spec/factories/`.
- Local `make test` runs `bundle exec rspec`. CI parallelizes specs with
  Knapsack across GitLab nodes.
- Prefer running a single spec file or example during development rather than
  the full suite:
  - `bundle exec rspec spec/jobs/data_freshness_job_spec.rb`
  - `bundle exec rspec spec/path/to/spec.rb:LINE`
- Always run the relevant specs after making code changes.

For environment setup (starting services, preparing databases, the Redis
requirement, and the `postgres` user assumption), see
[Running the test suite from a clean checkout](#running-the-test-suite-from-a-clean-checkout).

## Linting & Conventions

- Ruby is linted with **RuboCop** (`rubocop-rails`, `rubocop-rspec`,
  `rubocop-performance`). Config in `.rubocop.yml` (`DisabledByDefault: true` —
  only explicitly enabled cops run).
- Target Ruby 3.4, Target Rails 8.1.
- Run `make lint` before finishing a change; use `make lintfix` to auto-correct.
- Prefer self-documenting code over excessive comments.
- The `README.md` is **auto-generated** from `docs/`. Do not edit it directly —
  run `make README.md` to regenerate. CI fails if it is out of sync.

## Project Structure

- `app/jobs/` — Background jobs (Redshift sync, PII checks, reports, etc.).
  This is the heart of the app; most work happens here.
- `app/models/` — ActiveRecord models. Note the multiple database base classes:
  - `ApplicationRecord` (primary)
  - `DataWarehouseApplicationRecord` (Redshift / data warehouse)
  - `WorkerJobApplicationRecord` (GoodJob)
- `app/services/` — Service objects (e.g. `redshift_sync.rb`, masking).
- `lib/reporting/` — Report generation classes.
- `lib/tasks/` — Custom Rake tasks (e.g. migration checks, schema updates).
- `config/` — Rails config. Key files: `lib/identity_config.rb`,
  `config/redshift_config.yaml`, `config/pii_retention.yml`,
  `config/redshift_system_tables.yml`.

## Databases

This app connects to **multiple databases** (see `config/database.yml`):

- `primary` (+ `read_replica`) — PostgreSQL application DB.
- `worker_jobs` — PostgreSQL DB for GoodJob.
- `data_warehouse` — **Amazon Redshift** in production; PostgreSQL locally/test.
  Uses `activerecord-redshift-adapter`. `pg` is pinned to 1.5.9 for Redshift
  compatibility (< pgsql 10).

Migrations are separated by database:

- `db/primary_migrate/` (configured in `config/database.yml`; the primary DB
  has no migrations yet — `db/schema.rb` is currently empty/version 0).
- `db/worker_jobs_migrate/`
- `db/data_warehouse_migrate/` (+ `db/data_warehouse_test_migrate/` for tests)

When adding a migration, place it in the correct directory and confirm the
target database. Migration linting runs via `scripts/migration_check`.

## Security & Sensitive Data

This is a federal (GSA / Login.gov) reporting and warehouse-management app. It
is not the primary store of end-user PII, but it does have meaningful security
surface: it manages credentials (AWS Secrets Manager, Redshift user/role
passwords), and it contains code that processes and guards warehouse PII
(retention enforcement, PII row checks, masking, decryption UDFs). Treat that
surface with care; you do not need to treat every file in the repo as sensitive.

- **Never** read, print, or commit secrets, `.env*` files, keys,
  `config/application.yml`, or `config/credentials.yml.enc` contents.
- When working on PII-handling code (`pii_retention.yml`, `PiiRowCheckerJob`,
  masking jobs, decryption UDFs), do not log or expose decrypted PII.
- `make brakeman` and `bundle exec bundler-audit` run as part of CI; keep them
  passing.

## Git, GitLab & Pull Requests

This project is hosted on a self-hosted GitLab instance
(`git@gitlab.login.gov:lg/identity-reporting-rails.git`), **not** GitHub. The
`gh` CLI does not work here — use the GitLab CLI (`glab`) instead.

**Only commit, push, or open MRs/PRs when explicitly asked.** This applies to
any `glab` command that creates, closes, merges, or calls the API directly
(`glab api ...`); prefer the **read-only** verbs (`issue list/view`,
`mr list/view/diff`).

### Commit & MR conventions

See `CONTRIBUTING.md` for full details.

- Write commit summaries in the imperative ("Fix bug", not "Fixed bug").
- Include the GitLab issue ID in the title when applicable
  (e.g. "LG-1234 Add the stuff to the thing").
- In the body, explain **why** the change is needed, then **how**.
- Keep merge requests small and focused on a single topic.
- A new MR uses the template at `.gitlab/merge_request_templates/Default.md`.

### GitLab tooling (`glab`)

- Authenticate once with `glab auth login --hostname gitlab.login.gov` (or via
  a `GITLAB_TOKEN` env var). `glab auth status` confirms you are logged in. A
  single `GITLAB_TOKEN` is applied to every configured host, so `glab auth status` may report a `401` for `gitlab.com` even when `gitlab.login.gov`
  works fine — that warning is safe to ignore for this repo.
- `glab mr list -R lg/identity-reporting-rails`
- `glab mr view <id> -R lg/identity-reporting-rails`

### Issues

This repo (`lg/identity-reporting-rails`) generally has **no** issues of its
own. Team Data issues — including ones that touch this app (the
`reportingRails-*` alerts/jobs) — live in `lg-teams/Team-Data/data-warehouse-ag`.
Pass that path with `-R`:

- `glab issue list -R lg-teams/Team-Data/data-warehouse-ag`
- `glab issue view <id> -R lg-teams/Team-Data/data-warehouse-ag --comments`

## Further Documentation

- `docs/local-development.md` — Local setup and running.
- `docs/jobs.md` — Jobs.
- `docs/SECURITY.md` — Security guidance.
- `docs/troubleshooting.md` — Troubleshooting local development.
