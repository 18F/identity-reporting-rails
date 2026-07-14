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

See also: `docs/jobs.md` (job/sync architecture),
`docs/local-development.md` (local setup).

## Dev Environment (devenv / Nix)

The canonical development environment is managed by
[devenv](https://devenv.sh/) (Nix-based) and auto-activated by
[direnv](https://direnv.net/):

- `.envrc` runs `use devenv`, so entering the repo directory activates the
  environment automatically (`direnv allow` on first use). Without direnv, run
  `devenv shell` manually.
- `devenv.nix` provisions Ruby (from `.ruby-version`), Bundler, PostgreSQL 16,
  Redis, and CLI tools (`glab`, `gnumake`, `detect-secrets`, `foreman`). A
  `bundle install` task runs on shell entry.
- A **`detect-secrets` pre-commit git hook** is configured in `devenv.nix`. It
  blocks commits containing high-entropy strings (likely secrets), checked
  against `.secrets.baseline`. Commits made by agents will run this hook. Run
  `git commit` from within `devenv shell` — outside it the hook fails with
  "Executable `detect-secrets-hook` not found".
- PostgreSQL and Redis are provided by devenv as services, not system installs.
  Start them with `devenv up` (see Running services).

A manual Homebrew + rbenv path is also documented in
`docs/local-development.md` (uses the `Brewfile` and `make setup`). Prefer the
devenv path unless you have a reason not to.

### Running services (PostgreSQL + Redis)

The Postgres and Redis services declared in `devenv.nix` do **not** start on
shell entry — start them with `devenv up`. Both are required for the test
suite; `bin/setup` needs only Postgres. Both listen on their default local
ports (5432, 6379), so a system-installed Postgres or Redis must not be
running at the same time — the test suite connects to (and flushes Redis on)
whatever answers on those ports.

- If `devenv` is "command not found" (e.g. a non-interactive/sandbox shell that
  didn't source direnv), run `export PATH="$HOME/.nix-profile/bin:$PATH"`, then
  run everything through `devenv shell -- <cmd>` so Ruby/Bundler resolve.
- `devenv up` uses a TUI that needs a real terminal; when headless it fails
  with `open /dev/tty: no such device` — use `devenv up -d` (detached) instead.
  `devenv processes stop` stops the services.
- Service recovery (stale `postmaster.pid` after a crash,
  `Redis::CannotConnectError`, stale native gems after a `devenv.lock` update):
  see the Devenv section of `docs/troubleshooting.md`.
- Service data lives under `.devenv/state/`, created on first start.

### Running the test suite from a clean checkout

`devenv.nix`'s `enterShell` auto-creates `config/application.yml`, so no manual
config is needed. The remaining steps:

```sh
export PATH="$HOME/.nix-profile/bin:$PATH"   # only if devenv isn't on PATH
devenv up -d                                  # start Postgres + Redis (detached)
devenv shell -- bash -c 'RAILS_ENV=test bin/rails db:prepare && make test'
```

`devenv up -d` returns before the services are ready; if `db:prepare` gets
"connection refused" right after a first-ever start, wait a few seconds and
retry (it is idempotent). Each one-shot `devenv shell -- <cmd>` pays ~20–30s of
environment evaluation — for repeated commands, chain them or use one
persistent `devenv shell` session.

Redis is required for the **whole** suite: `spec/rails_helper.rb` flushes it
in a `before(:each)` hook, so without it every spec fails with
`Redis::CannotConnectError`.

Leave the devenv services **running** after a test run — do not stop them (or
ask whether to) unless the user explicitly requests it. The services bind fixed
local ports, so only one checkout/worktree can run them at a time — a second
checkout's tests would silently use the first one's services and databases.

### Running the dev server

`make run` runs the `Procfile` via `foreman` (`web` = Puma on port 3000,
`worker` = GoodJob). As with the test suite, start Postgres + Redis first and
run it inside the devenv shell:

```sh
export PATH="$HOME/.nix-profile/bin:$PATH"   # only if devenv isn't on PATH
devenv up -d                                  # start Postgres + Redis (detached)
devenv shell -- bash -c 'bin/rails db:prepare && make run'
```

- `foreman` is provisioned by `devenv.nix` (intentionally not in the Gemfile),
  so `make run` only works inside the devenv shell.
- The web process needs `tmp/pids/` to exist; it is kept in the repo via
  `tmp/pids/.keep`, and `bin/setup` clears/recreates tmp. If Puma dies at boot
  with `No such file or directory @ rb_sysopen - tmp/pids/server.pid`, run
  `mkdir -p tmp/pids`.
- Verify it booted by looking for `Listening on http://127.0.0.1:3000` in the
  output, or `curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:3000/`
  (expect `200`). The one-off `HTTP parse error ... non-SSL Puma?` line is a
  benign health-probe artifact, not a boot failure.
- `make run` invokes `foreman start -p 3000`, which runs the `Procfile`'s `web`
  entry (`bundle exec rackup config.ru`) — so the running web process is
  `rackup`/`puma`, not a literal `make`/`rails server`.
- Headless/non-interactive runs: `make run` streams foreman output and does not
  return, so launch it in the background (redirect to a log file) rather than
  blocking the shell. To stop it, terminate the `foreman`/`puma`/`good_job`
  process tree (SIGTERM). Leave the devenv Postgres/Redis services running (see
  the note above) unless explicitly asked to stop them.
- To confirm the server actually stopped, check for a listener rather than
  `curl`: `ps -eo pid,comm | grep -E 'foreman|puma|good_job'` (expect none) or
  `ss -ltnp | grep :3000` (expect nothing on 3000). `curl` can be misleading
  here — after shutdown it may return `500` (a cached/proxy response) instead of
  a connection-refused error, so a non-`000` status does **not** mean the server
  is still up.

## Setup & Common Commands

Prefer `Makefile` targets over raw commands. `make help` lists all targets;
the everyday drivers:

- `make setup` — `bin/setup`: packages, dependencies, databases, config files.
- `make run` — Start the dev server (`Procfile`: `web` = `rackup config.ru`,
  `worker` = `good_job start`).
- `make test` — Full RSpec suite (`RAILS_ENV=test`, `bundle exec rspec`).
- `make fast_test` — RSpec without the accessibility specs.
- `make lint` / `make lintfix` — Run linters / auto-fix rubocop + normalize YAML.
- **Run `make check` (lint then test) before pushing** — the pre-push gate.

Scheduled jobs are **GoodJob cron**, defined in
`config/initializers/job_configurations.rb` (not OS cron). Scheduling is skipped
when running in a Rails console.

## Testing

RSpec (with FactoryBot, Shoulda Matchers, WebMock). Specs in `spec/`, factories
in `spec/factories/`.

- `make test` runs specs **serially**; parallelism (Knapsack) exists only
  across CI nodes.
- Prefer a single spec over the full suite while developing:
  `bundle exec rspec spec/path/to/spec.rb:LINE`. Always run the relevant specs
  after changes.
- Environment setup: see
  [Running the test suite from a clean checkout](#running-the-test-suite-from-a-clean-checkout).

## Linting & Conventions

- Ruby is linted with **RuboCop** (`rubocop-rails`, `rubocop-rspec`,
  `rubocop-performance`). Config in `.rubocop.yml` (`DisabledByDefault: true` —
  only explicitly enabled cops run).
- Target Ruby 3.4, Target Rails 8.1.
- Run `make lint` before finishing a change; use `make lintfix` to auto-correct.
- In the sandbox, `make lint` may die at the RuboCop step with
  `Parallel::DeadWorker` (exit 2). That is RuboCop's parallel mode crashing a
  forked worker — a sandbox/resource artifact, **not** a lint offense. Rerun
  RuboCop serially to get the real result: `bundle exec rubocop --no-parallel`.
  Because `make lint` bails at RuboCop, run the remaining sub-checks
  individually: `make brakeman`, `make lint_lockfiles`, `make lint_readme`,
  `make lint_migrations`.
- `make lint_lockfiles` can also fail spuriously in the sandbox with "There are
  uncommitted changes after running 'bundle install'". This is the same
  bind-mount phantom-change artifact as the symlink issue: `git diff-index`
  reports `Gemfile.lock` as modified while `git diff Gemfile.lock` shows no
  content change. Confirm with `git diff Gemfile.lock` (empty = clean); it
  passes on the host.
- The `README.md` is **auto-generated** from `docs/`. Do not edit it directly —
  run `make README.md` to regenerate. CI fails if it is out of sync.

## Databases

This app connects to **multiple databases** (see `config/database.yml`):

- `primary` (+ `read_replica`) — PostgreSQL application DB.
- `worker_jobs` — PostgreSQL DB for GoodJob.
- `data_warehouse` — **Amazon Redshift** in production; PostgreSQL locally/test.
  Uses `activerecord-redshift-adapter`. `pg` is pinned to 1.5.9 for Redshift
  compatibility (< pgsql 10).

Each database has its own `ActiveRecord` base class — inherit from the right one
or a model writes to the wrong database:

- `ApplicationRecord` — primary
- `DataWarehouseApplicationRecord` — Redshift / data warehouse
- `WorkerJobApplicationRecord` — GoodJob (`worker_jobs`)

Migrations are separated by database:

- `db/primary_migrate/` — configured as the primary DB's `migrations_paths` in
  `config/database.yml`, but the directory does not exist yet (no primary
  migrations; `db/schema.rb` is version 0). Create it when adding the first one.
- `db/worker_jobs_migrate/`
- `db/data_warehouse_migrate/` (+ `db/data_warehouse_test_migrate/` for tests)

When adding a migration, place it in the correct directory and confirm the
target database. Migration linting runs via `scripts/migration_check`.

Gotchas:

- **The Rails console connects as the read-only DB user by default** — writes
  fail. Set `ALLOW_CONSOLE_DB_WRITE_ACCESS=true` in the environment to use the
  writable connection (`config/application.rb`).
- **Report queries run under a SQL `statement_timeout`** — long-running report
  queries abort rather than hang. Reports use
  `Reports::BaseReport.transaction_with_timeout` (timeout =
  `IdentityConfig.store.report_timeout`).

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

See `docs/SECURITY.md` for full guidance.

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
