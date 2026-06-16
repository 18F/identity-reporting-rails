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

## Setup & Common Commands

All common tasks are exposed through the `Makefile`. Prefer these over raw
commands.

- `make setup` — Full setup (config files, gems, brew packages, databases).
- `make fast_setup` — Abbreviated setup that skips linking some files.
- `make run` — Start the development server (runs the `Procfile`: web + worker).
- `make test` — Run the full RSpec suite (`RAILS_ENV=test`).
- `make fast_test` — RSpec without accessibility specs.
- `make lint` — Run all linters (rubocop, brakeman, lockfile/readme/migration checks).
- `make lintfix` — Auto-fix rubocop + normalize YAML.
- `make brakeman` — Security scan.
- `make check` — Runs `lint` then `test`.
- `make update` — `bundle install` + `rails db:migrate` (after a git pull).

Run `make help` to list all available targets.

## Testing

- Framework: **RSpec** (`rspec-rails`), with FactoryBot, Shoulda Matchers,
  WebMock, and SimpleCov.
- Specs live in `spec/`. Factories live in `spec/factories/`.
- Prefer running a single spec file or example during development rather than
  the full suite:
  - `bundle exec rspec spec/jobs/data_freshness_job_spec.rb`
  - `bundle exec rspec spec/path/to/spec.rb:LINE`
- Always run the relevant specs after making code changes.

## Linting & Conventions

- Ruby is linted with **RuboCop** (`rubocop-rails`, `rubocop-rspec`,
  `rubocop-performance`). Config in `.rubocop.yml` (`DisabledByDefault: true` —
  only explicitly enabled cops run).
- Target Ruby 3.4, Target Rails 8.0.
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
- `config/` — Rails config. Key files: `identity_config.rb` (via `lib/`),
  `redshift_config.yaml`, `pii_retention.yml`, `redshift_system_tables.yml`.

## Databases

This app connects to **multiple databases** (see `config/database.yml`):

- `primary` (+ `read_replica`) — PostgreSQL application DB.
- `worker_jobs` — PostgreSQL DB for GoodJob.
- `data_warehouse` — **Amazon Redshift** in production; PostgreSQL locally/test.
  Uses `activerecord-redshift-adapter`. `pg` is pinned to 1.5.9 for Redshift
  compatibility (< pgsql 10).

Migrations are separated by database:

- `db/primary_migrate/`
- `db/worker_jobs_migrate/`
- `db/data_warehouse_migrate/` (+ `db/data_warehouse_test_migrate/` for tests)

When adding a migration, place it in the correct directory and confirm the
target database. Migration linting runs via `scripts/migration_check`.

## Security & Sensitive Data

This is a federal (GSA / Login.gov) project handling PII and credentials.

- **Never** read, print, or commit secrets, `.env*` files, keys, or
  `config/credentials.yml.enc` contents.
- Be cautious with PII-related code (`pii_retention.yml`, masking jobs,
  decryption UDFs). Do not log or expose decrypted PII.
- `make brakeman` and `bundle exec bundler-audit` run as part of CI; keep them
  passing.

## Git & Pull Requests

See `CONTRIBUTING.md` for full details.

- Write commit summaries in the imperative ("Fix bug", not "Fixed bug").
- Include the Jira ticket ID in the title when applicable
  (e.g. "LG-1234 Add the stuff to the thing").
- In the body, explain **why** the change is needed, then **how**.
- Keep pull requests small and focused on a single topic.
- Only commit, push, or open PRs when explicitly asked.

## Further Documentation

- `docs/local-development.md` — Local setup and running.
- `docs/backend.md` — Back-end architecture.
- `docs/SECURITY.md` — Security guidance.
- `docs/troubleshooting.md` — Troubleshooting local development.
