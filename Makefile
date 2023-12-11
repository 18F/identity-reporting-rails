# Makefile for building and running the project.
# The purpose of this Makefile is to avoid developers having to remember
# project-specific commands for building, running, etc.  Recipes longer
# than one or two lines should live in script files of their own in the
# bin/ directory.

CONFIG = config/application.yml
HOST ?= localhost
PORT ?= 3000
GZIP_COMMAND ?= gzip
ARTIFACT_DESTINATION_FILE ?= ./tmp/idp.tar.gz

.PHONY: \
	brakeman \
	check \
	clobber_db \
	clobber_logs \
	fast_setup \
	fast_test \
	help \
	lint \
	lint_lockfiles \
	lint_yaml \
	lintfix \
	normalize_yaml \
	run \
	tidy \
	update \
	urn \
	README.md \
	setup \
	test \

help: ## Show this help
	@echo "--- Help ---"
	@ruby lib/makefile_help_parser.rb

all: check

setup $(CONFIG): config/application.yml.default ## Runs setup scripts (updates packages, dependencies, databases, etc)
	bin/setup

fast_setup: ## Abbreviated setup script that skips linking some files
	bin/fast_setup

check: lint test ## Runs lint tests and spec tests

lint: ## Runs all lint tests
	# Ruby
	@echo "--- rubocop ---"
ifdef JUNIT_OUTPUT
	bundle exec rubocop --parallel --format progress --format junit --out rubocop.xml --display-only-failed
else
	bundle exec rubocop --parallel
endif
	@echo "--- brakeman ---"
	make brakeman
	@echo "--- bundler-audit ---"
	bundle exec bundler-audit check --update
	# Other
	@echo "--- lint yaml ---"
	make lint_yaml
	@echo "--- lint lockfiles ---"
	make lint_lockfiles
	@echo "--- README.md ---"
	make lint_readme
	@echo "--- lint migrations ---"
	make lint_migrations

lint_yaml: normalize_yaml ## Lints YAML files
	(! git diff --name-only | grep "^config/.*\.yml$$") || (echo "Error: Run 'make normalize_yaml' to normalize YAML"; exit 1)

lint_migrations:
	scripts/migration_check

lint_gemfile_lock: Gemfile Gemfile.lock ## Lints the Gemfile and its lockfile
	@bundle check
	@git diff-index --quiet HEAD Gemfile.lock || (echo "Error: There are uncommitted changes after running 'bundle install'"; exit 1)

lint_lockfiles: lint_gemfile_lock ## Lints to ensure lockfiles are in sync

lint_readme: README.md ## Lints README.md
	(! git diff --name-only | grep "^README.md$$") || (echo "Error: Run 'make README.md' to regenerate the README.md"; exit 1)

lintfix: ## Try to automatically fix any Ruby, ERB, JavaScript, YAML, or CSS lint errors
	@echo "--- rubocop fix ---"
	bundle exec rubocop -a
	@echo "--- normalize yaml ---"
	make normalize_yaml

brakeman: ## Runs brakeman code security check
	(bundle exec brakeman) || (echo "Error: update code as needed to remove security issues. For known exceptions already in brakeman.ignore, use brakeman to interactively update exceptions."; exit 1)

test: export RAILS_ENV := test
test: $(CONFIG) ## Runs RSpec
	bundle exec rspec

test_serial: export RAILS_ENV := test
test_serial: $(CONFIG) ## Runs RSpec serially
	bundle exec rake spec

fast_test: export RAILS_ENV := test
fast_test: ## Abbreviated test run, runs RSpec tests without accessibility specs
	bundle exec rspec --exclude-pattern "**/features/accessibility/*_spec.rb"

tmp/$(HOST)-$(PORT).key tmp/$(HOST)-$(PORT).crt: ## Self-signed cert for local HTTPS development
	mkdir -p tmp
	openssl req \
		-newkey rsa:2048 \
		-x509 \
		-sha256 \
		-nodes \
		-days 365 \
		-subj "/C=US/ST=District of Columbia/L=Washington/O=GSA/OU=Login.gov/CN=$(HOST):$(PORT)"  \
		-keyout tmp/$(HOST)-$(PORT).key \
		-out tmp/$(HOST)-$(PORT).crt

run: ## Runs the development server
	foreman start -p $(PORT)

urn:
	@echo "⚱️"
	make run

run-https: tmp/$(HOST)-$(PORT).key tmp/$(HOST)-$(PORT).crt ## Runs the development server with HTTPS
	HTTPS=on FOREMAN_HOST="ssl://$(HOST):$(PORT)?key=tmp/$(HOST)-$(PORT).key&cert=tmp/$(HOST)-$(PORT).crt" foreman start -p $(PORT)

normalize_yaml: ## Normalizes YAML files (alphabetizes keys, fixes line length, smart quotes)
	normalize-yaml .rubocop.yml --disable-sort-keys --disable-smart-punctuation

update: ## Update dependencies, useful after a git pull
	bundle install
	bundle exec rails db:migrate

README.md: docs/ ## Generates README.md based on the contents of the docs directory
	bundle exec ruby scripts/generate_readme.rb --docs-dir $< > $@

clobber_db: ## Resets the database for make setup
	bin/rake db:create
	bin/rake db:environment:set
	bin/rake db:reset
	bin/rake db:environment:set
	bin/rake dev:prime

clobber_logs: ## Purges logs and tmp/
	rm -f log/*
	rm -rf tmp/cache/*

tidy: clobber_logs ## Remove logs, and unused gems, but leave DB alone
	bundle clean
