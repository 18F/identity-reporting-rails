{ pkgs, ... }:

{
  languages = {
    ruby = {
      enable = true;
      bundler.enable = true;
      versionFile = ./.ruby-version;
    };
  };

  packages = with pkgs; [
    detect-secrets
    git
    glab
    gnumake
    libyaml
  ];

  tasks = {
    "ruby:install_gems" = {
      exec = "bundle install";
      status = "bundle check";
      before = [ "devenv:enterShell" ];
    };
  };

  enterShell = ''
    # Conflicts with bundler
    export RUBYLIB=

    # The app refuses to boot (including under RSpec / rails tasks) without
    # config/application.yml. It is gitignored and seeded from the checked-in
    # default, so create it on first shell entry if it is missing. This mirrors
    # what `bin/setup` does and lets `make test` work without a manual copy.
    if [ ! -f config/application.yml ]; then
      cp config/application.yml.default config/application.yml
    fi

    # The Postgres cluster is bootstrapped with a `postgres` superuser (see
    # services.postgres.initdbArgs) rather than one named after the OS user.
    # The test database config (config/database.yml) reads POSTGRES_USER, so
    # point it at `postgres` when it isn't otherwise set. This also keeps
    # RedshiftUnexpectedUserDetectionJob specs green, since they assume the
    # local Redshift/Postgres user is `postgres`.
    export POSTGRES_USER="''${POSTGRES_USER:-postgres}"

    # libpq client tools (psql, pg_isready) default the connecting role to the
    # OS username when PGUSER is unset. Since the cluster only has a `postgres`
    # role, that produces recurring `FATAL: role "<os-user>" does not exist`
    # noise in the postgres log (most visibly from the process-compose
    # readiness probe, which runs `psql ... template1` every 10s). Default
    # PGUSER to `postgres` so those clients connect as an existing role.
    export PGUSER="''${PGUSER:-postgres}"
  '';

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    listen_addresses = "127.0.0.1";
    # Bootstrap the cluster with a `postgres` superuser so specs that assume the
    # default Redshift/Postgres user is `postgres` (e.g.
    # RedshiftUnexpectedUserDetectionJob) pass regardless of the OS user name.
    initdbArgs = [
      "--locale=C"
      "--encoding=UTF8"
      "--username=postgres"
    ];
    # Suppress devenv's default `createDatabase`, which runs
    # `CREATE DATABASE "$USER"` while connecting as `$USER` and fails because
    # initdb only created the `postgres` role. initdb already creates the
    # `postgres` role and database, so no `initialDatabases` entry is needed.
    createDatabase = false;
  };

  services.redis = {
    enable = true;
    bind = "127.0.0.1";
    port = 6379;
  };

  git-hooks.hooks = {
    detect-secrets = {
      enable = true;
      name = "detect-secrets";
      description = "Detects high entropy strings that are likely to be passwords.";
      entry = "detect-secrets-hook";
      language = "python";
      args = [
        "--baseline"
        ".secrets.baseline"
      ];
    };
  };
}
