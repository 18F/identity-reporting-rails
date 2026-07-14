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

    # The app won't boot without the gitignored config/application.yml; seed it
    # from the default (as bin/setup does) so tests run without manual setup.
    if [ ! -f config/application.yml ]; then
      cp config/application.yml.default config/application.yml
    fi

    # config/database.yml reads POSTGRES_USER in test; point it at the
    # cluster's only role (see services.postgres.initdbArgs).
    export POSTGRES_USER="''${POSTGRES_USER:-postgres}"

    # libpq clients (psql, pg_isready) otherwise connect as the OS user, which
    # has no role here and spams the postgres log with FATALs (mostly from the
    # process-compose readiness probe).
    export PGUSER="''${PGUSER:-postgres}"
  '';

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    listen_addresses = "127.0.0.1";
    # Bootstrap with a `postgres` superuser (not the OS user): specs like
    # RedshiftUnexpectedUserDetectionJob assume the local user is `postgres`.
    initdbArgs = [
      "--locale=C"
      "--encoding=UTF8"
      "--username=postgres"
    ];
    # Suppress devenv's default `createDatabase`: it runs
    # `CREATE DATABASE "$USER"` as `$USER`, which fails since only the
    # `postgres` role exists. initdb already creates the `postgres` database.
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
