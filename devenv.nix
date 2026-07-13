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

    # The Postgres cluster is bootstrapped with a `postgres` superuser (see
    # services.postgres.initdbArgs) rather than one named after the OS user.
    # The test database config (config/database.yml) reads POSTGRES_USER, so
    # point it at `postgres` when it isn't otherwise set. This also keeps
    # RedshiftUnexpectedUserDetectionJob specs green, since they assume the
    # local Redshift/Postgres user is `postgres`.
    export POSTGRES_USER="''${POSTGRES_USER:-postgres}"
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
    # Explicitly create the bootstrap database owned by `postgres`. Without
    # this, devenv's default `createDatabase` runs `CREATE DATABASE "$USER"`
    # while connecting as `$USER`, which fails because initdb only created the
    # `postgres` role.
    createDatabase = false;
    initialDatabases = [ { name = "postgres"; user = "postgres"; } ];
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
