{ pkgs, ... }:

{
  languages = {
    ruby = {
      enable = true;
      bundler.enable = true;
      versionFile = ./.ruby-version;
      # The default-enabled solargraph LSP fails to build (nokogiri native
      # extension), breaking `devenv shell` entirely. Gems come from bundler.
      lsp.enable = false;
    };
  };

  packages = with pkgs; [
    detect-secrets
    foreman
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

    # Seed the gitignored config/application.yml so test config (redis_url
    # etc.) exists and Makefile's $(CONFIG) prerequisite is satisfied — without
    # it, `make test` first triggers a full `bin/setup` run. The app itself
    # boots fine without the file (identity-hostdata falls back to
    # application.yml.default).
    if [ ! -f config/application.yml ]; then
      cp config/application.yml.default config/application.yml
    fi
  '';

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    listen_addresses = "127.0.0.1";
  };

  services.redis.enable = true;

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
