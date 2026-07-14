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

    # foreman (used by `make run`) is intentionally NOT in the Gemfile — per
    # foreman's own guidance it should live outside the app's bundle. Installing
    # it via the nix `foreman` package pulls in a mismatched Ruby, which breaks
    # the `bundle exec` child processes it spawns (rackup/good_job). Instead,
    # install it into the app's Ruby so parent and children share one runtime.
    "ruby:install_foreman" = {
      exec = "gem install foreman --conservative";
      status = "gem list -i foreman";
      before = [ "devenv:enterShell" ];
      after = [ "ruby:install_gems" ];
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
