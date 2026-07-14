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
  '';

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    listen_addresses = "127.0.0.1";
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
