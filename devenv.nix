{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  # https://devenv.sh/packages/

  languages = {
    ruby = {
      enable = true;
      bundler.enable = true;
      versionFile = ./.ruby-version;
    };
  };

  packages = with pkgs; [
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
  '';

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    listen_addresses = "127.0.0.1";
  };
}
