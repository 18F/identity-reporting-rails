# Local Development

## Installing on your local machine

This installation method is meant for those who are familiar with setting up local development environments on their machines. If you encounter errors, see the [Troubleshooting](#troubleshooting) section at the bottom of this README.

We recommend using [Homebrew](https://brew.sh/), [rbenv](https://github.com/rbenv/rbenv) or other version management tooling to install the below dependencies; while we don't anticipate changing these frequently, this will ensure that you will be able to easily switch to different versions as needed.

### Dependencies

Installing the packages differs slightly if you're on a macOS or a different OS.

If using macOS:

1. Install [rbenv](https://github.com/rbenv/rbenv) (lets you install and switch between different versions of Ruby)
1. Install Ruby. Choose the version [in the `.ruby-version` file](../.ruby-version)
1. Skip to the [set up local environment section](#set-up-local-environment). Your other dependencies will be installed in that step.

If not using macOS:

1. To start, make sure you have the following dependencies installed and a working development environment:

    - [rbenv](https://github.com/rbenv/rbenv) (lets you install and switch between different versions of Ruby)
    - Ruby. Choose the version [in the `.ruby-version` file](../.ruby-version)
    - [PostgreSQL](http://www.postgresql.org/download/)
    - [Redis 7+](http://redis.io/)

2. You will need to install openssl version 1.1:

    - Run `brew install openssl@1.1`

3. Test that you have Postgres and Redis running.

4. Continue to the [set up local environment section](#set-up-local-environment).

### Set up local environment

1. Run the following command to set up your local environment:

    ```
    $ make setup
    ```

    This command copies sample configuration files, installs required gems and brew packages (if using macOS), and sets up the database. Check out our [Makefile commands](../Makefile) to learn more about what this command does.

1. Now that you have you have everything installed, you can run the following command to start your local server:

    ```
    $ make run
    ```

    You should now be able to go to open up your favorite browser, go to `localhost:3000` and see your local development environment running.

### Running tests locally

  Identity Reporting uses the following tools for our testing:

  - [RSpec](https://relishapp.com/rspec/rspec-core/docs/command-line)

  To run our full test suite locally, use the following command:

  ```
  $ make test
  ```

  Use the following command to run a subset of our test suite, excluding slower tests:

  ```
  $ make fast_test
  ```

Check out our [Makefile commands](../Makefile) and learn more about how you can customize this command to run specific tests using rspec

## Linting

Run `make lint` to look for errors; `make lintfix` can repair some linting errors.


## Running jobs

We run background jobs / workers with ActiveJob and GoodJob. You shouldn't normally have to start it manually because `make run` runs [the `Procfile`](../Procfile), which handles it. The manual command is: `bundle exec good_job start`

