# Troubleshooting Local Development

## I am receiving errors when running `$ make setup`

If this command returns errors, you may need to install the dependencies first, outside of the Makefile:

```
$ bundle install
```

## I am receiving errors when creating the development and test databases

If you receive the following error (where _whoami_ == _your username_):

`psql: error: connection to server on socket "/tmp/.s.PGSQL.5432" failed: FATAL:  database "<whoami>" does not exist`

Running the following command first, may solve the issue:

```
$ createdb `whoami`
```

## I am receiving errors when running `$ make test`

### Errors related to running specs in _parallel_

`$ make test` runs specs in _parallel_ which could potentially return errors. Running specs _serially_ may fix the problem; to run specs _serially_:

```
$ make test_serial
```

---

### Errors relating to OpenSSL versions

If you get this error during test runs:

```
     Failure/Error: JWT::JWK.import(certs_response[:keys].first).public_key
     OpenSSL::PKey::PKeyError:
       rsa#set_key= is incompatible with OpenSSL 3.0
```

This problem has happened when Ruby was built and linked against the
wrong version of OpenSSL.

The procedure we have found that fixes the problem is to rebuild Ruby,
linked against the correct version of OpenSSL, then remove and
reinstall all of your gems.

These instructions have been used successfully for environments
managed using `asdf`, `chruby` and `rbenv`. Details for each are
below.

If you are using another Ruby version manager, the section on
`ruby-build` is likely your best starting point. Please add your
experience and any useful information to this document.

### Details

- These instructions assume you're on a Mac; if not, you will have to
  work out the equivalent directions based on these.
- As of this writing, the correct Ruby version for Identity Reporting is 3.2.2.
  Use whatever the current version is.

#### Finding out where you have openssl 1.1 installed

`brew --prefix openssl@1.1`

If not present, run `brew install openssl@1.1`

#### Version manager specifics

Most version managers simply require that the correct version of Ruby
be installed, usually using `ruby-build`.

##### Rebuilding Ruby using `asdf`

`asdf` uses `ruby-build` under the covers, but supplies some
configuration of its own, so we must use `asdf` to (re-)install Ruby.

Remove the existing Ruby version, if present:

`asdf uninstall ruby 3.2.2`

And re-install, using the correct OpenSSL installation:

`RUBY_CONFIGURE_OPTS="--with-openssl-dir=$(brew --prefix openssl@1.1)" asdf install ruby 3.2.2`

##### Rebuilding Ruby using `chruby`

Use the `ruby-build` instructions; `chruby` doesn't require anything special.

##### Rebuilding Ruby using `rbenv`

Use the `ruby-build` instructions; `rbenv` doesn't require anything special, although use `~/.rbenv/versions` for the install location, not `~/.rubies`.

##### Rebuilding Ruby using `ruby-build`

Make sure ruby-build is up to date

`brew upgrade ruby-build`

And then rebuild Ruby (this assumes your Rubies are in ~/.rubies)

`RUBY_CONFIGURE_OPTS="--with-openssl-dir=$(brew --prefix openssl@1.1)" ruby-build 3.2.2 ~/.rubies/3.2.2`

#### Exiting your shell

After your Ruby is built, exit your shell and open a new one, to clear caches.

#### Removing all of your gems

`gem uninstall -aIx`

#### Reinstalling your gems

`bundle install`
