source 'https://rubygems.org'
git_source(:github) { |repo_name| "https://github.com/#{repo_name}.git" }

ruby "~> #{File.read(File.join(__dir__, '.ruby-version')).strip}"
gem 'activerecord7-redshift-adapter-pennylane',
  require: 'active_record/connection_adapters/redshift_adapter'
gem 'rails', '~> 7.2.1'
gem 'bootsnap', '~> 1.0', require: false
gem 'csv'
gem 'faker'
gem 'good_job', '~> 3.0'
gem 'identity-hostdata', github: '18F/identity-hostdata', tag: 'v4.1.0'
gem 'identity-logging', github: '18F/identity-logging', tag: 'v0.1.0'
gem 'identity_validations', github: '18F/identity-validations', tag: 'v0.7.2'
gem 'puma', '>= 5.6.7'
gem 'pg'
gem 'rack', '>= 2.2.3.1'
gem 'redacted_struct'
gem 'tzinfo-data', platforms: %i[ windows jruby ]

group :development do
  gem 'better_errors', '>= 2.5.1'
  gem 'irb'
  gem 'rack-mini-profiler', '>= 1.1.3', require: false
end

group :development, :test do
  gem 'brakeman', require: false
  gem 'bullet', '~> 7.0'
  gem 'knapsack'
  gem 'listen'
  gem 'nokogiri', '>= 1.16.5'
  gem 'pg_query', require: false
  gem 'pry-byebug'
  gem 'pry-doc', '>= 1.5.0'
  gem 'pry-rails'
  gem 'psych'
  gem 'rexml', '>= 3.3.3'
  gem 'rspec', '~> 3.13.0'
  gem 'rspec-support', '~> 3.13.1'
  gem 'rspec-rails', '~> 6.0'
  gem 'rubocop', '~> 1.55.1', require: false
  gem 'rubocop-performance', '~> 1.19.0', require: false
  gem 'rubocop-rails', '>= 2.5.2', require: false
  gem 'rubocop-rspec', require: false
end

group :test do
  gem 'bundler-audit', require: false
  gem 'simplecov', '~> 0.22.0', require: false
  gem 'simplecov-cobertura'
  gem 'simplecov_json_formatter'
  gem 'factory_bot_rails', '>= 6.2.0'
  gem 'rack-test', '>= 1.1.0'
  gem 'rspec-retry'
  gem 'rspec_junit_formatter'
  gem 'shoulda-matchers', '~> 4.0', require: false
  gem 'webmock'
  gem 'zonebie'
end
