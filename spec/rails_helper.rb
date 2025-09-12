# This file is copied to spec/ when you run 'rails generate rspec:install'

if ENV['COVERAGE']
  require './spec/simplecov_helper'
  SimplecovHelper.start
end

ENV['RAILS_ENV'] = 'test'

require File.expand_path('../../config/environment', __FILE__)
require 'rails/test_unit/railtie'
require 'rspec/rails'
require 'spec_helper'
require 'factory_bot'

# Checks for pending migrations before tests are run.
# If you are not using ActiveRecord, you can remove this line.

# In order to segregate migrations for testing and production and ensure their
# successful execution, we need to comment out the line below.
# ActiveRecord::Migration.maintain_test_schema!

# Requires supporting ruby files with custom matchers and macros, etc, in
# spec/support/ and its subdirectories. Files matching `spec/**/*_spec.rb` are
# run as spec files by default. This means that files in spec/support that end
# in _spec.rb will both be required and run as specs, causing the specs to be
# run twice. It is recommended that you do not name files matching this glob to
# end with _spec.rb. You can configure this pattern with the --pattern
# option on the command line or in ~/.rspec, .rspec or `.rspec-local`.
Dir[Rails.root.join('spec', 'support', '**', '*.rb')].sort.each { |f| require f }

RSpec.configure do |config|
  config.use_transactional_fixtures = true
  config.infer_spec_type_from_file_location!

  config.include ActiveSupport::Testing::TimeHelpers

  config.before(:suite) do
    Rails.application.load_seed
  end

  config.before(:each) do
    I18n.locale = :en
  end

  config.before(:each, type: :controller) do
    @request.host = Identity::Hostdata.config.domain_name
  end

  config.before(:each) do
    Rails.cache.clear
  end

  config.before(:each) do
     REDIS_POOL.with { |client| client.flushdb } if Identity::Hostdata.config
  end

  config.around(:each, freeze_time: true) do |example|
    freeze_time { example.run }
  end
end
