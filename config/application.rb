require_relative 'boot'

require 'rails/all'

# Require the gems listed in Gemfile, including any gems
# you've limited to :test, :development, or :production.
Bundler.require(*Rails.groups)

require_relative '../lib/identity_config'
require_relative '../lib/version_headers'

APP_NAME = 'Identity Reporting Rails'.freeze

module IdentityReportingRails
  class Application < Rails::Application
    if (log_level = ENV['LOGIN_TASK_LOG_LEVEL'])
      Identity::Hostdata.logger.level = log_level
    end

    configuration = Identity::Hostdata::ConfigReader.new(
      app_root: Rails.root,
      logger: Identity::Hostdata.logger,
    ).read_configuration(
      Rails.env, write_copy_to: Rails.root.join('tmp', 'application.yml')
    )
    IdentityConfig.build_store(configuration)

    console do
      if ENV['ALLOW_CONSOLE_DB_WRITE_ACCESS'] != 'true' &&
         IdentityConfig.store.database_readonly_username.present? &&
         IdentityConfig.store.database_readonly_password.present?
        warn <<-EOS.squish
          WARNING: Loading database a configuration with the readonly database user.
          If you wish to make changes to records in the database set
          ALLOW_CONSOLE_DB_WRITE_ACCESS to "true" in the environment
        EOS

        ActiveRecord::Base.establish_connection :read_replica
      end
    end

    config.load_defaults '7.0'
    config.active_record.belongs_to_required_by_default = false

    FileUtils.mkdir_p(Rails.root.join('log'))
    config.active_job.logger = ActiveSupport::Logger.new(Rails.root.join('log', 'workers.log'))
    config.active_job.logger.formatter = config.log_formatter

    config.time_zone = 'UTC'

    routes.default_url_options[:host] = IdentityConfig.store.domain_name
  end
end
