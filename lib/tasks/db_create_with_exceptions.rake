# lib/tasks/db_create_with_exceptions.rake

# frozen_string_literal: true

require 'active_record'
require 'active_support/configuration_file'
require 'active_support/deprecation'

databases = ActiveRecord::Tasks::DatabaseTasks.setup_initial_database_yaml

namespace :db do
  # Define task to create all databases except the specified ones
  desc 'Create all databases except the specified one in production'
  task create_with_exceptions: :environment do
    # Ensure the RAILS_ENV environment variable is set
    env = ENV['RAILS_ENV']
    unless env
      puts 'RAILS_ENV environment variable is not set'
      exit 1
    end

    ActiveRecord::Tasks::DatabaseTasks.for_each(databases) do |name|
      db_config = ActiveRecord::Base.configurations.configs_for(env_name: env, name: name)
      if ['data_warehouse', 'analytics_zetl'].include? name
        $stdout.puts "Skipping creation of database: #{name} => #{db_config.database}"
        next
      end
      ActiveRecord::Tasks::DatabaseTasks.create(db_config)
    end
  end
end
