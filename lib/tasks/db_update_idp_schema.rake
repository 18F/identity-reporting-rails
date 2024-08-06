# lib/tasks/db_update_idp_schema.rake

# frozen_string_literal: true

require 'redshift_schema_updater'

namespace :db do
  desc 'Sync the schema of tables in the idp schema with the schema defined in the YAML file'
  task update_idp_schema: :environment do
    env = ENV['RAILS_ENV']
    unless env
      puts 'RAILS_ENV environment variable is not set'
      exit 1
    end

    schema_updater = RedshiftSchemaUpdater.new('idp')
    schema_updater.update_schema_from_yaml(
      File.join(
        '/etc/login.gov/repos/identity-devops/terraform/app',
        'dms-filter-columns-transformation-rules.yml',
      ),
    )
  end
end
