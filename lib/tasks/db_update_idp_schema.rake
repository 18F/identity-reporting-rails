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
    local_etl_path = IdentityConfig::REPO_PATHS[:etl_config]

    file_path = IdentityConfig.store.data_warehouse_fcms_enabled ?
        "#{local_etl_path}/modules/data_warehouse_etl_config/config/db-transformation-rules-fcms.yml" : # rubocop:disable Layout/LineLength
        "#{local_etl_path}/modules/data_warehouse_etl_config/config/db-transformation-rules.yml"
    schema_updater.update_schema_from_yaml(file_path)
  end
end
