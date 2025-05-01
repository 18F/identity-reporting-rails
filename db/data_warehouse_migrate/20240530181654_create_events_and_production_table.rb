class CreateEventsAndProductionTable < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up { execute 'CREATE SCHEMA IF NOT EXISTS logs' }
      dir.down { execute 'DROP SCHEMA IF EXISTS logs' }
    end

    tables = ['unextracted_events', 'unextracted_production', 'events', 'production']
    message_data_type = connection.adapter_name.downcase.include?('redshift') ? 'SUPER' : 'JSONB'

    tables.each do |table|
      execute <<-SQL
        CREATE TABLE IF NOT EXISTS logs.#{table} (
          message #{message_data_type},
          cloudwatch_timestamp TIMESTAMP
        );
      SQL
    end
  end
end
