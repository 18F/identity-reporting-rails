class RecreateLogsSchemaTables < ActiveRecord::Migration[7.2]
  def up
    if connection.adapter_name.downcase.include?('redshift')
      execute 'CREATE SCHEMA IF NOT EXISTS logs'

      message_data_type = connection.adapter_name.downcase == 'redshift' ? 'SUPER' : 'JSONB'

      #drop table if exists and create events table
      execute 'DROP TABLE IF EXISTS logs.events'
      execute <<-SQL
        CREATE TABLE logs.events (
          message #{message_data_type},
          cloudwatch_timestamp TIMESTAMP,
          id VARCHAR(256) PRIMARY KEY,
          name VARCHAR(256),
          time TIMESTAMP,
          visitor_id VARCHAR(256),
          visit_id VARCHAR(256),
          log_filename VARCHAR(256),
          new_events BOOLEAN,
          path VARCHAR(12000),
          user_id VARCHAR(256),
          locale VARCHAR(256),
          user_ip VARCHAR(256),
          hostname VARCHAR(256),
          pid INTEGER,
          service_provider VARCHAR(256),
          trace_id VARCHAR(256),
          git_sha VARCHAR(256),
          git_branch VARCHAR(256),
          user_agent VARCHAR(12000),
          browser_name VARCHAR(256),
          browser_version VARCHAR(256),
          browser_platform_name VARCHAR(256),
          browser_platform_version VARCHAR(256),
          browser_device_name VARCHAR(256),
          browser_mobile BOOLEAN,
          browser_bot BOOLEAN,
          success BOOLEAN
        );
      SQL

      #drop table if exists and create production table
      execute 'DROP TABLE IF EXISTS logs.production'
      execute <<-SQL
        CREATE TABLE logs.production (
          message #{message_data_type},
          cloudwatch_timestamp TIMESTAMP,
          uuid VARCHAR(256) PRIMARY KEY,
          method VARCHAR(256),
          path VARCHAR(12000),
          format VARCHAR(256),
          controller VARCHAR(256),
          action VARCHAR(256),
          status INTEGER,
          duration FLOAT,
          git_sha VARCHAR(256),
          git_branch VARCHAR(256),
          timestamp TIMESTAMP,
          pid INTEGER,
          user_agent VARCHAR(12000),
          ip VARCHAR(256),
          host VARCHAR(256),
          trace_id VARCHAR(256)
        );
      SQL
    else
      Rails.logger.warn("unsupported adapter: #{connection.adapter_name}.This is only supported for redshift")
    end
  end
  def down
    if connection.adapter_name.downcase.include?('redshift')
      execute 'DROP TABLE IF EXISTS logs.events'
      execute 'DROP TABLE IF EXISTS logs.production'
    else
      Rails.logger.warn("unsupported adapter: #{connection.adapter_name}.This is only supported for redshift")
    end
  end
end

