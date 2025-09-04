class CreateFcmsUnextractedEventsAndEventsTable < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up { execute 'CREATE SCHEMA IF NOT EXISTS fcms' }
      dir.down { execute 'DROP SCHEMA IF EXISTS fcms' }
    end

    tables = ['unextracted_events', 'encrypted_events' 'events']
    message_data_type = connection.adapter_name.downcase.include?('redshift') ? 'SUPER' : 'JSONB'

    tables.each do |table|
      execute <<-SQL
        CREATE TABLE IF NOT EXISTS fcms.#{table} (
          message #{message_data_type},
          import_timestamp TIMESTAMP
        );
      SQL
    end

    execute <<-SQL
      ALTER TABLE fcms.encrypted_events
        ADD COLUMN processed_timestamp TIMESTAMP NULL DEFAULT '';
    SQL

    execute <<-SQL
      ALTER TABLE fcms.events
        ADD COLUMN jti VARCHAR(256) NOT NULL DEFAULT '';
    SQL

    execute <<-SQL
      ALTER TABLE fcms.events ADD PRIMARY KEY (jti);
    SQL

  end
end
