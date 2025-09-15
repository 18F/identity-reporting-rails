class CreateFcmsUnextractedEventsAndEventsTable < ActiveRecord::Migration[7.2]
  def change
    using_redshift_adapter = connection.adapter_name.downcase.include?('redshift')

    reversible do |dir|
      dir.up do
        execute 'CREATE SCHEMA IF NOT EXISTS fcms'

        execute <<-SQL
          CREATE TABLE IF NOT EXISTS fcms.encrypted_events (
            event_key VARCHAR(256),
            message TEXT,
            event_timestamp TIMESTAMP,
            processed_timestamp TIMESTAMP
          );
        SQL

        execute <<-"SQL"
          CREATE TABLE IF NOT EXISTS fcms.events (
            event_key VARCHAR(256) PRIMARY KEY,
            message #{using_redshift_adapter ? 'SUPER' : 'JSONB'},
            event_timestamp TIMESTAMP
          );
        SQL
      end

      dir.down do
        execute 'DROP TABLE IF EXISTS fcms.unextracted_events'
        execute 'DROP TABLE IF EXISTS fcms.encrypted_events'
        execute 'DROP TABLE IF EXISTS fcms.events'
        execute 'DROP SCHEMA IF EXISTS fcms'
      end
    end
  end
end
