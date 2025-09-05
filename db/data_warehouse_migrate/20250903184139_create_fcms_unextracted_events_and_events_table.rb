class CreateFcmsUnextractedEventsAndEventsTable < ActiveRecord::Migration[7.2]
  def change
    using_redshift_adapter = connection.adapter_name.downcase.include?('redshift')

    reversible do |dir|
      dir.up { execute 'CREATE SCHEMA IF NOT EXISTS fcms' }
      dir.down { execute 'DROP SCHEMA IF EXISTS fcms' }
    end

    execute <<-SQL
      CREATE TABLE IF NOT EXISTS fcms.unextracted_events (
        message TEXT,
        import_timestamp TIMESTAMP
      );
    SQL

    execute <<-SQL
      CREATE TABLE IF NOT EXISTS fcms.encrypted_events (
        message TEXT,
        import_timestamp TIMESTAMP,
        processed_timestamp TIMESTAMP
      );
    SQL

    execute <<-"SQL"
      CREATE TABLE IF NOT EXISTS fcms.events (
        jti VARCHAR(256) PRIMARY KEY,
        message #{using_redshift_adapter ? 'SUPER' : 'JSONB'},
        import_timestamp TIMESTAMP
      );
    SQL

  end
end
