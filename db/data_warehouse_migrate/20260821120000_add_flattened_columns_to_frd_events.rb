class AddFlattenedColumnsToFrdEvents < ActiveRecord::Migration[8.0]
  def change
    reversible do |dir|
      dir.up do
        if using_redshift_adapter?
          add_columns('fraudops.frd_events')
        elsif table_exists?('frd_events')
          add_columns('frd_events')
        end
      end
      dir.down do
        if using_redshift_adapter?
          drop_columns('fraudops.frd_events')
        elsif table_exists?('frd_events')
          drop_columns('frd_events')
        end
      end
    end
  end

  private

  def add_columns(table)
    execute "ALTER TABLE #{table} ADD COLUMN event_type VARCHAR(256);"
    execute "ALTER TABLE #{table} ADD COLUMN success BOOLEAN;"
    execute "ALTER TABLE #{table} ADD COLUMN device_id VARCHAR(256);"
    execute "ALTER TABLE #{table} ADD COLUMN user_ip_address VARCHAR(256);"
    execute "ALTER TABLE #{table} ADD COLUMN agency_uuid VARCHAR(256);"
    execute "ALTER TABLE #{table} ADD COLUMN unique_session_id VARCHAR(256);"
  end

  def drop_columns(table)
    execute "ALTER TABLE #{table} DROP COLUMN unique_session_id;"
    execute "ALTER TABLE #{table} DROP COLUMN agency_uuid;"
    execute "ALTER TABLE #{table} DROP COLUMN user_ip_address;"
    execute "ALTER TABLE #{table} DROP COLUMN device_id;"
    execute "ALTER TABLE #{table} DROP COLUMN success;"
    execute "ALTER TABLE #{table} DROP COLUMN event_type;"
  end

  def using_redshift_adapter?
    ActiveRecord::Base.connection.adapter_name.downcase.include?('redshift')
  end

  def table_exists?(table)
    result = execute(
      "SELECT 1 FROM information_schema.tables " \
      "WHERE table_name = '#{table}' LIMIT 1",
    )
    result.any?
  end
end
