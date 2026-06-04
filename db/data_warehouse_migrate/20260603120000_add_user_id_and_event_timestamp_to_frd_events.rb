class AddUserIdAndEventTimestampToFrdEvents < ActiveRecord::Migration[8.0]
  def change
    reversible do |dir|
      dir.up do
        if using_redshift_adapter?
          execute 'ALTER TABLE fraudops.frd_events ADD COLUMN user_id VARCHAR(256);'
          execute 'ALTER TABLE fraudops.frd_events ADD COLUMN event_timestamp TIMESTAMP;'
        elsif table_exists?('frd_events')
          execute 'ALTER TABLE frd_events ADD COLUMN user_id VARCHAR(256);'
          execute 'ALTER TABLE frd_events ADD COLUMN event_timestamp TIMESTAMP;'
        end
      end
      dir.down do
        if using_redshift_adapter?
          execute 'ALTER TABLE fraudops.frd_events DROP COLUMN event_timestamp;'
          execute 'ALTER TABLE fraudops.frd_events DROP COLUMN user_id;'
        elsif table_exists?('frd_events')
          execute 'ALTER TABLE frd_events DROP COLUMN event_timestamp;'
          execute 'ALTER TABLE frd_events DROP COLUMN user_id;'
        end
      end
    end
  end

  private

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
