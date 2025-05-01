class CreateSystemTablesSyncMetadata < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up { execute 'CREATE SCHEMA IF NOT EXISTS system_tables' }
      dir.down { execute 'DROP SCHEMA IF EXISTS system_tables' }
    end
    create_table 'system_tables.sync_metadata', if_not_exists: true do |t|
      t.string :table_name
      t.timestamp :last_sync_time

      t.timestamps
    end
  end
end
