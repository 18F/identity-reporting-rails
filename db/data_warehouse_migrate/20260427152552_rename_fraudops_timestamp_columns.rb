class RenameFraudopsTimestampColumns < ActiveRecord::Migration[8.0]
  def change
    reversible do |dir|
      dir.up do
        execute 'ALTER TABLE fraudops.frd_encrypted_events RENAME COLUMN import_timestamp TO dw_created_at;'
        execute 'ALTER TABLE fraudops.frd_encrypted_events RENAME COLUMN processed_timestamp TO dw_processed_at;'
        execute 'ALTER TABLE fraudops.frd_events RENAME COLUMN import_timestamp TO dw_created_at;'
      end
      dir.down do
        execute 'ALTER TABLE fraudops.frd_encrypted_events RENAME COLUMN dw_created_at TO import_timestamp;'
        execute 'ALTER TABLE fraudops.frd_encrypted_events RENAME COLUMN dw_processed_at TO processed_timestamp;'
        execute 'ALTER TABLE fraudops.frd_events RENAME COLUMN dw_created_at TO import_timestamp;'
      end
    end
  end
end
