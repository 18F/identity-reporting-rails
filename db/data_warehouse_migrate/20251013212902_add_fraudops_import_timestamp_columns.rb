class AddFraudopsImportTimestampColumns < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'ALTER TABLE fraudops.encrypted_events ADD COLUMN import_timestamp TIMESTAMP;'
        execute 'ALTER TABLE fraudops.decrypted_events ADD COLUMN import_timestamp TIMESTAMP;'
      end
      dir.down do
        execute 'ALTER TABLE fraudops.encrypted_events DROP COLUMN import_timestamp;'
        execute 'ALTER TABLE fraudops.decrypted_events DROP COLUMN import_timestamp;'
      end
    end
  end
end
