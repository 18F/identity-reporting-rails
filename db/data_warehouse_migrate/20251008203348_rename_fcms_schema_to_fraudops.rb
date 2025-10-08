class RenameFcmsSchemaToFraudops < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'ALTER SCHEMA fcms RENAME TO fraudops;'
        execute 'ALTER TABLE fraudops.fraud_ops_events RENAME TO decrypted_events;'
      end
      dir.down do
        execute 'ALTER TABLE fraudops.decrypted_events RENAME TO fraud_ops_events;'
        execute 'ALTER SCHEMA fraudops RENAME TO fcms;'
      end
    end
  end
end
