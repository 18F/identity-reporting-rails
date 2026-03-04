class RenameFraudopsTables < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'ALTER TABLE fraudops.encrypted_events RENAME TO frd_encrypted_events;'
        execute 'ALTER TABLE fraudops.decrypted_events RENAME TO frd_events;'
      end
      dir.down do
        execute 'ALTER TABLE fraudops.frd_events RENAME TO decrypted_events;'
        execute 'ALTER TABLE fraudops.frd_encrypted_events RENAME TO encrypted_events;'
      end
    end
  end
end
