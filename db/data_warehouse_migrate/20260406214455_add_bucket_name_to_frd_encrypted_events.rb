class AddBucketNameToFrdEncryptedEvents < ActiveRecord::Migration[8.0]
  def change
    reversible do |dir|
      dir.up do
        if using_redshift_adapter?
          execute 'ALTER TABLE fraudops.frd_encrypted_events ADD COLUMN bucket_name VARCHAR(256);'
        elsif table_exists?('frd_encrypted_events')
          execute 'ALTER TABLE frd_encrypted_events ADD COLUMN bucket_name VARCHAR(256);'
        end
      end
      dir.down do
        if using_redshift_adapter?
          execute 'ALTER TABLE fraudops.frd_encrypted_events DROP COLUMN bucket_name;'
        elsif table_exists?('frd_encrypted_events')
          execute 'ALTER TABLE frd_encrypted_events DROP COLUMN bucket_name;'
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
