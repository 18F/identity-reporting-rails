class RenameFraudopsTables < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        if using_redshift_adapter?
          execute 'ALTER TABLE fraudops.encrypted_events RENAME TO frd_encrypted_events;'
          execute 'ALTER TABLE fraudops.decrypted_events RENAME TO frd_events;'
        else
          # In dev/test, tables may be in logs or public schema instead of fraudops
          move_and_rename('encrypted_events', 'frd_encrypted_events')
          move_and_rename('decrypted_events', 'frd_events')
        end
      end
      dir.down do
        if using_redshift_adapter?
          execute 'ALTER TABLE fraudops.frd_events RENAME TO decrypted_events;'
          execute 'ALTER TABLE fraudops.frd_encrypted_events RENAME TO encrypted_events;'
        else
          if table_exists_in_schema?('fraudops', 'frd_encrypted_events')
            execute 'ALTER TABLE fraudops.frd_encrypted_events SET SCHEMA logs;'
            execute 'ALTER TABLE logs.frd_encrypted_events RENAME TO encrypted_events;'
          end
          if table_exists_in_schema?('fraudops', 'frd_events')
            execute 'ALTER TABLE fraudops.frd_events SET SCHEMA logs;'
            execute 'ALTER TABLE logs.frd_events RENAME TO decrypted_events;'
          end
        end
      end
    end
  end

  private

  def using_redshift_adapter?
    ActiveRecord::Base.connection.adapter_name.downcase.include?('redshift')
  end

  def move_and_rename(old_name, new_name)
    if table_exists_in_schema?('fraudops', old_name)
      execute "ALTER TABLE fraudops.#{old_name} RENAME TO #{new_name};"
    elsif table_exists_in_schema?('logs', old_name)
      execute "ALTER TABLE logs.#{old_name} SET SCHEMA fraudops;"
      execute "ALTER TABLE fraudops.#{old_name} RENAME TO #{new_name};"
    elsif table_exists_in_schema?('public', old_name)
      execute "ALTER TABLE public.#{old_name} SET SCHEMA fraudops;"
      execute "ALTER TABLE fraudops.#{old_name} RENAME TO #{new_name};"
    end
  end

  def table_exists_in_schema?(schema, table)
    result = execute(
      "SELECT 1 FROM information_schema.tables " \
      "WHERE table_schema = '#{schema}' AND table_name = '#{table}' LIMIT 1",
    )
    result.any?
  end
end
