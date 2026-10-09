class SetSuperuserSessionTimeout < ActiveRecord::Migration[8.1]
  def change
    return unless connection.adapter_name.downcase.include?('redshift')

    reversible do |dir|
      dir.up { execute('ALTER USER superuser SESSION TIMEOUT 900;') }
      # Redshift rejects SESSION TIMEOUT 0 (range is 60s–20d); RESET restores the cluster default.
      dir.down { execute('ALTER USER superuser RESET SESSION TIMEOUT;') }
    end
  end
end
