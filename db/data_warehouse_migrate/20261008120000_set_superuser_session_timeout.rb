class SetSuperuserSessionTimeout < ActiveRecord::Migration[8.1]
  def change
    return unless connection.adapter_name.downcase.include?('redshift')

    reversible do |dir|
      dir.up { execute('ALTER USER superuser SESSION TIMEOUT 900;') }
      # Redshift SESSION TIMEOUT RESET restores the cluster default.
      dir.down { execute('ALTER USER superuser RESET SESSION TIMEOUT;') }
    end
  end
end
