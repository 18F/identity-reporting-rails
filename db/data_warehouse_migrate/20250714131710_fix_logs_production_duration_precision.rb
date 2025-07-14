class FixLogsProductionDurationPrecision < ActiveRecord::Migration[7.2]
  disable_ddl_transaction!

  def up
    if connection.adapter_name.downcase.include?('redshift')
      execute "ALTER TABLE logs.production RENAME COLUMN duration TO duration_old"
      execute "ALTER TABLE logs.production ADD COLUMN duration DECIMAL(15,4) NULL"
      execute "UPDATE logs.production SET duration = duration_old::DECIMAL(15,4)"
      execute "ALTER TABLE logs.production DROP COLUMN duration_old"
    else
      change_column 'logs.production', :duration, :decimal, precision: 15, scale: 4, null: true
    end
  end

  def down
    if connection.adapter_name.downcase.include?('redshift')
      execute "ALTER TABLE logs.production RENAME COLUMN duration TO duration_old"
      execute "ALTER TABLE logs.production ADD COLUMN duration DECIMAL(10,6) NULL"
      execute "UPDATE logs.production SET duration = duration_old::DECIMAL(10,6)"
      execute "ALTER TABLE logs.production DROP COLUMN duration_old"
    else
      change_column 'logs.production', :duration, :decimal, precision: 10, scale: 6, null: true
    end
  end
end
