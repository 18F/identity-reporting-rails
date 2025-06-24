class UpdateLogsProductionTablecolumn < ActiveRecord::Migration[7.2]
  disable_ddl_transaction!
  
  def up
    if connection.adapter_name.downcase.include?('redshift')
      execute "ALTER TABLE logs.production RENAME COLUMN duration TO duration_old"
      execute "ALTER TABLE logs.production ADD COLUMN duration DECIMAL(10,6)"
      execute "UPDATE logs.production SET duration = duration_old::DECIMAL(10,6)"
      execute "ALTER TABLE logs.production DROP COLUMN duration_old"
    else
      change_column 'logs.production', :duration, :decimal, precision: 10, scale: 6
    end
  end
  
  def down
    if connection.adapter_name.downcase.include?('redshift')
      execute "ALTER TABLE logs.production RENAME COLUMN duration TO duration_old"      
      execute "ALTER TABLE logs.production ADD COLUMN duration FLOAT"
      execute "UPDATE logs.production SET duration = duration_old::FLOAT"
      execute "ALTER TABLE logs.production DROP COLUMN duration_old"
    else
      change_column 'logs.production', :duration, :float
    end
  end
end
