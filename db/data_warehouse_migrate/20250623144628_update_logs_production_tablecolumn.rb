class UpdateLogsProductionTablecolumn < ActiveRecord::Migration[7.2]
  disable_ddl_transaction!
  
  def change
    change_column 'logs.production', :duration, :decimal, precision: 10, scale: 6
  end
end
