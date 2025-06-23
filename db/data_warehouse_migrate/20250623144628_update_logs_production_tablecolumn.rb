class UpdateLogsProductionTablecolumn < ActiveRecord::Migration[7.2]
  def change
    change_column 'logs.production', :duration, :decimal, precision: 10, scale: 6
  end
end
