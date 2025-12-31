class DropFcmsSchema < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'DROP SCHEMA IF EXISTS fcms CASCADE'
      end
      dir.down do
        execute 'CREATE SCHEMA IF NOT EXISTS fcms'
      end
    end
  end
end
