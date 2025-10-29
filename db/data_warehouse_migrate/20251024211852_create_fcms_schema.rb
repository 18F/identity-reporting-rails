class CreateFcmsSchema < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'CREATE SCHEMA IF NOT EXISTS fcms'
      end
      dir.down do
        execute 'DROP SCHEMA IF EXISTS fcms'
      end
    end
  end
end
