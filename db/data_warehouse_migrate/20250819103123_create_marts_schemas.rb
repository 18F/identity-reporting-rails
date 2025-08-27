class CreateMartsSchemas < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'CREATE SCHEMA IF NOT EXISTS marts'
        execute 'CREATE SCHEMA IF NOT EXISTS qa_marts'
      end
      dir.down do
        execute 'DROP SCHEMA IF EXISTS qa_marts'
        execute 'DROP SCHEMA IF EXISTS marts'
      end
    end
  end
end
