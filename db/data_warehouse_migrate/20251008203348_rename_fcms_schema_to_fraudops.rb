class RenameFcmsSchemaToFraudops < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up do
        execute 'ALTER SCHEMA fcms RENAME TO fraudops;'
      end
      dir.down do
        execute 'ALTER SCHEMA fraudops RENAME TO fcms;'
      end
    end
  end
end
