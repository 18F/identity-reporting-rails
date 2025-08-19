class CreateMartsSchema < ActiveRecord::Migration[7.2]
  def change
    reversible do |dir|
      dir.up { execute 'CREATE SCHEMA IF NOT EXISTS marts' }
      dir.down { execute 'DROP SCHEMA IF EXISTS marts' }
    end
  end
end
