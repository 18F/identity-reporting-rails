class UpdateSystemTableColumnTypes < ActiveRecord::Migration[7.2]

  def up
    convert_column_type('system_tables.stl_query', 'querytxt', 'VARCHAR(4000)')
    convert_column_type('system_tables.stl_query', 'label', 'VARCHAR(320)')
    convert_column_type('system_tables.svl_s3query_summary', 'is_partitioned', 'VARCHAR(MAX)')
    convert_column_type('system_tables.svl_s3query_summary', 'is_rrscan', 'VARCHAR(MAX)')
    convert_column_type('system_tables.svl_s3query_summary', 'is_nested', 'VARCHAR(MAX)')
  end
  
  def down
    raise ActiveRecord::IrreversibleMigration, 'This migration is irreversible, create a new migration to edit the columns'
  end

  private

  def convert_column_type(table_name, column_name, new_type)
    temp_column_name = "#{column_name}_temp"

    if connection.adapter_name.downcase.include?('redshift')
      execute "ALTER TABLE #{table_name} ADD COLUMN #{temp_column_name} #{new_type};"
      execute "UPDATE #{table_name} SET #{temp_column_name} = #{column_name};"
      execute "ALTER TABLE #{table_name} DROP COLUMN #{column_name};"
      execute "ALTER TABLE #{table_name} RENAME COLUMN #{temp_column_name} TO #{column_name};"
    end
  end
end
