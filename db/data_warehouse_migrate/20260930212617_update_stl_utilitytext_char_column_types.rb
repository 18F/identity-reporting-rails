class UpdateStlUtilitytextCharColumnTypes < ActiveRecord::Migration[8.1]
  # system_tables.stl_utilitytext predates create_target_table's CHAR -> VARCHAR
  # mapping, so `text` is still character(200) and `label` character(320).
  # Redshift allows only single-byte characters in fixed-length strings, so one
  # multibyte character (e.g. an em dash in dbt model SQL) fails the nightly
  # MERGE with "Invalid input ... code: 8001".
  #
  # Widths are preserved, not widened: a prod measurement found
  # MAX(OCTET_LENGTH(text)) = MAX(LEN(text)) = 200, and Redshift recommends the
  # smallest workable column size. The copy is verbatim, so stored bytes
  # (including trailing CHAR padding) are unchanged, as in 20251028200932.
  def up
    convert_column_type('system_tables.stl_utilitytext', 'text', 'VARCHAR(200)')
    convert_column_type('system_tables.stl_utilitytext', 'label', 'VARCHAR(320)')
  end

  def down
    raise ActiveRecord::IrreversibleMigration,
          'This migration is irreversible, create a new migration to edit the columns'
  end

  private

  # Redshift cannot ALTER COLUMN TYPE, so add/copy/drop/rename.
  def convert_column_type(table_name, column_name, new_type)
    return unless connection.adapter_name.downcase.include?('redshift')
    return unless connection.table_exists?(table_name)
    return unless connection.column_exists?(table_name, column_name)

    temp_column_name = "#{column_name}_temp"

    execute "ALTER TABLE #{table_name} ADD COLUMN #{temp_column_name} #{new_type};"
    execute "UPDATE #{table_name} SET #{temp_column_name} = #{column_name};"
    execute "ALTER TABLE #{table_name} DROP COLUMN #{column_name};"
    execute "ALTER TABLE #{table_name} RENAME COLUMN #{temp_column_name} TO #{column_name};"
  end
end
