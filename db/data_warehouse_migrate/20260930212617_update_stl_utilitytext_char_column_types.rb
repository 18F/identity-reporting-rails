class UpdateStlUtilitytextCharColumnTypes < ActiveRecord::Migration[8.1]
  # system_tables.stl_utilitytext was created in Nov 2024, before
  # create_target_table started mapping CHAR to VARCHAR, so `text` is still
  # character(200) and `label` is still character(320). Redshift only allows
  # single-byte characters in fixed-length strings, so any multibyte character
  # (e.g. an em dash in dbt model SQL) makes the nightly MERGE fail with
  # "Invalid input ... code: 8001".
  #
  # Widths are preserved rather than widened: MAX(OCTET_LENGTH(text)) and
  # MAX(LEN(text)) both measure 200 in prod, so the source view caps `text` at
  # 200 *bytes*, and Redshift recommends the smallest workable column size.
  #
  # Trailing CHAR padding is copied as-is (no RTRIM) so stored bytes are
  # unchanged; consumers already trim at read time.
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
