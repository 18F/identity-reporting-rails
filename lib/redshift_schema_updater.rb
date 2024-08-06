require 'yaml'

class RedshiftSchemaUpdater
  def initialize(schema_name)
    @schema_name = schema_name
  end

  def using_redshift_adapter?
    DataWarehouseApplicationRecord.
      connection.adapter_name.downcase.include?('redshift')
  end

  def update_schema_from_yaml(file_path)
    yaml_data = load_yaml(file_path)
    return unless yaml_data

    yaml_data.each do |table_data|
      table_name = "#{@schema_name}.#{table_data['table']}"
      columns = table_data['include_columns']

      if table_exists?(table_name)
        update_existing_table(table_name, columns)
      else
        create_table(table_name, columns)
      end
    end
  end

  def table_exists?(table_name)
    DataWarehouseApplicationRecord.connection.table_exists?(table_name)
  end

  def update_existing_table(table_name, columns)
    columns_objs = DataWarehouseApplicationRecord.connection.columns(table_name)
    existing_columns = columns_objs.map(&:name)

    # rubocop:disable Metrics/BlockLength
    columns.each do |column_info|
      config_column_name, config_column_data_type = column_info.values_at('name', 'datatype')
      datatype_metadata = columns_objs.find { |x| x.name == config_column_name }
      default_varchar_limit = using_redshift_adapter? ? 256 : nil
      config_column_options = {
        limit: column_info.fetch('limit', default_varchar_limit),
        if_not_exists: true,
      }
      column_exists = existing_columns.include?(config_column_name)

      if column_exists
        current_dw_data_types = [
          datatype_metadata.type.to_s,
          datatype_metadata.sql_type,
        ]
        data_type_requires_update = current_dw_data_types.exclude?(
          redshift_data_type(config_column_data_type),
        )
        is_string_data_type = (current_dw_data_types & ['string', 'text']).any?
        limit_changed = datatype_metadata.limit != config_column_options[:limit]
        varchar_requires_update = is_string_data_type && limit_changed
      end

      if !column_exists
        add_column(
          table_name,
          config_column_name,
          config_column_data_type,
          config_column_options,
        )
      elsif varchar_requires_update
        # Redshift supports altering the length of a VARCHAR column in place.
        update_varchar_length(table_name, config_column_name, config_column_options[:limit])
      elsif data_type_requires_update
        # Redshift does not support altering data type in place. Therefore, dropping
        # the column and adding it back with the new data type is required.
        update_column_data_type(
          table_name, config_column_name, config_column_data_type,
          config_column_options
        )
      end
    end
    # rubocop:enable Metrics/BlockLength

    existing_columns.each do |existing_column_name|
      column_names = columns.map { |item| item['name'] }
      unless column_names.include?(existing_column_name)
        remove_column(
          table_name,
          existing_column_name,
        )
      end
    end
  end

  def create_table(table_name, columns)
    DataWarehouseApplicationRecord.connection.create_table(table_name) do |t|
      columns.each do |column_info|
        column_name, column_data_type = column_info.values_at('name', 'datatype')
        config_column_options = {
          limit: column_info.fetch('limit', nil),
          if_not_exists: true,
        }
        next if column_name == 'id'

        t.column column_name, redshift_data_type(column_data_type), **config_column_options
      end
    end
  end

  def add_column(table_name, column_name, data_type, column_options = {})
    DataWarehouseApplicationRecord.connection.add_column(
      table_name,
      column_name,
      redshift_data_type(data_type),
      **column_options,
    )
  end

  def remove_column(table_name, column_name)
    DataWarehouseApplicationRecord.connection.remove_column(
      table_name, column_name, if_exists: true
    )
  end

  def update_varchar_length(table_name, column_name, new_limit)
    if using_redshift_adapter?
      DataWarehouseApplicationRecord.connection.execute(
        DataWarehouseApplicationRecord.sanitize_sql(
          "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} TYPE VARCHAR(#{new_limit})",
        ),
      )
    else
      DataWarehouseApplicationRecord.connection.change_column(
        table_name, column_name, 'string', limit: new_limit
      )
    end
  end

  def update_column_data_type(table_name, column_name, new_data_type, keyword_options)
    old_column_name = "#{column_name}_copy"
    rename_column(table_name, column_name, old_column_name)
    add_column(table_name, column_name, new_data_type, keyword_options)
    backfill_column(table_name, old_column_name, column_name)
    remove_column(table_name, old_column_name)
  end

  def rename_column(table_name, old_column_name, new_column_name)
    DataWarehouseApplicationRecord.connection.rename_column(
      table_name, old_column_name, new_column_name
    )
  end

  def backfill_column(table_name, from_column, to_column)
    DataWarehouseApplicationRecord.connection.execute(
      DataWarehouseApplicationRecord.sanitize_sql(
        "UPDATE #{table_name} SET #{to_column} = #{from_column};",
      ),
    )
  end

  def redshift_data_type(datatype)
    case datatype
    when 'json', 'jsonb'
      'super'
    when 'text'
      'string'
    else
      datatype
    end
  end

  private

  def load_yaml(file_path)
    YAML.load_file(file_path)
  rescue StandardError => e
    Rails.logger.error "Error loading include columns YML file: #{e.message}"
    nil
  end
end
