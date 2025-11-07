class RedshiftSystemTableSyncJob < ApplicationJob
  queue_as :default

  def perform
    table_definitions.each do |table|
      setup_instance_variables(table)
      create_target_table
      upsert_data
      update_sync_time
    end
  end

  private

  def table_definitions
    YAML.load_file(config_file_path)['tables']
  end

  def config_file_path
    Rails.root.join('config/redshift_system_tables.yml')
  end

  def setup_instance_variables(table)
    @source_schema = table['source_schema']
    @target_schema = table['target_schema']
    @source_table = table['source_table']
    @target_table = table['target_table']
    @timestamp_column = table['timestamp_column']
    @column_keys = table['column_keys']
    @target_table_with_schema = [@target_schema, @target_table].join('.')
    @source_table_with_schema = [@source_schema, @source_table].join('.')
  end

  def target_table_exists?
    DataWarehouseApplicationRecord.connection.table_exists?(@target_table_with_schema)
  end

  def create_target_table
    return if target_table_exists?

    create_schema_if_not_exists

    columns = fetch_source_columns

    DataWarehouseApplicationRecord.connection.create_table(
      @target_table_with_schema,
      id: false,
    ) do |t|
      columns.each do |column_info|
        column_name, column_data_type = column_info.values_at('column', 'type')

        t.column column_name, redshift_data_type(column_data_type)
      end
    end

    log_info(
      "Created target table #{@target_table}", true,
      target_table: @target_table
    )
  end

  def create_schema_if_not_exists
    build_params = {
      target_schema: @target_schema,
    }

    schema_query = format(<<~SQL.squish, build_params)
      CREATE SCHEMA IF NOT EXISTS %{target_schema}
    SQL

    DataWarehouseApplicationRecord.connection.execute(schema_query)
    log_info("Schema #{@target_schema} created", true)
  rescue ActiveRecord::StatementInvalid => e
    if /unacceptable schema name/i.match?(e.message)
      log_info("Schema #{@target_schema} already created", true)
    else
      log_info(e.message, false)
      raise e
    end
  end

  def fetch_source_columns
    build_params = {
      source_schema: @source_schema,
      source_table: @source_table,
    }

    if DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
      query = format(<<~SQL, build_params)
        SELECT *
        FROM pg_table_def
        WHERE schemaname= '%{source_schema}' AND tablename = '%{source_table}';
      SQL
    else
      query = format(<<~SQL, build_params)
        SELECT column_name AS column, data_type AS type
        FROM information_schema.columns
        WHERE table_schema = '%{source_schema}' AND table_name = '%{source_table}';
      SQL
    end

    columns = DataWarehouseApplicationRecord.connection.exec_query(query).to_a
    log_info("Columns fetched for #{@source_table}", true) if columns.present?

    columns
  end

  def upsert_data
    perform_merge_upsert
  end

  def perform_merge_upsert
    source_columns = fetch_source_columns
    columns = source_columns.map { |col| col['column'] }

    select_columns = source_columns.map do |col|
      column_name = col['column']
      column_type = col['type']
      target_type = redshift_data_type(column_type)

      if target_type != column_type
        "CAST(#{column_name} AS #{target_type}) AS #{column_name}"
      else
        column_name
      end
    end.join(', ')

    update_assignments = columns.map { |col| "#{col} = source.#{col}" }.join(', ')
    insert_columns = columns.join(', ')
    insert_values = columns.map { |col| "source.#{col}" }.join(', ')
    on_conditions = @column_keys.map do |key|
      "#{@source_table}.#{key} = source.#{key}"
    end.join(' AND ')
    partition_by = @column_keys.map { |key| "#{@source_table}.#{key}" }.join(', ')

    build_params = {
      target_table_with_schema: @target_table_with_schema,
      source_table: @source_table,
      select_columns: select_columns,
      on_conditions: on_conditions,
      timestamp_column: @timestamp_column,
      update_assignments: update_assignments,
      insert_columns: insert_columns,
      insert_values: insert_values,
      partition_by: partition_by,
    }

    merge_query = format(<<~SQL.squish, build_params)
      MERGE INTO %{target_table_with_schema}
      USING(
        SELECT *
        FROM (
            SELECT %{select_columns}, ROW_NUMBER() OVER (PARTITION BY %{partition_by}) AS row_num
            FROM %{source_table}
        )
        WHERE row_num = 1
      ) AS source
      ON %{on_conditions}
      WHEN MATCHED THEN
        UPDATE SET %{update_assignments}
      WHEN NOT MATCHED THEN
        INSERT (%{insert_columns})
        VALUES (%{insert_values});
    SQL

    log_info("Merge query #{@source_table}", true, merge_query: merge_query)
    DataWarehouseApplicationRecord.connection.execute(merge_query)
    log_info("MERGE executed for #{@target_table_with_schema}", true)
  end

  def fetch_last_sync_time
    sync_metadata = SystemTablesSyncMetadata.find_by(table_name: @target_table)
    sync_metadata&.last_sync_time || (Time.zone.now - 6.days)
  end

  def update_sync_time
    sync_metadata = SystemTablesSyncMetadata.find_or_initialize_by(table_name: @target_table)
    sync_metadata.last_sync_time = Time.zone.now
    sync_metadata.save!
  end

  def redshift_data_type(data_type)
    case data_type
    when 'json', 'jsonb'
      'super'
    when 'text'
      'VARCHAR(MAX)'
    when /^char/
      "VARCHAR(#{data_type[/\d+/] || 'MAX'})"
    else
      data_type
    end
  end

  def log_info(message, success, additional_info = {})
    Rails.logger.info(
      {
        job: self.class.name,
        success: success,
        message: message,
      }.merge(additional_info).to_json,
    )
  end
end
