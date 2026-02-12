class PiiRetentionEnforcementJob < ApplicationJob
  queue_as :default

  CONFIG_PATH = Rails.root.join('config', 'pii_retention.yml').freeze

  def perform
    unless job_enabled?
      Rails.logger.info(log_format('Skipped because fraud_ops_tracker_enabled is false'))
      return
    end

    Rails.logger.info(log_format('Job started'))

    @errors = []
    @total_deleted = 0

    config[:schemas].each do |schema_name, schema_config|
      process_schema(schema_name.to_s, schema_config || {})
    end

    if @errors.any?
      Rails.logger.error(
        log_format(
          'Job completed with errors',
          total_deleted: @total_deleted,
          error_count: @errors.size,
          errors: @errors,
        ),
      )
      error_msg =
        "PII retention enforcement failed for #{@errors.size} table(s): #{@errors.join('; ')}"
      raise StandardError, error_msg
    end

    Rails.logger.info(log_format('Job completed successfully', total_deleted: @total_deleted))
  end

  private

  def process_schema(schema_name, schema_config)
    excluded_tables = Array(schema_config[:excluded_tables] || schema_config['excluded_tables'])
    included_tables = schema_config[:included_tables] || schema_config['included_tables'] || '*'
    timestamp_columns =
      schema_config[:timestamp_columns] || schema_config['timestamp_columns'] || {}

    tables = fetch_tables_in_schema(schema_name)

    # Apply included_tables filter first
    filtered_tables = filter_included_tables(tables, included_tables)

    # Then remove excluded tables
    final_tables = filtered_tables.reject { |table| excluded_tables.include?(table) }

    Rails.logger.info(
      log_format(
        'Processing schema',
        schema: schema_name,
        table_count: final_tables.size,
        excluded_count: excluded_tables.size,
      ),
    )

    final_tables.each do |table_name|
      process_table(schema_name, table_name, timestamp_columns)
    rescue StandardError => e
      # Catch errors in case process_table is stubbed or raises before its rescue block
      error_message = "#{schema_name}.#{table_name}: #{e.message}"
      @errors ||= []
      @errors << error_message
      Rails.logger.error(
        log_format(
          'Error processing table',
          schema: schema_name,
          table: table_name,
          error: e.message,
        ),
      )
    end
  end

  def process_table(schema_name, table_name, timestamp_columns)
    timestamp_column = resolve_timestamp_column(schema_name, table_name, timestamp_columns)

    unless timestamp_column
      message = "No timestamp column found for #{schema_name}.#{table_name}, skipping"
      Rails.logger.warn(log_format(message, schema: schema_name, table: table_name))
      return
    end

    deleted_count = delete_expired_records(schema_name, table_name, timestamp_column)

    @total_deleted ||= 0
    @total_deleted += deleted_count
    Rails.logger.info(
      log_format(
        'Retention enforcement completed for table',
        schema: schema_name,
        table: table_name,
        timestamp_column: timestamp_column,
        deleted_count: deleted_count,
      ),
    )
  rescue StandardError => e
    error_message = "#{schema_name}.#{table_name}: #{e.message}"
    @errors ||= []
    @errors << error_message
    Rails.logger.error(
      log_format(
        'Error processing table',
        schema: schema_name,
        table: table_name,
        error: e.message,
      ),
    )
  end

  def resolve_timestamp_column(schema_name, table_name, timestamp_columns)
    # Check YAML config for table-specific override
    yaml_column = timestamp_columns[table_name] || timestamp_columns[table_name.to_sym]
    return yaml_column if yaml_column

    # Check standard timestamp columns in priority order
    columns = fetch_columns_for_table(schema_name, table_name)
    ['import_timestamp', 'updated_at', 'created_at'].find { |col| columns.include?(col) }
  end

  def fetch_tables_in_schema(schema_name)
    query = <<~SQL.squish
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ?
        AND table_type = 'BASE TABLE'
      ORDER BY table_name
    SQL

    sanitized_query = ActiveRecord::Base.sanitize_sql_array([query, schema_name])
    result = connection.exec_query(sanitized_query)
    result.rows.flatten
  end

  def fetch_columns_for_table(schema_name, table_name)
    query = <<~SQL.squish
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = ?
        AND table_name = ?
    SQL

    sanitized_query = ActiveRecord::Base.sanitize_sql_array([query, schema_name, table_name])
    result = connection.exec_query(sanitized_query)
    result.rows.flatten
  end

  def delete_expired_records(schema_name, table_name, timestamp_column)
    quoted_schema = connection.quote_table_name(schema_name)
    quoted_table = connection.quote_table_name(table_name)
    quoted_column = connection.quote_column_name(timestamp_column)

    delete_query = <<~SQL.squish
      DELETE FROM #{quoted_schema}.#{quoted_table}
      WHERE #{quoted_column} < CURRENT_DATE - ?
    SQL

    sanitized_query = ActiveRecord::Base.sanitize_sql_array([delete_query, retention_days])
    result = connection.execute(sanitized_query)

    # Redshift returns the number of affected rows
    extract_deleted_count(result)
  end

  def extract_deleted_count(result)
    # Handle different adapter responses
    if result.respond_to?(:cmd_tuples)
      result.cmd_tuples
    elsif result.is_a?(Integer)
      result
    elsif result.respond_to?(:rows) && result.rows.any?
      result.rows.first.first.to_i
    else
      0
    end
  end

  def config
    @config ||= YAML.safe_load_file(CONFIG_PATH, symbolize_names: true)
  end

  def retention_days
    config[:retention_days] || 366
  end

  def job_enabled?
    IdentityConfig.store.fraud_ops_tracker_enabled
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end

  def filter_included_tables(tables, included_tables)
    # Handle wildcard - return all tables
    return tables if included_tables == '*' || included_tables == ['*']

    # Handle nil or empty - default to all tables (backward compatibility)
    return tables if included_tables.nil? || included_tables.empty?

    # Convert to array if needed and filter to only included tables
    included_array = Array(included_tables)
    tables.select { |table| included_array.include?(table) }
  end

  def log_format(message, **data)
    {
      job: self.class.name,
      message: message,
    }.merge(data).to_json
  end
end
