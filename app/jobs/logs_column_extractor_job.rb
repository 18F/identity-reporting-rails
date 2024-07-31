class LogsColumnExtractorJob < ApplicationJob
  queue_as :default

  # THE ORDER OF THE FIELDS IN THE SELECT QUERY SHOULD MATCH THE ORDER OF THE FIELDS
  # IN THE TARGET TABLE FOR PRODUCTION RUNS; ADDITIONALLY FIELDS LISTED HERE SHOULD
  # CONINCIDE WITH MIGRATIONS MADE TO THE ASSOCIATED TABLES. FOR EXAMPLE, IF A COLUMN
  # IS ADDED OR REMOVED FROM THE EVENTS TABLE, THE SELECT QUERY SHOULD BE UPDATED TO
  # REFLECT THE CHANGE.
  COLUMN_MAPPING = {
    unextracted_events: [
      { column: 'id', key: 'id', type: 'VARCHAR' },
      { column: 'name', key: 'name', type: 'VARCHAR' },
      { column: 'time', key: 'time', type: 'TIMESTAMP' },
      { column: 'visitor_id', key: 'visitor_id', type: 'VARCHAR' },
      { column: 'visit_id', key: 'visit_id', type: 'VARCHAR' },
      { column: 'log_filename', key: 'log_filename', type: 'VARCHAR' },
      { column: 'new_event', key: 'properties.new_event', type: 'BOOLEAN' },
      { column: 'path', key: 'properties.path', type: 'VARCHAR(12000)' },
      { column: 'user_id', key: 'properties.user_id', type: 'VARCHAR' },
      { column: 'locale', key: 'properties.locale', type: 'VARCHAR' },
      { column: 'user_ip', key: 'properties.user_ip', type: 'VARCHAR' },
      { column: 'hostname', key: 'properties.hostname', type: 'VARCHAR' },
      { column: 'pid', key: 'properties.pid', type: 'INTEGER' },
      { column: 'service_provider', key: 'properties.service_provider', type: 'VARCHAR' },
      { column: 'trace_id', key: 'properties.trace_id', type: 'VARCHAR' },
      { column: 'git_sha', key: 'properties.git_sha', type: 'VARCHAR' },
      { column: 'git_branch', key: 'properties.git_branch', type: 'VARCHAR' },
      { column: 'user_agent', key: 'properties.user_agent', type: 'VARCHAR(12000)' },
      { column: 'browser_name', key: 'properties.browser_name', type: 'VARCHAR' },
      { column: 'browser_version', key: 'properties.browser_version', type: 'VARCHAR' },
      { column: 'browser_platform_name',
        key: 'properties.browser_platform_name',
        type: 'VARCHAR' },
      { column: 'browser_platform_version',
        key: 'properties.browser_platform_version',
        type: 'VARCHAR' },
      { column: 'browser_device_name', key: 'properties.browser_device_name', type: 'VARCHAR' },
      { column: 'browser_mobile', key: 'properties.browser_mobile', type: 'BOOLEAN' },
      { column: 'browser_bot', key: 'properties.browser_bot', type: 'BOOLEAN' },
      { column: 'success', key: 'properties.event_properties.success', type: 'BOOLEAN' },
    ],
    unextracted_production: [
      { column: 'uuid', key: 'uuid', type: 'VARCHAR' },
      { column: 'method', key: 'method', type: 'VARCHAR' },
      { column: 'path', key: 'path', type: 'VARCHAR(12000)' },
      { column: 'format', key: 'format', type: 'VARCHAR' },
      { column: 'controller', key: 'controller', type: 'VARCHAR' },
      { column: 'action', key: 'action', type: 'VARCHAR' },
      { column: 'status', key: 'status', type: 'INTEGER' },
      { column: 'duration', key: 'duration', type: 'FLOAT' },
      { column: 'git_sha', key: 'git_sha', type: 'VARCHAR' },
      { column: 'git_branch', key: 'git_branch', type: 'VARCHAR' },
      { column: 'timestamp', key: 'timestamp', type: 'TIMESTAMP' },
      { column: 'pid', key: 'pid', type: 'INTEGER' },
      { column: 'user_agent', key: 'user_agent', type: 'VARCHAR(12000)' },
      { column: 'ip', key: 'ip', type: 'VARCHAR' },
      { column: 'host', key: 'host', type: 'VARCHAR' },
      { column: 'trace_id', key: 'trace_id', type: 'VARCHAR' },
    ],
  }

  SOURCE_TABLE_NAMES = ['unextracted_events', 'unextracted_production']
  TYPES_TO_EXTRACT_AS_TEXT = ['TIMESTAMP']

  def perform(target_table_name)
    @schema_name = 'logs'
    @target_table_name = target_table_name
    @source_table_name = "unextracted_#{target_table_name}"
    unless SOURCE_TABLE_NAMES.include? @source_table_name
      raise "Invalid source table name: #{@source_table_name}"
    end
    @column_map = COLUMN_MAPPING[@source_table_name.to_sym]
    @merge_key = get_unique_id

    Rails.logger.info(<<~STR.squish)
      LogsColumnExtractorJob: Processing records from source
      #{@source_table_name} to target #{@schema_name}.#{@target_table_name}
    STR

    Rails.logger.info 'LogsColumnExtractorJob: Executing queries...'
    DataWarehouseApplicationRecord.transaction do
      DataWarehouseApplicationRecord.connection.execute(lock_table_query)
      DataWarehouseApplicationRecord.connection.execute(create_temp_table_query)
      DataWarehouseApplicationRecord.connection.execute(drop_duplicate_rows_from_temp_query)
      DataWarehouseApplicationRecord.connection.execute(merge_temp_with_target_query)
      DataWarehouseApplicationRecord.connection.execute(truncate_source_table_query)
    end
    Rails.logger.info 'LogsColumnExtractorJob: Query executed successfully'
  end

  def lock_table_query
    DataWarehouseApplicationRecord.sanitize_sql(
      <<~SQL,
        LOCK #{@schema_name}.#{@source_table_name};
      SQL
    )
  end

  def create_temp_table_query
    DataWarehouseApplicationRecord.sanitize_sql(
      <<~SQL,
        CREATE TEMP TABLE if not exists #{@source_table_name}_temp AS
        #{select_message_fields}
        FROM #{@schema_name}.#{@source_table_name};
      SQL
    )
  end

  def drop_duplicate_rows_from_temp_query
    DataWarehouseApplicationRecord.sanitize_sql(
      <<~SQL,
        WITH duplicate_rows as (
            SELECT #{@merge_key}
            , ROW_NUMBER() OVER (PARTITION BY #{@merge_key} ORDER BY cloudwatch_timestamp desc) as row_num
            FROM #{@source_table_name}_temp
        )
        DELETE FROM #{@source_table_name}_temp
        USING duplicate_rows
        WHERE duplicate_rows.#{@merge_key} = #{@source_table_name}_temp.#{@merge_key} and duplicate_rows.row_num > 1;
      SQL
    )
  end

  def merge_temp_with_target_query
    if DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
      DataWarehouseApplicationRecord.sanitize_sql(
        <<~SQL,
          MERGE INTO #{@schema_name}.#{@target_table_name}
          USING #{@source_table_name}_temp
          ON #{@schema_name}.#{@target_table_name}.#{@merge_key} = #{@source_table_name}_temp.#{@merge_key}
          REMOVE DUPLICATES;
        SQL
      )
    else
      # Local Postgres DB does not support REMOVE DUPLICATES clause
      # MERGE is not supported in Postges@14; use INSERT ON CONFLICT instead
      DataWarehouseApplicationRecord.sanitize_sql(
        <<~SQL,
          INSERT INTO #{@schema_name}.#{@target_table_name} (
              message ,cloudwatch_timestamp ,#{@column_map.map { |c| c[:column] }.join(' ,')}
          )
          SELECT *
          FROM #{@source_table_name}_temp
          #{conflict_update_set};
        SQL
      )
    end
  end

  def truncate_source_table_query
    DataWarehouseApplicationRecord.sanitize_sql(
      <<~SQL,
        TRUNCATE #{@schema_name}.#{@source_table_name};
      SQL
    )
  end

  def conflict_update_set
    match_column_mappings = @column_map.map do |c|
      "#{c[:column]} = EXCLUDED.#{c[:column]}"
    end.join(' ,')
    <<~SQL.chomp
      ON CONFLICT (#{@merge_key})
      DO UPDATE SET
          message = EXCLUDED.message ,cloudwatch_timestamp = EXCLUDED.cloudwatch_timestamp ,#{match_column_mappings}
    SQL
  end

  def select_message_fields
    extract_and_cast_statements = @column_map.map do |col|
      col_name = extract_json_key(
        column: 'message',
        key: col[:key],
        type: col[:type],
      )

      "#{col_name}::#{col[:type]} as #{col[:column]}"
    end
    select_query = <<~SQL.chomp
      SELECT
          message, cloudwatch_timestamp, #{extract_and_cast_statements.join(" ,")}
    SQL
    DataWarehouseApplicationRecord.sanitize_sql(select_query)
  end

  def get_unique_id
    if @source_table_name in 'unextracted_events'
      'id'
    elsif @source_table_name in 'unextracted_production'
      'uuid'
    end
  end

  def extract_json_key(column:, key:, type:)
    if DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
      # Redshift environment using SUPER Column type
      "#{column}.#{key}"
    else
      # Local/Test environment using JSONB Column type
      key_parts = key.split('.')
      key_parts.map! { |part| DataWarehouseApplicationRecord.connection.quote(part) }
      to_string = TYPES_TO_EXTRACT_AS_TEXT.include?(type) || type.include?('VARCHAR') ? true : false
      if to_string
        if key_parts.length == 1
          "(#{column}->>'#{key}')"
        else
          "(#{column}->#{key_parts[0..-2].join('->') + '->>' + key_parts[-1]})"
        end
      else
        "(#{column}->#{key_parts.join('->')})"
      end
    end
  end
end
