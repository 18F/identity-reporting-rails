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
      { column: 'git_sha', key: 'git_sha', type: 'VARCHAR' },
      { column: 'git_branch', key: 'git_branch', type: 'VARCHAR' },
      { column: 'timestamp', key: 'timestamp', type: 'TIMESTAMP' },
      { column: 'pid', key: 'pid', type: 'INTEGER' },
      { column: 'user_agent', key: 'user_agent', type: 'VARCHAR(12000)' },
      { column: 'ip', key: 'ip', type: 'VARCHAR' },
      { column: 'host', key: 'host', type: 'VARCHAR' },
      { column: 'trace_id', key: 'trace_id', type: 'VARCHAR' },
      { column: 'duration', key: 'duration', type: 'DECIMAL(15,4)' },
    ],
  }

  SOURCE_TABLE_NAMES = ['unextracted_events', 'unextracted_production']
  TYPES_TO_EXTRACT_AS_TEXT = ['TIMESTAMP']

  def perform(target_table_name)
    @schema_name = 'logs'
    @target_table_name = target_table_name
    @source_table_name = "unextracted_#{target_table_name}"
    unless SOURCE_TABLE_NAMES.include?(@source_table_name)
      Rails.logger.info(
        {
          job: self.class.name,
          success: false,
          message: 'Invalid table name',
          schema_name: @schema_name,
          source_table_name: @source_table_name,
        }.to_json,
      )
      return
    end

    @column_map = COLUMN_MAPPING[@source_table_name.to_sym]
    @merge_key = get_unique_id

    Rails.logger.info(
      {
        job: self.class.name,
        success: true,
        message: 'Processing records from source to target. Executing queries...',
        schema_name: @schema_name,
        source_table_name: @source_table_name,
        target_table_name: @target_table_name,
      }.to_json,
    )

    source_table_count =
      DataWarehouseApplicationRecord.
        connection.exec_query(source_table_count_query).first['c']

    if source_table_count > 0
      begin
        DataWarehouseApplicationRecord.transaction do
          transaction_queries_to_run.each do |query|
            DataWarehouseApplicationRecord.connection.execute(query)
          end
        end
      rescue => e
        Rails.logger.info(
          {
            job: self.class.name,
            success: false,
            message: e.message,
            schema_name: @schema_name,
            source_table_name: @source_table_name,
          }.to_json,
        )
        return
      end
    else
      Rails.logger.info(
        {
          job: self.class.name,
          success: false,
          message: 'Missing data in source table',
          schema_name: @schema_name,
          source_table_name: @source_table_name,
        }.to_json,
      )
      return
    end
    Rails.logger.info(
      {
        job: self.class.name,
        success: true,
        message: 'Query executed successfully',
        schema_name: @schema_name,
        source_table_name: @source_table_name,
        target_table_name: @target_table_name,
      }.to_json,
    )
  end

  def build_params
    {
      schema_name: DataWarehouseApplicationRecord.connection.quote_table_name(@schema_name),
      source_table_name: DataWarehouseApplicationRecord.
        connection.quote_table_name(@source_table_name),
      source_table_name_temp: DataWarehouseApplicationRecord.
        connection.quote_table_name("#{@source_table_name}_temp"),
      source_table_name_with_dups_temp: DataWarehouseApplicationRecord.
        connection.quote_table_name("#{@source_table_name}_with_dups_temp"),
      target_table_name: DataWarehouseApplicationRecord.
        connection.quote_table_name(@target_table_name),
      merge_key: DataWarehouseApplicationRecord.
        connection.quote_column_name(@merge_key),
    }
  end

  def lock_table_query
    format(<<~SQL, build_params)
      LOCK %{schema_name}.%{source_table_name};
    SQL
  end

  def create_temp_source_table_query
    duplicate_key = extract_json_key(
      column: 'message',
      key: @merge_key,
      type: 'VARCHAR',
    )
    format(<<~SQL, build_params)
      CREATE TEMP TABLE %{source_table_name_with_dups_temp} AS
      SELECT *, ROW_NUMBER() OVER (PARTITION BY #{duplicate_key}) as row_num
      FROM %{schema_name}.%{source_table_name};
    SQL
  end

  def create_temp_source_table_without_dups_query
    format(<<~SQL, build_params)
      CREATE TEMP TABLE %{source_table_name_temp} AS
      #{select_message_fields}
      FROM %{source_table_name_with_dups_temp}
      WHERE row_num = 1;
    SQL
  end

  def build_merge_variables(source_table_name_temp, column_map)
    # Include message and cloudwatch_timestamp at the beginning of the column map
    all_columns = [
      { column: 'message', key: 'message', type: 'JSONB' },
      { column: 'cloudwatch_timestamp', key: 'cloudwatch_timestamp', type: 'TIMESTAMP' },
    ] + column_map.map do |col|
      { column: col[:column], key: col[:key], type: col[:type] }
    end

    update_set = all_columns.map do |c|
      "#{c[:column]} = #{source_table_name_temp}.#{c[:column]}"
    end
    insert_columns = all_columns.map { |c| c[:column] }.join(' ,')
    insert_values = all_columns.map { |c| "#{source_table_name_temp}.#{c[:column]}" }.join(' ,')

    {
      update_set: update_set.join(",\n    "),
      insert_columns: insert_columns,
      insert_values: insert_values,
    }
  end

  def merge_temp_with_target_query
    @source_table_name_temp = "#{@source_table_name}_temp"
    vars = build_merge_variables(@source_table_name_temp, @column_map)

    if DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
      format(<<~SQL, build_params)
        MERGE INTO %{schema_name}.%{target_table_name}
        USING %{source_table_name_temp}
        ON %{schema_name}.%{target_table_name}.#{@merge_key} = %{source_table_name_temp}.#{@merge_key}
        WHEN MATCHED THEN
          UPDATE SET
            #{vars[:update_set]}
        WHEN NOT MATCHED THEN
          INSERT (#{vars[:insert_columns]})
          VALUES (#{vars[:insert_values]} )
          ;
        REMOVE DUPLICATES;
      SQL
    else
      # Local Postgres DB does not support REMOVE DUPLICATES clause
      # MERGE is not supported in Postges@14; use INSERT ON CONFLICT instead
      format(<<~SQL, build_params)
        INSERT INTO %{schema_name}.%{target_table_name} (
            message ,cloudwatch_timestamp ,#{@column_map.map { |c| c[:column] }.join(' ,')}
        )
        SELECT *
        FROM %{source_table_name_temp}
        ;
      SQL
    end
  end

  def drop_merged_records_from_source_table_query
    merge_key_from_json = extract_json_key(
      column: 'message',
      key: @merge_key,
      type: 'VARCHAR',
      keep_parenthesis: false,
    )
    format(<<~SQL, build_params)
      DELETE FROM %{schema_name}.%{source_table_name}
      USING %{schema_name}.%{target_table_name}
      WHERE %{schema_name}.%{source_table_name}.#{merge_key_from_json} = %{schema_name}.%{target_table_name}.%{merge_key}
      AND %{schema_name}.%{target_table_name}.cloudwatch_timestamp BETWEEN 
        (
          SELECT MIN(cloudwatch_timestamp)
          FROM %{source_table_name_temp}
        ) 
        AND
        (
          SELECT MAX(cloudwatch_timestamp)
          FROM %{source_table_name_temp}
        )
      ;
    SQL
  end

  def transaction_queries_to_run
    [
      create_temp_source_table_query,
      create_temp_source_table_without_dups_query,
      merge_temp_with_target_query,
      drop_merged_records_from_source_table_query,
    ]
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

  def source_table_count_query
    format(<<~SQL, build_params)
      SELECT COUNT(*) AS c
      FROM %{schema_name}.%{source_table_name}
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
    format(<<~SQL)
      #{select_query}
    SQL
  end

  def get_unique_id
    if @source_table_name in 'unextracted_events'
      'id'
    elsif @source_table_name in 'unextracted_production'
      'uuid'
    end
  end

  def extract_json_key(column:, key:, type:, keep_parenthesis: true)
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
          final_key = "(#{column}->>'#{key}')"
        else
          final_key = "(#{column}->#{key_parts[0..-2].join('->') + '->>' + key_parts[-1]})"
        end
      else
        final_key = "(#{column}->#{key_parts.join('->')})"
      end
      keep_parenthesis ? final_key : final_key[1..-2]
    end
  end
end
