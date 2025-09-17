class IdvRedisToRedshiftJob < ApplicationJob
  queue_as :default

  def perform
    @schema_name = 'fcms'
    @target_table_name = "encrypted_events"
    log_info('IdvRedisToRedshiftJob: Job started.', true)

    begin
      # poll Redis for IDV events and process them in batches
      redis_client = AttemptsApi::RedisClient.new
      fetch_redis_idv_batches(redis_client: redis_client) do |response_data|
        log_info("IdvRedisToRedshiftJob: Processing #{response_data.size} events into Redshift.", true)
        import_to_redshift(redis_client: redis_client, event_payloads: response_data)
      end
    rescue => e
      log_info('IdvRedisToRedshiftJob: Error occurred.', false, { error: e.message })
      raise
    end
    log_info('IdvRedisToRedshiftJob: Job completed.', true)
  end

  private

  def fetch_redis_idv_batches(redis_client: redis_client, batch_size: 1000)
    # Fetch data from Redis for IDV
    return unless IdentityConfig.store.data_warehouse_fcms_enabled

    while true
      events = redis_client.read_events(batch_size: batch_size)
      log_info(
        "IdvRedisToRedshiftJob: Read #{events.size} event(s) from Redis for processing.", true
      )
      break if events.empty?
      yield events
    end
  end

  def import_to_redshift(redis_client: redis_client, event_payloads: event_payloads)
    return if event_payloads.empty?

    begin
      DataWarehouseApplicationRecord.transaction do
        transaction_queries_to_run(event_payloads).each do |query|
          DataWarehouseApplicationRecord.connection.execute(query)
        end
      end
    end

    log_info(
      'IdvRedisToRedshiftJob: Data import to Redshift succeeded.', true,
      { row_count: event_payloads.size }
    )

    records_deleted = redis_client.delete_events(keys: event_payloads.keys)

    log_info(
      'IdvRedisToRedshiftJob: Deleted events from Redis.', true,
      { records_deleted: records_deleted }
    )
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

  def build_params
    {
      schema_name: DataWarehouseApplicationRecord.connection.quote_table_name(@schema_name),
      source_table_name_temp: DataWarehouseApplicationRecord.
        connection.quote_table_name("#{@target_table_name}_temp"),
      target_table_name: DataWarehouseApplicationRecord.
        connection.quote_table_name(@target_table_name),
    }
  end

  def transaction_queries_to_run(event_payloads)
    [
      drop_temp_source_table_query,
      create_temp_source_table_query,
      load_batch_into_temp_table_query(event_payloads),
      merge_temp_with_target_query,
    ]
  end

  def drop_temp_source_table_query
    format(<<~SQL, build_params)
      DROP TABLE IF EXISTS %{source_table_name_temp};
    SQL
  end 

  def create_temp_source_table_query
    format(<<~SQL, build_params)
      CREATE TABLE %{source_table_name_temp} (LIKE %{schema_name}.%{target_table_name} INCLUDING DEFAULTS);
    SQL
  end

  def load_batch_into_temp_table_query(event_payloads)
    values_list = event_payloads.map do |key, value|
      # Extract in the exact same order as your INSERT statement
      cols = [key, value[0], value[1]]

      # Quote each value for SQL
      quoted = cols.map { |val| DataWarehouseApplicationRecord.connection.quote(val) }

      "(#{quoted.join(', ')})"
    end.join(",\n")


    format(<<~SQL, build_params.merge(values_list: values_list))
      INSERT INTO %{source_table_name_temp} (event_key, message, partition_dt)
      VALUES
      %{values_list};
    SQL
  end

  def merge_temp_with_target_query
    if DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
      format(<<~SQL, build_params)
        MERGE INTO %{schema_name}.%{target_table_name}
        USING %{source_table_name_temp}
        ON %{schema_name}.%{target_table_name}.event_key = %{source_table_name_temp}.event_key
        AND %{schema_name}.%{target_table_name}.partition_dt = %{source_table_name_temp}.partition_dt
        WHEN NOT MATCHED THEN
          INSERT (event_key, message, partition_dt)
          VALUES (
            %{source_table_name_temp}.event_key,
            %{source_table_name_temp}.message,
            %{source_table_name_temp}.partition_dt,
          )
        ;
      SQL
    else
      # Local Postgres DB does not support REMOVE DUPLICATES clause
      # MERGE is not supported in Postges@14; use INSERT ON CONFLICT instead
      format(<<~SQL, build_params)
        INSERT INTO %{schema_name}.%{target_table_name} (
            event_key, message, processed_timestamp
        )
        SELECT *
        FROM %{source_table_name_temp}
        ;
      SQL
    end
  end
end 