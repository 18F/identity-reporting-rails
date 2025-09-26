class IdvRedisToRedshiftJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency
  queue_as :default

  good_job_control_concurrency_with(
    total_limit: 1,
  )

  def perform
    enabled = IdentityConfig.store.fraud_ops_tracker_enabled
    unless enabled
      log_info(
        "IdvRedisToRedshiftJob: fraud_ops_tracker_enabled is #{enabled}, skipping job.",
        false,
      )
      return
    end

    @schema_name = 'fcms'
    @target_table_name = 'encrypted_events'
    @redis_client = FraudOps::RedisClient.new
    log_info('IdvRedisToRedshiftJob: Job started.', true)

    begin
      # poll Redis for IDV events and process them in batches
      fetch_redis_idv_batches do |response_data|
        log_info(
          "IdvRedisToRedshiftJob: Processing #{response_data.size} events into Redshift.",
          true,
        )
        @events_payload = response_data
        import_to_redshift
      end
    rescue => e
      log_info('IdvRedisToRedshiftJob: Error occurred.', false, { error: e.message })
      raise
    end
    log_info('IdvRedisToRedshiftJob: Job completed.', true)
  end

  private

  def fetch_redis_idv_batches(batch_size: 1000)
    # Fetch data from Redis for IDV

    loop do
      events = @redis_client.read_events(batch_size: batch_size)
      log_info(
        "IdvRedisToRedshiftJob: Read #{events.size} event(s) from Redis for processing.", true
      )
      break if events.empty?
      yield events
    end
  end

  def import_to_redshift
    return if @events_payload.empty?

    begin
      DataWarehouseApplicationRecord.transaction do
        transaction_queries_to_run.each do |query|
          DataWarehouseApplicationRecord.connection.execute(query)
        end
      end
    end

    log_info(
      'IdvRedisToRedshiftJob: Data import to Redshift succeeded.', true,
      { row_count: @events_payload.size }
    )

    records_deleted = @redis_client.delete_events(keys: @events_payload.keys)

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

  def transaction_queries_to_run
    [
      drop_temp_source_table_query,
      create_temp_source_table_query,
      load_batch_into_temp_table_query,
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

  def load_batch_into_temp_table_query
    values_list = @events_payload.map do |key, value|
      # Extract in the exact same order as your INSERT statement
      # key: event_key, value[0]: message, value[1]: partition_dt
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
    format(<<~SQL, build_params)
      MERGE INTO %{schema_name}.%{target_table_name}
      USING %{source_table_name_temp} source
      ON %{schema_name}.%{target_table_name}.event_key = source.event_key
      AND %{schema_name}.%{target_table_name}.partition_dt = source.partition_dt
      WHEN MATCHED THEN
        UPDATE SET message = source.message
      WHEN NOT MATCHED THEN
        INSERT (event_key, message, partition_dt)
        VALUES (source.event_key, source.message, source.partition_dt)
      ;
    SQL
  end
end
