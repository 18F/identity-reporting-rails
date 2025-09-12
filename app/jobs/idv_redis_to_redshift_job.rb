require 'net/http'
require 'json'
require 'jwt'
require 'jwe'

class IDVRedisToRedshiftJob < ApplicationJob
  queue_as :default

  def perform
    log_info('IDVRedisToRedshiftJob: Job started.', true)
    redis_client = AttemptsApi::RedisClient.new

    begin
      response_data = fetch_redis_idv_data(redis_client: redis_client, batch_size: 1000)

      log_info("IDVRedisToRedshiftJob: Processing #{response_data[:sets].size} events into Redshift.", true)

      import_to_redshift(redis_client: redis_client, event_payloads: response_data[:sets])

      # todo: this is for testing purposes. Job will be handled in job_configurations.rb
    rescue => e
      log_info('IDVRedisToRedshiftJob: Error during API attempt.', false, { error: e.message })
      raise
    end

    log_info('IDVRedisToRedshiftJob: Job completed.', true)
  end

  private

  def fetch_redis_idv_data(redis_client: redis_client, batch_size: 1000)
    # Fetch data from Redis for IDV
    return unless IdentityConfig.store.data_warehouse_fcms_enabled


    events = redis_client.read_all_events(batch_size: batch_size)

    Rails.logger.info(
      "IDVRedisToRedshiftJob: Read #{events.size} events for processing."
    )

    events
  end

  def import_to_redshift(redis_client: redis_client, event_payloads: event_payloads)
    return if event_payloads.empty?

    values = event_payloads.flat_map do |payload_hash|
      key_hash, encrypted_payload = payload_hash.first
      [key_hash, encrypted_payload]
    end

    sql = <<~SQL
      INSERT INTO fcms.unextracted_events (key_hash, message, import_timestamp)
      VALUES #{(['(?, ?, CURRENT_TIMESTAMP)'] * encrypted_payloads.size).join(', ')}
    SQL

    DataWarehouseApplicationRecord.transaction do
      DataWarehouseApplicationRecord.connection.execute(
        ActiveRecord::Base.sanitize_sql_array([sql, *values]),
      )
    end

    log_info(
      'IDVRedisToRedshiftJob: Data import to Redshift succeeded.', true,
      { row_count: event_payloads.size }
    )

    records_deleted = redis_client.delete_events(event_payloads.map { |e| e.keys.first })

    log_info(
      'IDVRedisToRedshiftJob: Deleted events from Redis.', true,
      { records_deleted: records_deleted }
    )

  rescue => e
    log_info(
      'IDVRedisToRedshiftJob: Data import to Redshift failed.', false,
      { error: e.message, error_class: e.class.name }
    )
    raise
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