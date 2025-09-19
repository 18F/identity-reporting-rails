class FcmsPiiDecryptJob < ApplicationJob
  queue_as :default

  LogHelper = JobHelpers::LogHelper

  def perform
    return skip_job_execution unless job_enabled?

    encrypted_events = fetch_encrypted_events
    return LogHelper.log_info('No encrypted events to process') if encrypted_events.empty?

    successfully_processed_ids = process_encrypted_events(encrypted_events, private_key)
    mark_events_as_processed(successfully_processed_ids)

    LogHelper.log_success(
      'Job completed',
      total_events: encrypted_events.size,
      successfully_processed: successfully_processed_ids.size,
    )
  rescue => e
    LogHelper.log_error('Job failed', error: e.message)
    raise
  end

  private

  def fetch_encrypted_events
    query = <<~SQL.squish
      SELECT event_key, message
      FROM fcms.encrypted_events
      WHERE processed_timestamp IS NULL
    SQL
    connection.execute(query).to_a
  end

  def process_encrypted_events(encrypted_events, private_key)
    successfully_processed_ids = []
    decrypted_events = []

    encrypted_events.each do |event|
      decrypted_message = decrypt_data(event['message'], private_key)
      unless decrypted_message
        LogHelper.log_info('Failed to decrypt event', event_key: event['event_key'])
        next
      end

      decrypted_events << {
        event_key: event['event_key'],
        message: decrypted_message,
      }
      successfully_processed_ids << event['event_key']
    end

    insert_decrypted_events(decrypted_events) unless decrypted_events.empty?
    successfully_processed_ids
  end

  def insert_decrypted_events(decrypted_events)
    return if decrypted_events.empty?

    placeholders = (['(?, ?)'] * decrypted_events.size).join(', ')
    values = decrypted_events.flat_map do |event|
      [event[:event_key], event[:message].to_json]
    end

    # todo: we need to confirm if/how we want to handle possible duplicates
    insert_query = <<~SQL.squish
      INSERT INTO fcms.events (event_key, message)
      VALUES #{placeholders};
    SQL

    sanitized_sql = ActiveRecord::Base.send(:sanitize_sql_array, [insert_query, *values])

    DataWarehouseApplicationRecord.transaction do
      connection.execute(sanitized_sql)
    end
    LogHelper.log_success('Data inserted to events table', row_count: decrypted_events.size)
  rescue ActiveRecord::StatementInvalid => e
    LogHelper.log_error('Failed to insert data to events table', error: e.message)
    raise
  end

  def decrypt_data(encrypted_data, private_key)
    decrypted_data = JWE.decrypt(encrypted_data, private_key)
    JSON.parse(decrypted_data).deep_symbolize_keys
  rescue => e
    LogHelper.log_error('Failed to decrypt data', error: e.message)
    nil
  end

  def mark_events_as_processed(event_ids)
    return if event_ids.empty?

    query = ActiveRecord::Base.sanitize_sql_array(
      [
        "UPDATE fcms.encrypted_events SET processed_timestamp = CURRENT_TIMESTAMP " \
        "WHERE event_key IN (#{(['?'] * event_ids.size).join(', ')})",
        *event_ids,
      ],
    )

    begin
      DataWarehouseApplicationRecord.transaction do
        connection.execute(query)
      end
    end
    LogHelper.log_success(
      'Updated processed_timestamp in encrypted_events',
      updated_count: event_ids.size,
    )
  rescue ActiveRecord::StatementInvalid => e
    LogHelper.log_error('Failed to update processed_timestamp', error: e.message)
    raise
  end

  def job_enabled?
    IdentityConfig.store.fraud_ops_tracker_enabled
  end

  def skip_job_execution
    LogHelper.log_info('Skipped because fraud_ops_tracker_enabled is false')
  end

  def private_key
    OpenSSL::PKey::RSA.new(IdentityConfig.store.fraud_ops_private_key)
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end
end
