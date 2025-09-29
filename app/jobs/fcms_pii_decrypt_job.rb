class FcmsPiiDecryptJob < ApplicationJob
  queue_as :default

  def perform(batch_size: 1000)
    return log_info('Skipped because fraud_ops_tracker_enabled is false') unless job_enabled?

    total_processed = 0
    loop do
      encrypted_events = fetch_encrypted_events(limit: batch_size)
      break if encrypted_events.empty?

      processed_this_batch = process_encrypted_events_bulk(encrypted_events)
      total_processed += processed_this_batch
      break if encrypted_events.size < batch_size # no more remaining
    end

    log_info(
      'Job completed',
      successfully_processed: total_processed,
      batch_size: batch_size,
    )
    nil
  rescue => e
    log_error('Job failed', error: e.message, backtrace: e)
    raise
  end

  private

  def fetch_encrypted_events(limit:)
    query = <<~SQL.squish
      SELECT event_key, message
      FROM fcms.encrypted_events
      WHERE processed_timestamp IS NULL
      ORDER BY event_key
      LIMIT ?
    SQL
    sanitized_query = ActiveRecord::Base.send(:sanitize_sql_array, [query, limit])
    connection.execute(sanitized_query).to_a
  end

  def process_encrypted_events_bulk(encrypted_events)
    return 0 if encrypted_events.empty?

    decrypted_events, successful_ids = decrypt_events(encrypted_events)

    if decrypted_events.empty?
      log_info('No successfully decrypted events in batch')
      return 0
    end

    ActiveSupport::Notifications.instrument('fcms_pii_decrypt_job.persist_batch') do
      DataWarehouseApplicationRecord.transaction do
        bulk_insert_decrypted_events(decrypted_events)
        bulk_update_processed_timestamp(successful_ids)
      end
    end

    log_info(
      'Bulk operations completed',
      inserted_count: decrypted_events.size,
      updated_count: successful_ids.size,
    )
    successful_ids.size
  rescue ActiveRecord::StatementInvalid => e
    log_error('Bulk processing failed', error: e.message, backtrace: e)
    raise
  end

  def decrypt_events(encrypted_events)
    decrypted_events = []
    successful_ids   = []

    encrypted_events.each do |row|
      decrypted = decrypt_data(row['message'], private_key, row['event_key'])
      next unless decrypted

      decrypted_events << {
        event_key: row['event_key'],
        message: decrypted,
      }
      successful_ids << row['event_key']
    end

    [decrypted_events, successful_ids]
  end

  def bulk_insert_decrypted_events(decrypted_events)
    return if decrypted_events.empty?

    value_fragment = using_redshift_adapter? ? '(?, JSON_PARSE(?))' : '(?, ?::jsonb)'
    placeholders = Array.new(decrypted_events.size, value_fragment).join(', ')

    values = decrypted_events.flat_map do |event|
      [event[:event_key], JSON.generate(event[:message])]
    end

    insert_sql = <<~SQL.squish
      INSERT INTO fcms.fraud_ops_events (event_key, message)
      VALUES #{placeholders}
    SQL

    sanitized = ActiveRecord::Base.send(:sanitize_sql_array, [insert_sql, *values])
    connection.execute(sanitized)

    log_info('Bulk insert completed', row_count: decrypted_events.size)
  end

  def bulk_update_processed_timestamp(event_ids)
    return if event_ids.empty?

    placeholders = (['?'] * event_ids.size).join(', ')
    update_sql = <<~SQL.squish
      UPDATE fcms.encrypted_events
      SET processed_timestamp = CURRENT_TIMESTAMP
      WHERE event_key IN (#{placeholders})
    SQL

    sanitized = ActiveRecord::Base.send(:sanitize_sql_array, [update_sql, *event_ids])
    connection.execute(sanitized)

    log_info('Bulk update completed', updated_count: event_ids.size)
  end

  def decrypt_data(encrypted_data, key, event_key)
    json = JWE.decrypt(encrypted_data, key)
    JSON.parse(json).deep_symbolize_keys
  rescue => e
    log_error('Failed to decrypt and parse data', event_key: event_key, error: e.message)
    nil
  end

  def job_enabled?
    IdentityConfig.store.fraud_ops_tracker_enabled
  end

  def using_redshift_adapter?
    DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
  end

  def skip_job_execution
    log_info('Skipped because fraud_ops_tracker_enabled is false')
  end

  def private_key
    @private_key ||= OpenSSL::PKey::RSA.new(IdentityConfig.store.fraud_ops_private_key)
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end

  def log_info(message, **data)
    payload = log_message(message, 'info').merge(data)
    Rails.logger.info(payload.to_json)
  end

  def log_error(message, **data)
    payload = log_message(message, 'error').merge(data)
    Rails.logger.error(payload.to_json)
  end

  def log_message(message, level)
    {
      job: self.class.name,
      level: level,
      message: message,
    }
  end
end
