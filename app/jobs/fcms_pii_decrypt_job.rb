class FcmsPiiDecryptJob < ApplicationJob
  queue_as :default

  BATCH_SIZE = 1_000
  FETCH_SQL = <<~SQL.squish
    SELECT event_key, message
    FROM fcms.encrypted_events
    WHERE processed_timestamp IS NULL
    ORDER BY event_key
    LIMIT ?
  SQL

  def perform(batch_size: BATCH_SIZE)
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
    log_error('Job failed', error: e.message, backtrace: trimmed_backtrace(e))
    raise
  end

  private

  def fetch_encrypted_events(limit:)
    sql = ActiveRecord::Base.send(:sanitize_sql_array, [FETCH_SQL, limit])
    connection.execute(sql).to_a
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
    log_error('Bulk processing failed', error: e.message, backtrace: trimmed_backtrace(e))
    raise
  end

  # Splits decryption so it is testable and isolated
  def decrypt_events(encrypted_events)
    decrypted_events = []
    successful_ids   = []

    encrypted_events.each do |row|
      decrypted = decrypt_data(row['message'], private_key)
      unless decrypted
        log_info('Failed to decrypt event', event_key: row['event_key'])
        next
      end

      decrypted_events << {
        event_key: row['event_key'],
        message: decrypted, # hash (symbolized keys)
      }
      successful_ids << row['event_key']
    end

    [decrypted_events, successful_ids]
  end

  def bulk_insert_decrypted_events(decrypted_events)
    return if decrypted_events.empty?

    adapter = connection.adapter_name.downcase
    redshift = adapter.include?('redshift')

    # For Redshift SUPER use JSON_PARSE(?), for Postgres jsonb use ?::jsonb
    value_fragment = redshift ? '(?, JSON_PARSE(?))' : '(?, ?::jsonb)'
    placeholders = Array.new(decrypted_events.size, value_fragment).join(', ')

    values = decrypted_events.flat_map do |ev|
      json_payload = ev[:message].is_a?(String) ? ev[:message] : JSON.generate(ev[:message])
      [ev[:event_key], json_payload]
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

  def decrypt_data(encrypted_data, key)
    json = JWE.decrypt(encrypted_data, key)
    JSON.parse(json).deep_symbolize_keys
  rescue => e
    log_error('Failed to decrypt data', error: e.message)
    nil
  end

  def job_enabled?
    IdentityConfig.store.fraud_ops_tracker_enabled
  end

  def private_key
    @private_key ||= begin
      raw = IdentityConfig.store.fraud_ops_private_key
      raw.is_a?(OpenSSL::PKey::RSA) ? raw : OpenSSL::PKey::RSA.new(raw)
    end
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end

  def log_info(message, **data)
    payload = base_log_payload(message).merge(data)
    Rails.logger.info(payload.to_json)
  end

  def log_error(message, **data)
    payload = base_log_payload(message).merge(level: 'error').merge(data)
    Rails.logger.error(payload.to_json)
  end

  def base_log_payload(message)
    {
      job: self.class.name,
      message: message,
      timestamp: Time.zone.now.utc.iso8601,
    }
  end

  def trimmed_backtrace(exception, lines: 5)
    Array(exception.backtrace).first(lines)
  end
end
