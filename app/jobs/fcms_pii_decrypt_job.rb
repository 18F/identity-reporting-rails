class FcmsPiiDecryptJob < ApplicationJob
  queue_as :default

  LogHelper = JobHelpers::LogHelper

  def perform(private_key_pem = nil)
    return skip_job_execution unless job_enabled?

    move_unextracted_to_encrypted_events

    encrypted_events = fetch_encrypted_events
    return LogHelper.log_info('No encrypted events to process') if encrypted_events.empty?

    # todo: this will be replaced by the secret manager key
    private_key = get_private_key(private_key_pem)
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

  def get_private_key(private_key_pem = nil)
    OpenSSL::PKey::RSA.new(private_key_pem || JobHelpers::AttemptsApiKeypairHelper.private_key.to_pem)
  end

  def move_unextracted_to_encrypted_events
    query = <<~SQL
      WITH moved_records AS (
        DELETE FROM fcms.unextracted_events
        RETURNING event_key, message, event_timestamp
      )
      INSERT INTO fcms.encrypted_events (event_key, message, event_timestamp)
      SELECT event_key, message, event_timestamp
      FROM moved_records;
    SQL

    connection.execute(query)
    LogHelper.log_success('Data moved from unextracted_events to encrypted_events')
  rescue ActiveRecord::StatementInvalid => e
    LogHelper.log_error(
      'Failed to move data from unextracted_events to encrypted_events',
      error: e.message,
    )
    raise
  end

  def fetch_encrypted_events
    query = 'SELECT event_key, message FROM fcms.encrypted_events WHERE processed_timestamp IS NULL'
    connection.execute(query).to_a
  end

  def process_encrypted_events(encrypted_events, private_key)
    successfully_processed_ids = []
    decrypted_events = []

    encrypted_events.each do |event|
      decrypted_message = decrypt_data(event['message'], private_key)
      unless decrypted_message
        LogHelper.log_warning('Failed to decrypt event', event_id: event['id'])
        next
      end

      decrypted_events << {
        id: event['id'],
        jti: decrypted_message['jti'],
        message: decrypted_message,
      }
      successfully_processed_ids << event['id']
    end

    insert_decrypted_events(decrypted_events) unless decrypted_events.empty?
    successfully_processed_ids
  end

  def insert_decrypted_events(decrypted_events)
    values = build_insert_values(decrypted_events)
    return if values.empty?

    insert_query = <<~SQL.squish
      INSERT INTO fcms.events (jti, message, import_timestamp)
      VALUES #{values.join(', ')}
      ON CONFLICT (jti) DO NOTHING;
    SQL

    connection.execute(insert_query)
    LogHelper.log_success('Data inserted to events table', row_count: decrypted_events.size)
  rescue ActiveRecord::StatementInvalid => e
    LogHelper.log_error('Failed to insert data to events table', error: e.message)
    raise
  end

  def build_insert_values(decrypted_events)
    decrypted_events.map do |event|
      sanitized_message = connection.quote(event[:message].to_json)
      sanitized_jti = connection.quote(event[:jti])
      "(#{sanitized_jti}, #{sanitized_message}, CURRENT_TIMESTAMP)"
    end
  end

  def decrypt_data(encrypted_data, private_key)
    decoded_data = Base64.decode64(encrypted_data)
    decrypted_data = private_key.private_decrypt(decoded_data)
    JSON.parse(decrypted_data)
  rescue StandardError => e
    LogHelper.log_error('Failed to decrypt data', error: e.message)
    nil
  end

  def mark_events_as_processed(event_ids)
    return if event_ids.empty?

    # Convert array to SQL-safe format
    ids_list = event_ids.map { |id| connection.quote(id) }.join(', ')

    query = <<~SQL
      UPDATE fcms.encrypted_events
      SET processed_timestamp = CURRENT_TIMESTAMP
      WHERE id IN (#{ids_list})
    SQL

    connection.execute(query)
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

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end
end
