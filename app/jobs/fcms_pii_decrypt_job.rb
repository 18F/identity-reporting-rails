class FcmsPiiDecryptJob < ApplicationJob
  queue_as :default

  def perform(private_key_pem)
    # todo: this will be replaced by the secret manager key
    private_key = OpenSSL::PKey::RSA.new(private_key_pem)

    fetch_insert_delete_data_from_redshift

    insert_data_to_redshift_events(private_key)

    log_info('FcmsPiiDecryptJob: Job completed', true)
  rescue => e
    log_info('FcmsPiiDecryptJob: Job failed', false, { error: e.message })
    raise
  end

  private

  def decrypt_jwt(encrypted_jwt, private_key)
    decrypted_jwt = JWE.decrypt(encrypted_jwt, private_key)
    JSON.parse(decrypted_jwt)
  end

  def fetch_insert_delete_data_from_redshift
    query = <<-SQL
      WITH moved_records AS (
        DELETE FROM fcms.unextracted_events
        RETURNING message
      )
      INSERT INTO fcms.encrypted_events (message, import_timestamp)
      SELECT message, CURRENT_TIMESTAMP FROM moved_records;
    SQL
    DataWarehouseApplicationRecord.connection.exec_query(query).to_a
    log_info(
      'FcmsPiiDecryptJob: Data fetch from unextracted_events to encrypted_events succeeded',
      true,
    )
  rescue ActiveRecord::StatementInvalid => e
    log_info(
      'FcmsPiiDecryptJob: Data fetch from unextracted_events to encrypted_events failed',
      false, { error: e.message }
    )
    raise
  end

  def insert_data_to_redshift_events(private_key)
    encrypted_events = DataWarehouseApplicationRecord.connection.exec_query(
      'SELECT message FROM fcms.encrypted_events WHERE processed_timestamp IS NULL',
    ).to_a

    return if encrypted_events.empty?

    values = encrypted_events.map do |event|
      message = decrypt_jwt(event['message'], private_key)
      jti = message['jti']
      sanitized_message = ActiveRecord::Base.connection.quote(message.to_json)
      sanitized_jti = ActiveRecord::Base.connection.quote(jti)
      "(#{sanitized_jti}, #{sanitized_message}, CURRENT_TIMESTAMP)"
    end.join(', ')

    if values.empty?
      log_info('FcmsPiiDecryptJob: No new encrypted events to process', true)
      return
    end

    insert_query = <<-SQL.squish
      INSERT INTO fcms.events (jti, message, import_timestamp)
      VALUES #{values}
      ON CONFLICT (jti) DO NOTHING;
    SQL

    begin
      DataWarehouseApplicationRecord.connection.execute(insert_query)
      log_info(
        'FcmsPiiDecryptJob: Data insert to Redshift events succeeded', true,
        { row_count: encrypted_events.size }
      )
    rescue ActiveRecord::StatementInvalid => e
      log_info(
        'FcmsPiiDecryptJob: Data insert to Redshift events failed', false,
        { error: e.message }
      )
      raise
    end
    update_encrypted_events_processed
  end

  def update_encrypted_events_processed
    begin
      query = <<~SQL
        UPDATE fcms.encrypted_events
        SET processed_timestamp = CURRENT_TIMESTAMP
        WHERE processed_timestamp IS NULL
      SQL
      DataWarehouseApplicationRecord.connection.execute(query)
      log_info('FcmsPiiDecryptJob: Updated processed_timestamp in encrypted_events', true)
    rescue ActiveRecord::StatementInvalid => e
      log_info(
        'FcmsPiiDecryptJob: Failed to update processed_timestamp in encrypted_events',
        false, { error: e.message }
      )
      raise
    end
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
