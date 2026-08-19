class FraudOpsPiiDecryptJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :default

  good_job_control_concurrency_with(
    total_limit: 1,
  )

  def perform(batch_size: 1000)
    unless job_enabled?
      return Rails.logger.info(log_format('Skipped because fraud_ops_tracker_enabled is false'))
    end

    Rails.logger.info(log_format('Job started', batch_size: batch_size))

    total_processed = 0
    loop do
      encrypted_events = fetch_encrypted_events(limit: batch_size)
      break if encrypted_events.empty?

      processed_this_batch = process_encrypted_events_bulk(encrypted_events)
      total_processed += processed_this_batch
      break if encrypted_events.size < batch_size # no more remaining
    end

    Rails.logger.info(
      log_format(
        'Job completed',
        successfully_processed: total_processed,
        batch_size: batch_size,
      ),
    )
    nil
  rescue => e
    Rails.logger.error(log_format('Job failed', error: e.message, backtrace: e))
    raise
  end

  private

  def fetch_encrypted_events(limit:)
    query = <<~SQL.squish
      SELECT event_key, message
      FROM fraudops.frd_encrypted_events
      WHERE dw_processed_at IS NULL
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
      Rails.logger.info(log_format('No successfully decrypted events in batch'))
      return 0
    end

    ActiveSupport::Notifications.instrument('fraud_ops_pii_decrypt_job.persist_batch') do
      DataWarehouseApplicationRecord.transaction do
        bulk_insert_decrypted_events(decrypted_events)
        bulk_update_processed_timestamp(successful_ids)
      end
    end

    Rails.logger.info(
      log_format(
        'Bulk operations completed',
        inserted_count: decrypted_events.size,
        updated_count: successful_ids.size,
      ),
    )
    successful_ids.size
  end

  def decrypt_events(encrypted_events)
    decrypted_events = []
    successful_ids   = []

    encrypted_events.each do |row|
      decrypted = decrypt_data(row['message'], private_key, row['event_key'])
      next unless decrypted

      extractor = FraudOps::EventFieldExtractor.new(decrypted)
      event_object = extractor.event_object || {}
      flattened = extractor.call

      decrypted_events << {
        event_key: row['event_key'],
        message: decrypted,
        user_id: event_object[:user_id],
        user_uuid: event_object[:user_uuid],
        event_timestamp: event_object[:occurred_at] && Time.zone.at(event_object[:occurred_at]),
      }.merge(flattened)
      successful_ids << row['event_key']
    end

    [decrypted_events, successful_ids]
  end

  # Columns written on every insert, in order. The flattened event columns are
  # sourced from FraudOps::EventFieldExtractor::COLUMNS so adding/removing a
  # flattened column requires no change here — edit FIELDS in the extractor.
  FIXED_LEADING_COLUMNS = %i[
    event_key message user_id user_uuid event_timestamp
  ].freeze

  def insert_columns
    @insert_columns ||= FIXED_LEADING_COLUMNS + FraudOps::EventFieldExtractor::COLUMNS +
                        [:dw_created_at]
  end

  def bulk_insert_decrypted_events(decrypted_events)
    return if decrypted_events.empty?

    column_list = insert_columns.join(', ')

    if using_redshift_adapter?
      values_sql = decrypted_events.map { |event| redshift_insert_values(event) }.join(', ')
      connection.execute(<<~SQL.squish)
        INSERT INTO fraudops.frd_events
          (#{column_list})
        VALUES #{values_sql}
      SQL
    else
      value_fragment = "(#{postgres_value_fragment})"
      placeholders = Array.new(decrypted_events.size, value_fragment).join(', ')
      values = decrypted_events.flat_map { |event| postgres_insert_values(event) }
      insert_sql = <<~SQL.squish
        INSERT INTO fraudops.frd_events
          (#{column_list})
        VALUES #{placeholders}
      SQL
      sanitized = ActiveRecord::Base.send(:sanitize_sql_array, [insert_sql, *values])
      connection.execute(sanitized)
    end

    Rails.logger.info(log_format('Bulk insert completed', row_count: decrypted_events.size))
  end

  # Postgres placeholder fragment (without the surrounding parens). message casts
  # to jsonb; dw_created_at is a literal; every other column binds one `?`.
  def postgres_value_fragment
    fragments = insert_columns.map do |column|
      case column
      when :message then '?::jsonb'
      when :dw_created_at then 'CURRENT_TIMESTAMP'
      else '?'
      end
    end
    fragments.join(', ')
  end

  # Bound values for the Postgres path, in insert_columns order, skipping the
  # literal dw_created_at (which is inlined in the fragment). Nil values are
  # preserved as binds (they become SQL NULL) — only the literal column is dropped.
  def postgres_insert_values(event)
    bound_insert_columns.map do |column|
      column == :message ? JSON.generate(event[:message]) : event[column]
    end
  end

  def bound_insert_columns
    @bound_insert_columns ||= insert_columns - [:dw_created_at]
  end

  def redshift_insert_values(event)
    parts = insert_columns.map do |column|
      redshift_column_literal(column, event)
    end
    "(#{parts.join(', ')})"
  end

  # Renders a single column's Redshift SQL literal. Fixed columns keep their
  # bespoke handling; flattened columns are rendered by their configured sql_type.
  def redshift_column_literal(column, event)
    case column
    when :event_key then connection.quote(event[:event_key])
    when :message then "JSON_PARSE(#{dollar_quote(JSON.generate(event[:message]))})"
    when :user_id then redshift_integer_literal(event[:user_id])
    when :user_uuid, :event_timestamp
      event[column] ? connection.quote(event[column]) : 'NULL'
    when :dw_created_at then 'CURRENT_TIMESTAMP'
    else redshift_flattened_literal(column, event[column])
    end
  end

  # Renders a flattened column per its FIELDS sql_type: booleans via redshift_bool
  # (nil-safe), everything else quoted-or-NULL.
  def redshift_flattened_literal(column, value)
    config = FraudOps::EventFieldExtractor::FIELDS.fetch(column)
    if config[:sql_type] == :boolean
      redshift_bool(value)
    else
      value ? connection.quote(value) : 'NULL'
    end
  end

  def redshift_bool(value)
    return 'NULL' if value.nil?

    value ? 'TRUE' : 'FALSE'
  end

  # user_id is spliced as a bare literal, so anything non-integral renders as
  # NULL instead of raw SQL (message still preserves the original payload).
  def redshift_integer_literal(value)
    Integer(value, exception: false)&.to_s || 'NULL'
  end

  def dollar_quote(str)
    tag = 'json'
    tag = "j#{SecureRandom.hex(4)}" while str.include?("$#{tag}$")
    "$#{tag}$#{str}$#{tag}$"
  end

  def bulk_update_processed_timestamp(event_ids)
    return if event_ids.empty?

    placeholders = (['?'] * event_ids.size).join(', ')
    update_sql = <<~SQL.squish
      UPDATE fraudops.frd_encrypted_events
      SET dw_processed_at = CURRENT_TIMESTAMP
      WHERE event_key IN (#{placeholders})
    SQL

    sanitized = ActiveRecord::Base.send(:sanitize_sql_array, [update_sql, *event_ids])
    connection.execute(sanitized)

    Rails.logger.info(log_format('Bulk update completed', updated_count: event_ids.size))
  end

  def decrypt_data(encrypted_data, key, event_key)
    json = JWE.decrypt(encrypted_data, key)
    JSON.parse(json).deep_symbolize_keys
  rescue => e
    Rails.logger.error(
      log_format(
        'Failed to decrypt and parse data', event_key: event_key,
                                            error: e.message
      ),
    )
    nil
  end

  def job_enabled?
    IdentityConfig.store.fraud_ops_tracker_enabled
  end

  def using_redshift_adapter?
    connection.adapter_name.downcase.include?('redshift')
  end

  def private_key
    @private_key ||= OpenSSL::PKey::RSA.new(IdentityConfig.store.fraud_ops_private_key)
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end

  def log_format(message, **data)
    {
      job: self.class.name,
      message:,
    }.merge(data).to_json
  end
end
