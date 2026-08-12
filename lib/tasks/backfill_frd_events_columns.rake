# frozen_string_literal: true

namespace :frd_events do
  # message arrives as a Hash (Postgres jsonb) or a JSON string (Redshift SUPER
  # text); normalize to a symbol-keyed Hash for the extractor. Never log its
  # contents (PII).
  module BackfillColumns
    module_function

    def parse_message(message)
      parsed = message.is_a?(String) ? JSON.parse(message) : message
      parsed.respond_to?(:deep_symbolize_keys) ? parsed.deep_symbolize_keys : {}
    rescue JSON::ParserError
      {}
    end
  end

  desc 'Backfill flattened event columns on fraudops.frd_events from message (batched, idempotent)'
  task :backfill_columns, [:batch_size] => :environment do |_t, args|
    batch_size = (args[:batch_size] || 1000).to_i
    connection = DataWarehouseApplicationRecord.connection
    total = 0

    log = ->(message, **data) do
      payload = { task: 'frd_events:backfill_columns', message: message }.merge(data)
      Rails.logger.info(payload.to_json)
    end

    log.call('Backfill started', batch_size: batch_size)

    loop do
      rows = connection.exec_query(
        ActiveRecord::Base.send(
          :sanitize_sql_array,
          ['SELECT event_key, message FROM fraudops.frd_events ' \
           'WHERE event_type IS NULL AND message IS NOT NULL ' \
           'ORDER BY event_key LIMIT ?', batch_size],
        ),
      ).to_a
      break if rows.empty?

      DataWarehouseApplicationRecord.transaction do
        rows.each do |row|
          decrypted = BackfillColumns.parse_message(row['message'])
          fields = FraudOps::EventFieldExtractor.call(decrypted)
          connection.execute(
            ActiveRecord::Base.send(
              :sanitize_sql_array,
              ['UPDATE fraudops.frd_events SET ' \
               'event_type = ?, success = ?, device_id = ?, user_ip_address = ?, ' \
               'agency_uuid = ?, unique_session_id = ? WHERE event_key = ?',
               fields[:event_type], fields[:success], fields[:device_id],
               fields[:user_ip_address], fields[:agency_uuid], fields[:unique_session_id],
               row['event_key']],
            ),
          )
        end
      end

      total += rows.size
      log.call('Batch complete', batch_rows: rows.size, total_updated: total)
      break if rows.size < batch_size
    end

    log.call('Backfill complete', total_updated: total)
  end
end
