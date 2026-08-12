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

    # Monotonic cursor over event_key drives termination INDEPENDENTLY of what the
    # UPDATE writes. This is critical: the extractor returns event_type = nil for
    # rows whose message yields no event object (empty hash, unparseable JSON, or no
    # /event-type/ key), so those rows keep matching `event_type IS NULL`. Without a
    # cursor, a batch full of such rows would re-select forever and never terminate.
    # event_key is a VARCHAR, so every real key sorts greater than '' — starting the
    # cursor at '' includes the lowest event_key in the first batch.
    #
    # The `event_type IS NULL` predicate is retained on top of the cursor so a re-run
    # after a partial/interrupted run skips already-populated rows and never collides
    # with the live ingestion job. Within a run the cursor guarantees forward progress;
    # across runs the predicate guarantees idempotency.
    cursor = ''

    loop do
      rows = connection.exec_query(
        ActiveRecord::Base.send(
          :sanitize_sql_array,
          ['SELECT event_key, message FROM fraudops.frd_events ' \
           'WHERE event_type IS NULL AND message IS NOT NULL AND event_key > ? ' \
           'ORDER BY event_key LIMIT ?', cursor, batch_size],
        ),
      ).to_a
      break if rows.empty?

      # NOTE: this single Postgres-style parameterized UPDATE is what actually runs
      # in local/test (jsonb) and against Redshift in prod (SUPER). The Redshift path
      # is verified-by-inspection only — it is not exercised by these specs, per the
      # plan's Redshift caveat — but the statement shape is adapter-agnostic.
      #
      # The SET clause and its bound values are both derived from
      # FraudOps::EventFieldExtractor::COLUMNS, so adding/removing a flattened column
      # (via FIELDS + a migration) needs no change here.
      columns = FraudOps::EventFieldExtractor::COLUMNS
      set_clause = columns.map { |column| "#{column} = ?" }.join(', ')
      update_sql = "UPDATE fraudops.frd_events SET #{set_clause} WHERE event_key = ?"

      DataWarehouseApplicationRecord.transaction do
        rows.each do |row|
          decrypted = BackfillColumns.parse_message(row['message'])
          fields = FraudOps::EventFieldExtractor.call(decrypted)
          bindings = columns.map { |column| fields[column] } + [row['event_key']]
          connection.execute(
            ActiveRecord::Base.send(:sanitize_sql_array, [update_sql, *bindings]),
          )
        end
      end

      # Advance the cursor past the last processed key so the next batch strictly
      # progresses, even for rows that were left with event_type = NULL.
      cursor = rows.last['event_key']
      total += rows.size
      log.call('Batch complete', batch_rows: rows.size, total_updated: total)
      break if rows.size < batch_size
    end

    log.call('Backfill complete', total_updated: total)
  end
end
