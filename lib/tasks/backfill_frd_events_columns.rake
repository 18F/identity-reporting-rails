# frozen_string_literal: true

namespace :frd_events do
  # message arrives as a Hash (Postgres jsonb) or a JSON string (Redshift SUPER
  # text); normalize to a symbol-keyed Hash for the extractor. Never log its
  # contents (PII).
  module BackfillColumns
    STAGING_TABLE = 'frd_events_backfill_stage'

    module_function

    def parse_message(message)
      parsed = message.is_a?(String) ? JSON.parse(message) : message
      parsed.respond_to?(:deep_symbolize_keys) ? parsed.deep_symbolize_keys : {}
    rescue JSON::ParserError
      {}
    end

    # Literals are built with the DW connection's quote (NOT ActiveRecord::Base's
    # primary-Postgres quoting): Redshift defaults to standard_conforming_strings
    # = off, so the two escape backslashes differently.
    def sql_literal(connection, sql_type, value)
      if sql_type == :boolean
        # Only real booleans render as TRUE/FALSE (the string "false" is NULL,
        # not a truthy-coerced TRUE) — mirrors the ingestion job's redshift_bool.
        case value
        when true then 'TRUE'
        when false then 'FALSE'
        else 'NULL'
        end
      else
        connection.quote(value)
      end
    end
  end

  desc 'Backfill flattened event columns on fraudops.frd_events from message (batched, idempotent)'
  task :backfill_columns, [:batch_size] => :environment do |_t, args|
    batch_size = Integer(args[:batch_size] || 1000, exception: false)
    unless batch_size&.positive?
      abort("batch_size must be a positive integer (got #{args[:batch_size].inspect})")
    end

    connection = DataWarehouseApplicationRecord.connection
    total = 0

    log = ->(message, **data) do
      payload = { task: 'frd_events:backfill_columns', message: message }.merge(data)
      Rails.logger.info(payload.to_json)
    end

    log.call('Backfill started', batch_size: batch_size)

    # Each batch is applied as ONE set-based UPDATE joined to a temp staging table
    # (Redshift runs per-row UPDATEs at ~450 rows/min; it does not accept VALUES in
    # a FROM clause, hence staging). The staging schema, INSERT column list, and SET
    # clause all derive from FraudOps::EventFieldExtractor::FIELDS, so adding or
    # removing a flattened column (via FIELDS + a migration) needs no change here.
    columns = FraudOps::EventFieldExtractor::COLUMNS
    fields = FraudOps::EventFieldExtractor::FIELDS
    staging = BackfillColumns::STAGING_TABLE
    staging_ddl = columns.map do |column|
      type = fields.fetch(column)[:sql_type] == :boolean ? 'BOOLEAN' : 'VARCHAR(256)'
      "#{column} #{type}"
    end.join(', ')
    set_clause = columns.map { |column| "#{column} = s.#{column}" }.join(', ')

    connection.execute("DROP TABLE IF EXISTS #{staging}")
    connection.execute("CREATE TEMP TABLE #{staging} (event_key VARCHAR(256), #{staging_ddl})")

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
        DataWarehouseApplicationRecord.send(
          :sanitize_sql_array,
          ['SELECT event_key, message FROM fraudops.frd_events ' \
           'WHERE event_type IS NULL AND message IS NOT NULL AND event_key > ? ' \
           'ORDER BY event_key LIMIT ?', cursor, batch_size],
        ),
      ).to_a
      break if rows.empty?

      values = rows.map do |row|
        decrypted = BackfillColumns.parse_message(row['message'])
        extracted = FraudOps::EventFieldExtractor.call(decrypted)
        literals = [connection.quote(row['event_key'])] + columns.map do |column|
          sql_type = fields.fetch(column)[:sql_type]
          BackfillColumns.sql_literal(connection, sql_type, extracted[column])
        end
        "(#{literals.join(', ')})"
      end

      # DELETE (not TRUNCATE — Redshift TRUNCATE implicitly commits) keeps the
      # stage/apply pair atomic per batch.
      DataWarehouseApplicationRecord.transaction do
        connection.execute("DELETE FROM #{staging}")
        connection.execute(
          "INSERT INTO #{staging} (event_key, #{columns.join(', ')}) VALUES #{values.join(', ')}",
        )
        connection.execute(
          "UPDATE fraudops.frd_events SET #{set_clause} " \
          "FROM #{staging} s WHERE fraudops.frd_events.event_key = s.event_key",
        )
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
