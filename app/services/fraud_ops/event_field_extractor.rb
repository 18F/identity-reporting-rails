# frozen_string_literal: true

module FraudOps
  # Single source of truth for flattening a decrypted FraudOps event into the
  # columns promoted onto fraudops.frd_events. Used by both FraudOpsPiiDecryptJob
  # (going-forward ingestion) and the frd_events:backfill_columns rake task so the
  # two can never drift.
  #
  # To add or remove a flattened column, edit FIELDS below (and add/drop the column
  # via a migration). Everything downstream — the extracted hash, both INSERT
  # column lists/values in FraudOpsPiiDecryptJob, and the backfill UPDATE — derives
  # from FIELDS, so no other code needs to change.
  class EventFieldExtractor
    # Declarative column configuration. Each entry maps a flattened column name to:
    #   sql_type: :string or :boolean — controls how the Redshift value is rendered
    #             (strings are quoted, booleans use TRUE/FALSE/NULL literals). The
    #             Postgres path binds every value directly regardless of type.
    #   from:     an optional lambda deriving the value from the decrypted payload;
    #             when omitted, the value is read from the event object by column name.
    # The order here is the canonical column order used by every SQL statement.
    FIELDS = {
      event_type: {
        sql_type: :string,
        from: ->(extractor) { extractor.event_type_slug },
      },
      success: { sql_type: :boolean },
      device_id: { sql_type: :string },
      user_ip_address: { sql_type: :string },
      agency_uuid: { sql_type: :string },
      unique_session_id: { sql_type: :string },
    }.freeze

    COLUMNS = FIELDS.keys.freeze

    # Marks a key that looks like an attempts-api event-type identifier (URL) —
    # used to locate the event object in the no-envelope shape.
    EVENT_TYPE_KEY = %r{/event-type/}

    def self.call(decrypted)
      new(decrypted).call
    end

    def self.event_object(decrypted)
      new(decrypted).event_object
    end

    def initialize(decrypted)
      @decrypted = decrypted || {}
    end

    # Returns the flattened fields as a Hash keyed by COLUMNS, in canonical order.
    def call
      obj = event_object || {}
      FIELDS.each_with_object({}) do |(column, config), result|
        result[column] =
          if config[:from]
            config[:from].call(self)
          else
            obj[column]
          end
      end
    end

    # The single event value, in either shape. One record == one event (verified
    # against prod: max_events_per_row == 1), so we take the first pair.
    def event_object
      _key, value = event_pair
      value
    end

    # Public because a FIELDS `from:` lambda derives event_type from it.
    def event_type_slug
      key, _value = event_pair
      return nil if key.nil?

      key.to_s.split('/').last
    end

    private

    # Returns [event_type_key, event_object] for both payload shapes, or [nil, nil].
    # A prod census (2026-08-19) found every readable payload enveloped; the
    # no-envelope branch is kept defensively.
    def event_pair
      @event_pair ||= begin
        events = @decrypted[:events]
        if events.is_a?(Hash) && events.any?
          pair = events.first # enveloped: { events: { "<url>" => {...} } }
          pair.last.is_a?(Hash) ? pair : [nil, nil]
        else
          # no-envelope: the event-type URL is a top-level key
          pair = @decrypted.find { |k, v| v.is_a?(Hash) && k.to_s.match?(EVENT_TYPE_KEY) }
          pair || [nil, nil]
        end
      end
    end
  end
end
