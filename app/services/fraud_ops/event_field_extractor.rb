# frozen_string_literal: true

module FraudOps
  # Single source of truth for flattening a decrypted FraudOps event into the
  # columns promoted onto fraudops.frd_events. Used by both FraudOpsPiiDecryptJob
  # (going-forward ingestion) and the frd_events:backfill_columns rake task so the
  # two can never drift.
  class EventFieldExtractor
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

    def call
      obj = event_object || {}
      {
        event_type: event_type_slug,
        success: obj[:success],
        device_id: obj[:device_id],
        user_ip_address: obj[:user_ip_address],
        agency_uuid: obj[:agency_uuid],
        unique_session_id: obj[:unique_session_id],
      }
    end

    # The single event value, in either shape. One record == one event (verified
    # against prod: max_events_per_row == 1), so we take the first pair.
    def event_object
      _key, value = event_pair
      value
    end

    private

    def event_type_slug
      key, _value = event_pair
      return nil if key.nil?

      key.to_s.split('/').last
    end

    # Returns [event_type_key, event_object] for both payload shapes, or [nil, nil].
    def event_pair
      events = @decrypted[:events]
      if events.is_a?(Hash) && events.any?
        events.first # enveloped: { events: { "<url>" => {...} } }
      else
        # no-envelope: the event-type URL is a top-level key (~0.38% of prod rows)
        pair = @decrypted.find { |k, v| v.is_a?(Hash) && k.to_s.match?(EVENT_TYPE_KEY) }
        pair || [nil, nil]
      end
    end
  end
end
