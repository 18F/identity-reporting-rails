# frozen_string_literal: true

module FraudOps
  class RedisClient
    attr_reader :redis_pool
    def initialize
      @redis_pool = REDIS_POOL
    end

    def read_events(batch_size: 1000)
      events = {}
      bucket_keys.each do |bucket_key|
        @redis_pool.with do |client|
          client.hscan_each(bucket_key, count: batch_size) do |k, v|
            break if events.keys.count == batch_size

            # key is event_key, value is JWE, and we also capture partition_dt for Redshift
            events[k] = [v, get_partition_dt(bucket_key)]
          end
        end
      end
      events
    end

    def delete_events(keys:)
      total_deleted = 0
      bucket_keys.each do |bucket_key|
        @redis_pool.with do |client|
          total_deleted += client.hdel(bucket_key, keys)
        end
      end

      total_deleted
    end

    private

    def get_partition_dt(bucket_key)
      bucket_key.split(':').second.to_time.in_time_zone('UTC').strftime('%Y-%m-%d')
    end

    def bucket_keys
      timestamp = Time.current
      current_bucket = timestamp.
        in_time_zone('UTC').
        change(min: (timestamp.min / 5) * 5).
        iso8601
      bucket_keys = []
      @redis_pool.with do |client|
        bucket_keys = client.keys('fraud-ops-events:*')
      end.sort

      # exclude keys with current bucket to avoid processing partial data
      bucket_keys.reject { |key| key.include?("fraud-ops-events:#{current_bucket}") }
    end
  end
end
