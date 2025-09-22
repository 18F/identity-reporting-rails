# frozen_string_literal: true

module FraudOps
  class RedisClient
    attr_reader :redis_pool
    def initialize
      @redis_pool = REDIS_POOL
    end

    def read_events(batch_size: 1000)
      events = {}
      hourly_keys.each do |hourly_key|
        @redis_pool.with do |client|
          client.hscan_each(hourly_key, count: batch_size) do |k, v|
            break if events.keys.count == batch_size

            # key is event_key, value is JWE, and we also capture partition_dt for Redshift
            events[k] = [v, get_partition_dt(hourly_key)]
          end
        end
      end
      events
    end

    def delete_events(keys:)
      total_deleted = 0
      hourly_keys.each do |hourly_key|
        @redis_pool.with do |client|
          total_deleted += client.hdel(hourly_key, keys)
        end
      end

      total_deleted
    end

    private

    def get_partition_dt(hourly_key)
      hourly_key.split(':').second.to_time.in_time_zone('UTC').strftime('%Y-%m-%d')
    end

    def hourly_keys
      @redis_pool.with do |client|
        client.keys('fraud-ops-events:*')
      end.sort
    end
  end
end
