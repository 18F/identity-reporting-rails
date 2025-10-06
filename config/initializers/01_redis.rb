# frozen_string_literal: true

if IdentityConfig.store.fraud_ops_tracker_enabled
  REDIS_POOL = ConnectionPool.new(size: IdentityConfig.store.redis_pool_size) do
    Redis.new(url: IdentityConfig.store.redis_url)
  end.freeze
end
