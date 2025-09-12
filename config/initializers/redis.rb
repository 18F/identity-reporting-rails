# frozen_string_literal: true

REDIS_POOL = ConnectionPool.new(size: IdentityConfig.store.redis_pool_size) do
  Redis.new(url: IdentityConfig.store.redis_url)
end.freeze

# TODO: Discuss with team if we need to enable this and use it anywhere
# REDIS_THROTTLE_POOL_SIZE = ConnectionPool.new(size: IdentityConfig.store.redis_throttle_pool_size) do
#   Redis.new(url: IdentityConfig.store.redis_throttle_url)
# end.freeze
