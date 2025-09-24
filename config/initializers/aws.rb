# frozen_string_literal: true

Aws.config.update(
  region: Identity::Hostdata.aws_region,
  http_open_timeout: IdentityConfig.store.aws_http_timeout.to_f,
  http_read_timeout: IdentityConfig.store.aws_http_timeout.to_f,
  retry_limit: IdentityConfig.store.aws_http_retry_limit,
  retry_max_delay: IdentityConfig.store.aws_http_retry_max_delay
)
