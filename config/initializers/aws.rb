# frozen_string_literal: true

Aws.config.update(
  region: IdentityConfig.store.aws_region,
)
