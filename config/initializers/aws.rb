# frozen_string_literal: true

Aws.config.update(
  region: Identity::Hostdata.aws_region,
)
