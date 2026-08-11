# frozen_string_literal: true

# Keeps fraudops.frd_email_addresses_zetl in sync with the zero-ETL curated view.
class FraudOpsEmailAddressesZetlJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :default

  good_job_control_concurrency_with(
    total_limit: 1,
  )

  def perform(lookback_minutes: FraudOps::EmailAddressesZetlSync::DEFAULT_LOOKBACK_MINUTES)
    unless IdentityConfig.store.zero_etl_enabled
      return log_message(:info, 'zero_etl_enabled is false, skipping job.', false)
    end

    log_message(:info, 'Job started.', true, { lookback_minutes: lookback_minutes })

    result = FraudOps::EmailAddressesZetlSync.new.sync(lookback_minutes: lookback_minutes)

    log_message(:info, 'Job completed.', true, result)
  rescue => e
    log_message(:error, 'Job failed.', false, { error: e.message })
    raise
  end
end
