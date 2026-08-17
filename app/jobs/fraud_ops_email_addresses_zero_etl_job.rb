# frozen_string_literal: true

# Keeps fraudops.frd_email_addresses_zero_etl in sync with the zero-ETL curated view.
class FraudOpsEmailAddressesZeroEtlJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin

  good_job_control_concurrency_with(
    total_limit: 1,
  )

  def perform
    unless IdentityConfig.store.idp_zero_etl_enabled
      return log_message(:info, 'idp_zero_etl_enabled is false, skipping job.', false)
    end

    log_message(:info, 'Job started.', true)

    result = FraudOps::EmailAddressesZeroEtlSync.new.sync

    log_message(:info, 'Job completed.', true, result)
  rescue => e
    log_message(:error, 'Job failed.', false, { error: e.message })
    raise
  end
end
