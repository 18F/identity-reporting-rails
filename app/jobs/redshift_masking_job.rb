# frozen_string_literal: true

class RedshiftMaskingJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin # Requires superuser for CREATE/ATTACH MASKING POLICY

  good_job_control_concurrency_with(
    perform_limit: 1,
  )

  def perform(user_filter: nil)
    unless IdentityConfig.store.fraud_ops_tracker_enabled ||
           IdentityConfig.store.dw_fraudops_email_enabled
      Rails.logger.info('RedshiftMasking job is disabled, skipping')
      return
    end

    RedshiftMaskingSync.new.sync_configured_columns(user_filter: user_filter)
  end
end
