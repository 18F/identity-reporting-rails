# frozen_string_literal: true

class RedshiftDbtMaskingJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin

  good_job_control_concurrency_with(perform_limit: 1)

  def perform
    unless IdentityConfig.store.fraud_ops_tracker_enabled ||
           IdentityConfig.store.dw_fraudops_email_enabled
      Rails.logger.info('Redshift dbt masking job is disabled, skipping')
      return
    end

    sync = RedshiftMaskingSync.new
    RedshiftMasking::DbtConfiguration.new.schemas.each do |schema|
      sync.sync_dbt_schema(schema: schema)
    end
  end
end
