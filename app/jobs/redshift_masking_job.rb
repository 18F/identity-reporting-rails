# frozen_string_literal: true

class RedshiftMaskingJob < ApplicationJob
  queue_as :admin # Requires superuser for CREATE/ATTACH MASKING POLICY

  def perform(user_filter: nil)
    unless IdentityConfig.store.fraud_ops_tracker_enabled ||
           IdentityConfig.store.dw_fraudops_email_enabled
      Rails.logger.info('RedshiftMasking job is disabled, skipping')
      return
    end

    sync_services.each do |sync_service|
      Rails.logger.info("RedshiftMasking syncing with #{sync_service.class.name}")
      sync_service.sync(user_filter: user_filter)
    end
  end

  private

  # Masking policies are always synced against the data_warehouse database via
  # RedshiftMaskingSync. When zero-ETL is enabled, they are additionally synced
  # against the analytics_zetl database via RedshiftMaskingZetlSync.
  def sync_services
    services = [RedshiftMaskingSync.new]
    services << RedshiftMaskingZetlSync.new if IdentityConfig.store.zero_etl_enabled
    services
  end
end
