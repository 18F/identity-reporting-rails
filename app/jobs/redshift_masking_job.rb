# frozen_string_literal: true

class RedshiftMaskingJob < ApplicationJob
  queue_as :default

  def perform(user_filter: nil)
    unless IdentityConfig.store.fraud_ops_tracker_enabled
      Rails.logger.info('RedshiftMasking job is disabled, skipping')
      return
    end

    RedshiftMaskingSync.new.sync(user_filter: user_filter)
  end
end
