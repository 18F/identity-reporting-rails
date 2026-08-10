# frozen_string_literal: true

require 'zetl_binding_view_sync'

class ZetlBindingViewSyncJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin # Requires privileges to CREATE SCHEMA / CREATE VIEW

  good_job_control_concurrency_with(
    perform_limit: 1,
  )

  def perform
    unless IdentityConfig.store.zero_etl_enabled
      logger.info(
        {
          name: 'ZetlBindingViewSyncJob',
          skipped: 'zero_etl_enabled is false',
        }.to_json,
      )
      return
    end

    results = ZetlBindingViewSync.new.sync

    logger.info(
      {
        name: 'ZetlBindingViewSyncJob',
        success: true,
        message: 'ZETL binding view sync completed successfully',
        results: results,
      }.to_json,
    )
  rescue StandardError => e
    logger.error(
      {
        name: 'ZetlBindingViewSyncJob',
        error: e.message,
      }.to_json,
    )
    raise
  end

  private

  def logger
    @logger ||= IdentityJobLogSubscriber.new.logger
  end
end
