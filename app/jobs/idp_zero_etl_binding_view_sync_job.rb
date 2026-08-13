# frozen_string_literal: true

require 'idp_zero_etl_binding_view_sync'

class IdpZeroEtlBindingViewSyncJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin # Requires privileges to CREATE SCHEMA / CREATE VIEW

  good_job_control_concurrency_with(
    perform_limit: 1,
  )

  def perform
    unless IdentityConfig.store.idp_zero_etl_enabled
      logger.info(
        {
          name: 'IdpZeroEtlBindingViewSyncJob',
          skipped: 'idp_zero_etl_enabled is false',
        }.to_json,
      )
      return
    end

    results = ZetlBindingViewSync.new.sync

    logger.info(
      {
        name: 'IdpZeroEtlBindingViewSyncJob',
        success: true,
        message: 'ZETL binding view sync completed successfully',
        results: results,
      }.to_json,
    )
  rescue StandardError => e
    logger.error(
      {
        name: 'IdpZeroEtlBindingViewSyncJob',
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
