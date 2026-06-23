# frozen_string_literal: true

class QuicksightSyncJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin

  good_job_control_concurrency_with(
    perform_limit: 1,
  )

  def perform
    unless IdentityConfig.store.quicksight_sync_enabled
      logger.info(
        {
          name: 'QuicksightSyncJob',
          skipped: 'quicksight_sync_enabled is false',
        }.to_json,
      )
      return
    end

    QuicksightSync.new.sync

    logger.info(
      {
        name: 'QuicksightSyncJob',
        success: true,
      }.to_json,
    )
  rescue StandardError => e
    logger.error(
      {
        name: 'QuicksightSyncJob',
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
