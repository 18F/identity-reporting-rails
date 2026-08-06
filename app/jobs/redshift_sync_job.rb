# frozen_string_literal: true

class RedshiftSyncJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin # Requires superuser for CREATE USER, GRANT/REVOKE

  good_job_control_concurrency_with(
    perform_limit: 1,
  )

  def perform
    RedshiftSync.new.sync

    logger.info(
      {
        name: 'RedshiftSyncJob',
        success: true,
      }.to_json,
    )
  rescue StandardError => e
    logger.error(
      {
        name: 'RedshiftSyncJob',
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
