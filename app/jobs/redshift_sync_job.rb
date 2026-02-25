# frozen_string_literal: true

class RedshiftSyncJob < ApplicationJob
  queue_as :default

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
