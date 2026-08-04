# frozen_string_literal: true

class RedshiftSyncJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :admin # Requires superuser for CREATE USER, GRANT/REVOKE

  good_job_control_concurrency_with(
    perform_limit: 1,
  )

  DATABASES = ['analytics', 'analytics_zetl'].freeze

  def perform
    DATABASES.each do |database|
      RedshiftSync.new(database: database).sync

      logger.info(
        {
          name: 'RedshiftSyncJob',
          database: database,
          success: true,
        }.to_json,
      )
    end
  rescue StandardError => e
    logger.error(
      {
        name: 'RedshiftSyncJob',
        database: database,
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
