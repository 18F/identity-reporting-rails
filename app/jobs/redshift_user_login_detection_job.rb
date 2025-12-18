# RedshiftUserLoginDetectionJob
#
# Checks if there are any user logins in Redshift for a set list of users in the last 15 minutes
# and writes a log entry for each unique user.
# The set list of users are defined in the `users_to_check` method.
#
# Usage:
#   RedshiftUserLoginDetectionJob.perform_later
#
require 'yaml'

class RedshiftUserLoginDetectionJob < ApplicationJob
  queue_as :default

  def perform
    log_user_logins
  rescue StandardError => e
    log_error(e.message)
  end

  private

  def using_redshift_adapter?
    DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
  end

  def users_to_check
    ['pii_reader']
  end

  def user_logins_detected_from_redshift
    query = <<~SQL
      SELECT DISTINCT user_name AS users
      FROM SYS_CONNECTION_LOG
      WHERE event = 'authenticated' 
      AND user_name IN (#{users_to_check.map { |s| "'#{s}'" }.join(", ")}) 
      AND record_time >= CURRENT_TIMESTAMP - INTERVAL '15 MINUTES';
    SQL
    result = DataWarehouseApplicationRecord.connection.execute(query)
    result.map(&:values).flatten
  end

  def log_user_logins
    user_logins_detected_from_redshift.each do |user|
      logger.info(
        {
          name: 'RedshiftUserLoginDetectionJob',
          detected_user: user,
        }.to_json,
      )
    end
  end

  def log_error(message)
    logger.error(
      {
        name: 'RedshiftUserLoginDetectionJob',
        error: message,
      }.to_json,
    )
  end

  def logger
    @logger ||= IdentityJobLogSubscriber.new.logger
  end
end
