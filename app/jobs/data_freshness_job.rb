# DataFreshnessJob
#
# Checks the freshness of the most recent logs.production table against a predefined threshold.
# Logs whether the data is 'within_range' or 'out_of_range' based on how many hours have
# passed since the latest entry. If no logs are found, logs an error message.

class DataFreshnessJob < ApplicationJob
  queue_as :default

  def perform
    if latest_production_record
      log_data_freshness
    else
      log_error('No logs found!')
    end

    true
  end

  private

  def latest_production_record
    return @latest_production_record if defined?(@latest_production_record)

    @latest_production_record = Production.maximum(:cloudwatch_timestamp)
  end

  def log_data_freshness
    freshness_hours = ((Time.zone.now - latest_production_record) / 1.hour).round(2)
    limit = Identity::Hostdata.config.data_freshness_threshold_hours.hours.ago
    status = latest_production_record > limit ? 'within_range' : 'out_of_range'

    logger.info(
      {
        name: 'DataFreshnessJob',
        freshness_hours: freshness_hours,
        status: status,
      }.to_json,
    )
  end

  def log_error(message)
    logger.error(
      {
        name: 'DataFreshnessJob',
        error: message,
      }.to_json,
    )
  end

  def logger
    @logger ||= IdentityJobLogSubscriber.new.logger
  end
end
