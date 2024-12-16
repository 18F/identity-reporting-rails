class RedshiftUnloadLogCheckerJob < ApplicationJob
  queue_as :default

  def perform
    fetch_data_from_redshift
  end

  private

  def fetch_data_from_redshift
    build_params = {
      transfer_size_threshold: Identity::Hostdata.config.transfer_size_threshold,
    }

    query = format(<<~SQL, build_params)
      SELECT *
      FROM stl_unload_log
      WHERE transfer_size > %{transfer_size_threshold}
    SQL

    result = DataWarehouseApplicationRecord.connection.exec_query(query).to_a
    if result.present?
      log_info('RedshiftUnloadLogCheckerJob: Found unload logs above threshold', false)
    else
      log_info('RedshiftUnloadLogCheckerJob: No unload logs found above threshold', true)
    end
  end

  def log_info(message, success, additional_info = {})
    Rails.logger.info(
      {
        job: self.class.name,
        success: success,
        message: message,
      }.merge(additional_info).to_json,
    )
  end
end