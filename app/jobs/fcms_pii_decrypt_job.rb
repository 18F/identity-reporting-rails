class RedshiftUnloadLogCheckerJob < ApplicationJob
  queue_as :default

  def perform
    fetch_data_from_redshift
  end

  private

  def fetch_data_from_redshift
    build_params = {
      line_count_threshold: Identity::Hostdata.config.unload_line_count_threshold,
      five_minutes_ago: 5.minutes.ago.utc.strftime('%Y-%m-%d %H:%M'),
    }
    query = format(<<~SQL, build_params)
      SELECT userid, start_time, end_time, line_count, path as s3_path, query as query_id
      FROM stl_unload_log
      WHERE line_count > %{line_count_threshold}
        AND start_time > '%{five_minutes_ago}'
    SQL

    result = DataWarehouseApplicationRecord.connection.exec_query(query).to_a
    if result.present?
      merge_result = result.map do |row|
        {
          userid: row['userid'],
          starttime: row['start_time'].strftime('%Y-%m-%d %H:%M'),
          endtime: row['end_time'].strftime('%Y-%m-%d %H:%M'),
          s3_path: row['s3_path'],
          query_id: row['query_id'],
          line_count: row['line_count'],
        }
      end
      log_info(
        "RedshiftUnloadLogCheckerJob: Found #{result.count} unload logs above threshold", false,
        data: merge_result
      )
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
