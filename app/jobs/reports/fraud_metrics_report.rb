# frozen_string_literal: true

require 'csv'
require 'reporting/fraud_metrics_lg99_report'

module Reports
  class FraudMetricsReport < BaseReport
    REPORT_NAME = 'fraud-metrics-report'

    attr_reader :report_date

    def initialize(report_date = nil, *args, **rest)
      @report_date = report_date
      super(*args, **rest)
    end

    def perform(date = Time.zone.yesterday.end_of_day)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redhsift SIA V3 is disabled'
        return false
      end

      @report_date = date

      reports.each do |report|
        upload_to_s3(report[:table], report_name: report[:filename])
      end
    end

    def reports
      @reports ||= fraud_metrics_lg99_report.as_reports
    end

    def fraud_metrics_lg99_report
      @fraud_metrics_lg99_report ||= Reporting::FraudMetricsLg99Report.new(
        time_range: report_date.all_month,
      )
    end

    def upload_to_s3(report_body, report_name: nil)
      _latest, path = generate_s3_paths(
        REPORT_NAME, 'csv', directory: 'idp',
                            subname: report_name, now: report_date
      )

      if bucket_name.present?
        upload_file_to_s3_bucket(
          path: path,
          body: csv_file(report_body),
          content_type: 'text/csv',
          bucket: bucket_name,
        )
      end
    end

    def csv_file(report_array)
      CSV.generate do |csv|
        report_array.each do |row|
          csv << row
        end
      end
    end
  end
end
