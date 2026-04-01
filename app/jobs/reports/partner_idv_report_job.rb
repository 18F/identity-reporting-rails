# frozen_string_literal: true

require 'reporting/partner_idv_report'

module Reports
  class PartnerIdvReportJob < BaseReport
    REPORT_NAME = 'partner-idv-report'

    # @param [Time] report_date
    # @param [Integer] service_provider_id
    # @param [Integer] month_start_calendar_id
    def perform(
      report_date = Time.zone.yesterday.end_of_day,
      service_provider_id:,
      month_start_calendar_id:
    )
      report = build_report(
        service_provider_id: service_provider_id,
        month_start_calendar_id: month_start_calendar_id,
      )

      path = report_s3_path(report_date)

      upload_report(report, path: path)
    end

    private

    def build_report(service_provider_id:, month_start_calendar_id:)
      Reporting::PartnerIdvReport.new(
        service_provider_id: service_provider_id,
        month_start_calendar_id: month_start_calendar_id,
      )
    end

    def report_s3_path(report_date)
      _latest, path = generate_s3_paths(REPORT_NAME, 'json', now: report_date)
      path
    end

    def upload_report(report, path:)
      upload_file_to_s3_bucket(
        path: path,
        body: report.results_json,
        content_type: 'application/json',
        bucket: data_warehouse_bucket_name,
      )
    end

    def data_warehouse_bucket_name
      bucket_prefix = IdentityConfig.store.s3_data_warehouse_replica_bucket_prefix
      aws_account_id = Identity::Hostdata.aws_account_id
      aws_region = Identity::Hostdata.aws_region
      "#{bucket_prefix}-#{aws_account_id}-#{aws_region}"
    end
  end
end
