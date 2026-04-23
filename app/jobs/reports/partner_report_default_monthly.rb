# frozen_string_literal: true

require 'reporting/partner_report_default_monthly'

module Reports
  class PartnerDefaultReportMonthly < BaseReport
    REPORT_NAME = 'partner-default-report-monthly'

    attr_reader :report_date

    def initialize(report_date = nil, *args, **rest)
      @report_date = report_date
      super(report_date, *args, **rest)
    end

    def perform(date = 3.days.ago.end_of_day)
      # Default date is 3 days ago, as this job should run on the third day of a month
      # Why? Since event data is ingested into the warehouse 1x/day and transforms
      #  run 1x/day, there is an up to 2 day lag for completed month data.
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end

      return unless IdentityConfig.store.s3_reports_enabled

      @report_date = date
      time_range = report_date.all_month

      Rails.logger.info "Generating partner default monthly reports for " \
                        " #{time_range.begin.strftime('%B %Y')}"

      generate_and_upload_reports(time_range)

      Rails.logger.info 'Completed partner default monthly report'
    end

    private

    def generate_and_upload_reports(time_range)
      nested_reports = partner_reports(time_range)

      nested_reports.each do |issuer, monthly_data|
        Rails.logger.info "Processing reports for issuer: #{issuer}"

        monthly_data.each do |month_start_date, json_data|
          upload_to_s3(json_data, issuer: issuer, month: month_start_date)
        end
      end
    rescue StandardError => err
      Rails.logger.error "Failed to generate partner default monthly reports: #{err.message}"
      raise err
    end

    def partner_reports(time_range)
      Reporting::PartnerReportDefaultMonthly.new(
        time_range: time_range,
      ).generate_reports
    end

    def upload_to_s3(json_data, issuer:, month:)
      # S3 path structure: issuer/monthly/2025-01-01.json
      path = "#{issuer}/monthly/#{month}.json"

      if bucket_name.present?
        upload_file_to_s3_bucket(
          path: path,
          body: json_file(json_data),
          content_type: 'application/json',
          bucket: bucket_name,
        )
        Rails.logger.info "Uploaded partner report to S3: #{path}"
      end
    end

    def json_file(data)
      JSON.pretty_generate(data)
    end
  end
end
