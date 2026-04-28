# frozen_string_literal: true

require 'reporting/partner_report_default'

module Reports
  class PartnerReportDefault < BaseReport
    REPORT_NAME = 'partner-default-report'
    REPORT_CADENCE = 'monthly' # Eventually this will be a parameter, hardcoding monthly for now

    # Default report date is 3 days ago
    # This is because event data is ingested into the warehouse 1x/day and transforms
    # run 1x/day, there is an up to 2 day lag for marts data
    REPORT_DELAY_DAYS = 3

    attr_reader :report_date

    def initialize(report_date = nil, *args, **rest)
      @report_date = report_date
      super(report_date, *args, **rest)
    end

    def perform(date = REPORT_DELAY_DAYS.days.ago.end_of_day)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end
      return unless IdentityConfig.store.s3_reports_enabled

      @report_date = date

      Rails.logger.info "Generating partner default #{REPORT_CADENCE} reports for report date: "\
                        "#{report_date} (#{REPORT_CADENCE} report period starting on #{period_date}"

      generate_and_upload_reports(report_date)
      Rails.logger.info "Completed partner default #{REPORT_CADENCE} report"
    end

    private

    def generate_and_upload_reports(report_date)
      issuer_reports = partner_reports(report_date)
      uploaded_count = 0
      skipped_count = 0

      issuer_reports.each do |issuer, json_data|
        if json_data.nil?
          Rails.logger.warn "Skipping upload for #{issuer}: report generation "\
                            "failed and returned nil"
          skipped_count += 1
          next
        end

        upload_to_s3(json_data, issuer: issuer, period_date: period_date)
        uploaded_count += 1
      end

      Rails.logger.info "Upload summary: #{uploaded_count} successful, #{skipped_count} skipped"
    rescue StandardError => err
      Rails.logger.error "Failed to generate partner default "\
                         "#{REPORT_CADENCE} reports: #{err.message}"
      raise err
    end

    def partner_reports(report_date)
      Reporting::PartnerReportDefault.new(
        report_date: report_date,
        report_cadence: REPORT_CADENCE,
      ).generate_reports
    end

    def upload_to_s3(json_data, issuer:, period_date:)
      # S3 path structure: issuer/REPORT_CADENCE/2025-01-01.json
      base_path = generate_base_s3_path(directory: 'portal')
      path = "#{base_path}#{issuer}/#{REPORT_CADENCE}/#{period_date.strftime('%Y-%m-%d')}.json"

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

    def period_date
      @period_date ||= begin
        Reporting::PartnerReportDefault.get_period_date_from_report_date(
          REPORT_CADENCE,
          report_date,
        )
      end
    end
  end
end
