# frozen_string_literal: true

require 'reporting/identity_verification_report'
require 'csv'

module Reports
  class MonthlyKeyMetricsIdvS3Report < BaseReport
    REPORT_NAME = 'MonthlyKeyMetricsIdvS3Report'

    CONDENSED_IDV_FILENAME = 'condensed_idv'
    PROOFING_RATE_FILENAME = 'proofing_rate_metrics'

    attr_reader :report_date

    def initialize(init_report_date = nil, *args, **rest)
      @report_date = init_report_date
      super(init_report_date, *args, **rest)
    end

    def perform(perform_report_date = nil)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn "#{REPORT_NAME}: Redshift SIA V3 is disabled"
        return false
      end

      unless IdentityConfig.store.s3_reports_enabled
        Rails.logger.warn "#{REPORT_NAME}: S3 reports are disabled"
        return false
      end

      @report_date = perform_report_date || report_date || Time.zone.yesterday

      allowed_types = [Date, Time, ActiveSupport::TimeWithZone]
      unless allowed_types.any? { |type| @report_date.is_a?(type) }
        raise ArgumentError, 'report_date must be a valid Date or Time object'
      end
      if @report_date.to_date > Time.zone.today
        raise ArgumentError, 'report_date cannot be in the future'
      end

      # Normalize to the calendar date, anchored in UTC — avoids end-of-day
      # local timestamps rolling into the next UTC day.
      @report_date = @report_date.to_date.in_time_zone('UTC')

      Rails.logger.info(
        "#{REPORT_NAME}: generating IDV key metrics reports for #{report_date.to_date}",
      )

      upload_to_s3(
        condensed_idv_table,
        filename: CONDENSED_IDV_FILENAME,
      )

      upload_to_s3(
        proofing_rate_table,
        filename: PROOFING_RATE_FILENAME,
      )

      Rails.logger.info("#{REPORT_NAME}: finished uploading reports to S3")
    end

    private

    def condensed_idv_table
      report = monthly_idv_report

      [
        ['Metric', report.time_range.begin.strftime('%b %Y')],
        ['IDV started', report.idv_started],
        ['# of successfully verified users', report.successfully_verified_users],
        ['% IDV started to successfully verified', report.blanket_proofing_rate],
        ['# of workflow completed', report.idv_final_resolution],
        ['% rate of workflow completed', report.idv_final_resolution_rate],
        ['# of users verified (total)', report.verified_user_count],
      ]
    end

    def proofing_rate_table
      report = trailing_30_day_report

      [
        ['Metric', 'Trailing 30d'],
        ['Start Date', report.time_range.begin.to_date],
        ['End Date', report.time_range.end.to_date],
        ['IDV Started', report.idv_started],
        ['Welcome Submitted', report.idv_doc_auth_welcome_submitted],
        ['Image Submitted', report.idv_doc_auth_image_vendor_submitted],
        ['Socure', report.idv_doc_auth_socure_verification_data_requested],
        ['Successfully Verified', report.successfully_verified_users],
        ['IDV Rejected (Non-Fraud)', report.idv_doc_auth_rejected],
        ['IDV Rejected (Fraud)', report.idv_fraud_rejected],
        [
          'Blanket Proofing Rate (IDV Started to Successfully Verified)',
          report.blanket_proofing_rate,
        ],
        [
          'Intent Proofing Rate (Welcome Submitted to Successfully Verified)',
          report.intent_proofing_rate,
        ],
        [
          'Actual Proofing Rate (Image Submitted to Successfully Verified)',
          report.actual_proofing_rate,
        ],
        [
          'Industry Proofing Rate (Verified minus IDV Rejected)',
          report.industry_proofing_rate,
        ],
      ]
    end

    def monthly_idv_report
      @monthly_idv_report ||= Reporting::IdentityVerificationReport.new(
        time_range: monthly_range,
      )
    end

    def trailing_30_day_report
      @trailing_30_day_report ||= Reporting::IdentityVerificationReport.new(
        time_range: trailing_30_day_range,
      )
    end

    def monthly_range
      Range.new(
        report_date.beginning_of_month.beginning_of_day,
        report_date.end_of_month.end_of_day,
      )
    end

    def trailing_30_day_range
      Range.new(
        (report_date - 30.days).beginning_of_day,
        report_date.end_of_day,
      )
    end

    def upload_to_s3(report_body, filename:)
      unless bucket_name.present?
        Rails.logger.warn "#{REPORT_NAME}: bucket_name is blank, skipping upload"
        return
      end

      paths_for(filename).each do |path|
        upload_file_to_s3_bucket(
          path: path,
          body: csv_file(report_body),
          content_type: 'text/csv',
          bucket: bucket_name,
        )

        Rails.logger.info "#{REPORT_NAME}: uploaded #{filename} to #{path}"
      end
    end

    def paths_for(filename)
      report_day = report_date.to_date
      date_prefix = report_day.strftime('%Y%m%d')
      year = report_day.strftime('%Y')
      month = report_day.strftime('%m')

      base_path = "#{generate_base_s3_path(directory: 'idp')}#{REPORT_NAME}/#{year}/#{month}/"

      [
        "#{base_path}#{date_prefix}_monthly_#{filename}.csv",
      ]
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
