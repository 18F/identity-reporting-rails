# frozen_string_literal: true

require 'reporting/demographics_metrics_report'
require 'reporting/issuer_string_to_sp_id_helper'

module Reports
  class DemographicsMetricsReport < BaseReport
    include Reporting::IssuerStringToSpIdHelper

    REPORT_NAME = 'demographics-metrics-report'
    TIME_FRAME = 'quarterly' # Future options: 'monthly', etc.
    REPORT_DELAY_DAYS = 3 # 3 day lag to account for data sync delay

    attr_reader :report_date, :time_frame

    def initialize(report_date = nil, time_frame = TIME_FRAME, *args, **rest)
      @report_date = report_date || @report_date || REPORT_DELAY_DAYS.days.ago.end_of_day
      @time_frame = time_frame
      super(report_date, time_frame, *args, **rest)
    end

    def perform(report_date = nil, time_frame = nil)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end

      return unless IdentityConfig.store.s3_reports_enabled

      # Default to method argument, then constructor arguments, then default
      @report_date = report_date || @report_date || REPORT_DELAY_DAYS.days.ago.end_of_day
      @time_frame = time_frame || @time_frame || TIME_FRAME
      issuer_strings = report_configs

      Rails.logger.info "Starting demographics metrics report generation for"\
                        " #{issuer_strings.length} issuers with #{@time_frame} time frame"

      issuer_strings.each do |issuer_config|
        generate_and_upload_report_for_issuer(issuer_config)
      end

      Rails.logger.info 'Completed demographics metrics '\
                        'report generation for all issuers'
    end

    private

    def generate_and_upload_report_for_issuer(issuer_config)
      issuer_string = issuer_config['issuer_string']

      Rails.logger.info "Generating demographics report for issuer: #{issuer_string}"

      # Get service provider ID for this issuer
      sp_id = get_sp_id_for_issuer(issuer_string)
      unless sp_id
        Rails.logger.error "No service provider ID found for issuer: #{issuer_string}. Skipping."
        return
      end

      # Generate reports for this single issuer
      reports = demographics_reports_for_issuer(issuer_string)

      reports.each do |report|
        table = report.fetch(:table)
        filename = report.fetch(:filename)
        upload_to_s3(table, sp_id: sp_id, filename: filename)
      end

      Rails.logger.info "Completed demographics report for issuer: #{issuer_string}"
    rescue StandardError => err
      Rails.logger.error "Failed to generate demographics report for issuer #{issuer_string}:"\
                         " #{err.message}"
      raise err
    end

    def demographics_reports_for_issuer(issuer_string)
      Reporting::DemographicsMetricsReport.new(
        issuer_string: issuer_string,
        time_range: report_time_range,
      ).as_reports
    end

    def report_time_range
      case @time_frame
      when 'quarterly'
        @report_date.all_quarter
      when 'monthly'
        @report_date.all_month
      else
        raise ArgumentError, "Unsupported time frame: #{@time_frame}"
      end
    end

    def get_end_date_fp(time_range_obj)
      # We run this report monthly even though it's quarterly and send it to internal emails
      # For off-quarter months, we want the filename to indicate that data is in progress quarterly
      # I.e. report_date of Feb 27 will have 2026-01-01_2026_02-28 (for first quarter)

      # For the actual partner facing report run on April 2nd with report date of March 31,
      # the two expressions are equal - 2026-01-01_2026_03_31

      # This is specifically for quarterly data pulls which we send monthly, but the logic
      # should work for monthly reports as well if we choose to ever generate those
      raise ArgumentError, 'Report date cannot be in the future' if @report_date > Time.zone.today
      end_date = [@report_date.all_month.end, time_range_obj.end].min
      end_date.strftime('%Y%m%d')
    end

    def upload_to_s3(report_body, sp_id:, filename:)
      # Generate the S3 path using the new directory structure
      # DemographicsMetricsReport/{sp_id}/{time_frame}/SP{sp_id}_YYYYMMDD_YYYYMMDD_{filename}.csv
      time_range_obj = report_time_range
      start_date_fp = time_range_obj.begin.strftime('%Y%m%d')
      end_date_fp = get_end_date_fp(time_range_obj)

      # Use instance variable @time_frame instead of constant TIME_FRAME
      file_key = "DemographicsMetricsReport/#{sp_id}/"\
                "#{@time_frame}/SP#{sp_id}_#{start_date_fp}_#{end_date_fp}_#{filename}.csv"

      if bucket_name.present?
        upload_file_to_s3_bucket(
          path: file_key,
          body: csv_file(report_body),
          content_type: 'text/csv',
          bucket: bucket_name,
        )
        Rails.logger.info "Uploaded #{filename} to S3: #{file_key}"
      end
    end

    def report_configs
      # This should return an array of issuer configurations
      # Each config should have 'issuer_string' only
      IdentityConfig.store.demographics_metrics_report_configs
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
