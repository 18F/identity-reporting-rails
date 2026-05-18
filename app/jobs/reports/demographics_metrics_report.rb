# frozen_string_literal: true

require 'reporting/demographics_metrics_report'
require 'reporting/issuer_string_to_sp_id_helper'
require 'date'

module Reports
  class DemographicsMetricsReport < BaseReport
    include Reporting::IssuerStringToSpIdHelper

    REPORT_NAME = 'demographics-metrics-report'
    DATA_LAG_DAYS = 2 # 2 day lag to account for data sync delay into DW

    attr_reader :run_date, :days_back_for_time_period, :time_frame

    def initialize(init_run_date = Time.zone.now, init_days_back_for_time_period = 4,
                   init_time_frame = 'quarterly', *args, **rest)
      @run_date = init_run_date
      @days_back_for_time_period = init_days_back_for_time_period
      @time_frame = init_time_frame
      super(init_run_date, init_days_back_for_time_period, init_time_frame, *args, **rest)
    end

    def perform(perform_run_date = nil, perform_days_back_for_time_period = nil,
                perform_time_frame = nil)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end

      return unless IdentityConfig.store.s3_reports_enabled

      # Default to method argument, then constructor arguments, then default
      @run_date = perform_run_date || @run_date || Time.zone.now
      @days_back_for_time_period = perform_days_back_for_time_period ||
                                   @days_back_for_time_period ||
                                   4
      @time_frame = perform_time_frame || @time_frame || 'quarterly'

      raise ArgumentError, "#{@time_frame} is not a valid time frame - must be 'quarterly'"\
                           unless @time_frame == 'quarterly'
      unless @days_back_for_time_period.between?(0, 90)
        raise ArgumentError, "days_back_for_time_period must be between 0 and 90, "\
                            "got #{@days_back_for_time_period}. Adjust run_date for periods "\
                            "greater than 90 days."
      end

      issuer_strings = report_configs

      Rails.logger.info "Starting #{report_type}-facing #{@time_frame} demographics metrics "\
                        "report generation for #{issuer_strings.length} issuers"

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
        @run_date.prev_day(@days_back_for_time_period).all_quarter
      when 'monthly'
        @run_date.prev_day(@days_back_for_time_period).all_month
      when 'daily'
        @run_date.prev_day(@days_back_for_time_period).all_day
      else
        raise ArgumentError, "Unsupported time frame: #{@time_frame}"
      end
    end

    def report_time_range_label
      end_of_range = report_time_range.end
      case @time_frame
      when 'quarterly'
        # Q1
        q_int = ((end_of_range.month - 1) / 3) + 1
        label_start = "Q#{q_int}"
      when 'monthly'
        # Jan
        label_start = end_of_range.strftime('%b')
      when 'daily'
        # Jan01
        label_start = "#{end_of_range.strftime('%b')}"\
                      "#{end_of_range.strftime('%d')}"
      else
        raise ArgumentError, "Unsupported time frame: #{@time_frame}"
      end
      # Q12026, Jan2026, Jan012026
      "#{label_start}#{end_of_range.strftime('%Y')}"
    end

    # True (External) when quarter has ended + lag has passed
    # I.e. True if March 31st <= April 6th - DATA_LAG_DAYS
    def is_external_report
      report_time_range.end.to_date <= Date.current - DATA_LAG_DAYS.days
    end

    def report_type
      is_external_report ? 'external' : 'internal'
    end

    def upload_to_s3(report_body, sp_id:, filename:)
      now_date_fp = Time.zone.now.strftime('%Y%m%d')

      fname_specific = "SP#{sp_id}_#{now_date_fp}_#{report_type}_#{filename}.csv"
      fname_latest_internal = "latest_SP#{sp_id}_#{filename}.csv"
      fname_latest_external = "latest_external_SP#{sp_id}_#{filename}.csv"

      # Determine which files to upload
      files_to_upload = [fname_specific]

      # Always update latest internal
      files_to_upload << fname_latest_internal

      # Only update latest external if this is an external report
      files_to_upload << fname_latest_external if is_external_report

      # Generate base path
      bucket_idp_path = generate_base_s3_path(directory: 'idp')
      base_directory = "#{bucket_idp_path}DemographicsMetricsReport/#{sp_id}/"\
                      "#{@time_frame.downcase}/#{report_time_range_label}/"

      files_to_upload.each do |filename|
        full_path = "#{base_directory}#{filename}"

        if bucket_name.present?
          upload_file_to_s3_bucket(
            path: full_path,
            body: csv_file(report_body),
            content_type: 'text/csv',
            bucket: bucket_name,
          )
          Rails.logger.info "Uploaded #{filename} to S3: #{full_path}"
        end
      end
    end

    def report_configs
      # This should return an array of issuer configurations
      # Each config should have 'issuer_string' only
      IdentityConfig.store.demographics_metrics_s3_report_configs
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
