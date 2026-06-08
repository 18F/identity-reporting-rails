# frozen_string_literal: true

require 'reporting/demographics_metrics_report'
require 'reporting/issuer_string_to_sp_id_helper'
require 'date'
require 'csv'

module Reports
  class DemographicsMetricsReport < BaseReport
    include Reporting::IssuerStringToSpIdHelper

    # Demographics report pull from logs data, which should replicate very quickly
    # into data warehouse. Thus, there is no data lag to account for and look back
    # days are 2
    REPORT_NAME = 'DemographicsMetricsReport' # Used for S3 file paths
    DATA_LAG_DAYS = 0 # 0 day lag to account for no data modeling delay into DW - log data
    DEFAULT_LOOK_BACK_DAYS = 4 # Cron job runs on 1st, looks back 2 days
    SCHEMA_CUTOFF_DATE = Date.new(2025, 10, 1).freeze
    attr_reader :run_date, :days_back_for_time_period, :time_frame

    def initialize(init_run_date = Time.zone.now,
                   init_days_back_for_time_period = DEFAULT_LOOK_BACK_DAYS,
                   init_time_frame = 'quarterly', *args, **rest)
      validate_parameters!(init_run_date, init_days_back_for_time_period, init_time_frame)
      assign_parameters(init_run_date, init_days_back_for_time_period, init_time_frame)
      super(init_run_date, init_days_back_for_time_period, init_time_frame, *args, **rest)
    end

    def perform(perform_run_date = nil, perform_days_back_for_time_period = nil,
                perform_time_frame = nil)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end

      return unless IdentityConfig.store.s3_reports_enabled

      final_run_date = perform_run_date || @run_date
      final_days_back = perform_days_back_for_time_period || @days_back_for_time_period
      final_time_frame = perform_time_frame || @time_frame
      validate_parameters!(final_run_date, final_days_back, final_time_frame)
      assign_parameters(final_run_date, final_days_back, final_time_frame)

      issuer_strings = report_configs
      if issuer_strings.nil? || issuer_strings.empty?
        Rails.logger.error 'demographics_metrics_s3_report_configs is empty or nil - no work to do'
        raise ArgumentError, 'No issuer configurations found in'\
                             ' demographics_metrics_s3_report_configs'
      end
      Rails.logger.info "Starting #{report_type}-facing #{@time_frame} demographics metrics "\
                        "report generation for #{issuer_strings.length} issuers "\
                        "#{report_time_range.begin.to_date} to #{report_time_range.end.to_date}"

      failed_issuers = []
      issuer_strings.each do |issuer_config|
        begin
          generate_and_upload_report_for_issuer(issuer_config)
        rescue StandardError => err
          issuer_string = issuer_config['issuer_string']
          Rails.logger.error "Failed to generate demographics report for issuer"\
                             " #{issuer_string}: #{err.message}"
          failed_issuers << issuer_string
        end
      end

      if failed_issuers.any?
        Rails.logger.warn "Demographics report generation completed with #{failed_issuers.length}"\
                          " failures: #{failed_issuers.join(', ')}"
      else
        Rails.logger.info 'Completed demographics metrics report generation'\
                          ' for all issuers successfully'
      end
    end

    private

    def assign_parameters(run_date, days_back, time_frame)
      @run_date = run_date || Time.zone.now
      @days_back_for_time_period = days_back || DEFAULT_LOOK_BACK_DAYS
      @time_frame = time_frame || 'quarterly'
    end

    def validate_parameters!(run_date = @run_date, days_back = @days_back_for_time_period,
                             time_frame = @time_frame)
      unless time_frame == 'quarterly'
        raise ArgumentError, "#{time_frame} time frame not yet implemented - must be 'quarterly'"
      end
      unless days_back.between?(0, 90)
        raise ArgumentError, "days_back_for_time_period must be between 0 and 90, "\
                            "got #{days_back}. Adjust run_date for periods "\
                            "greater than 90 days."
      end
      if run_date.to_date < SCHEMA_CUTOFF_DATE
        Rails.logger.warn "Running demographics report for #{run_date.to_date}, which is before "\
                          "#{SCHEMA_CUTOFF_DATE}. App log data schema assumptions may be invalid "\
                          "before this date."
      end
    end

    def generate_and_upload_report_for_issuer(issuer_config)
      issuer_string = issuer_config['issuer_string']

      Rails.logger.info "Generating demographics report for issuer: #{issuer_string}"

      # Get service provider ID for this issuer (from IssuerStringToSpIdHelper)
      sp_id = get_sp_id_for_issuer(issuer_string)
      unless sp_id
        raise StandardError, "No service provider ID found for issuer: #{issuer_string}"
      end

      # Generate reports for this single issuer
      reports = demographics_reports_for_issuer(issuer_string)

      reports.each do |report|
        table = report.fetch(:table)
        filename = report.fetch(:filename)
        upload_to_s3(table, sp_id: sp_id, filename: filename)
      end

      Rails.logger.info "Completed demographics report for issuer: #{issuer_string}"
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
        raise NotImplementedError, 'Monthly reporting is not yet implemented'
        # @run_date.prev_day(@days_back_for_time_period).all_month
      when 'daily'
        raise NotImplementedError, 'Daily reporting is not yet implemented'
        # @run_date.prev_day(@days_back_for_time_period).all_day
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
    # I.e. True if March 31st < April 6th - DATA_LAG_DAYS
    # Note, as mentioned before, data_lag_days is irrelevant in this report
    # because we are pulling log data with minimal data modeling delay
    def external_report?
      report_time_range.end.to_date < Date.current - DATA_LAG_DAYS.days
    end

    def report_type
      external_report? ? 'external' : 'internal'
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
      files_to_upload << fname_latest_external if external_report?

      # Generate base path
      bucket_idp_path = generate_base_s3_path(directory: 'idp')
      base_directory = "#{bucket_idp_path}#{REPORT_NAME}/#{sp_id}/"\
                      "#{@time_frame.downcase}/#{report_time_range_label}/"

      files_to_upload.each do |generated_filename|
        full_path = "#{base_directory}#{generated_filename}"

        if bucket_name.present?
          upload_file_to_s3_bucket(
            path: full_path,
            body: csv_file(report_body),
            content_type: 'text/csv',
            bucket: bucket_name,
          )
          Rails.logger.info "Uploaded #{generated_filename} to S3: #{full_path}"
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
