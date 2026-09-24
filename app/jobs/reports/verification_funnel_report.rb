# frozen_string_literal: true

require 'reporting/verification_funnel_report'
require 'reporting/issuer_string_to_sp_id_helper'
require 'date'
require 'csv'

module Reports
  class VerificationFunnelReport < BaseReport
    include Reporting::IssuerStringToSpIdHelper

    REPORT_NAME = 'VerificationFunnelReport' # Used for S3 file paths

    # Log data has no modeling lag; 1-day look-back reports yesterday's period.
    DATA_LAG_DAYS = 0
    DEFAULT_LOOK_BACK_DAYS = 1

    VALID_TIME_FRAMES = %w[daily weekly monthly quarterly].freeze

    attr_reader :run_date, :days_back_for_time_period, :time_frame

    def initialize(init_run_date = Time.zone.now,
                   init_days_back_for_time_period = DEFAULT_LOOK_BACK_DAYS,
                   init_time_frame = 'monthly', *args, **rest)
      validate_parameters!(init_days_back_for_time_period, init_time_frame)
      assign_parameters(init_run_date, init_days_back_for_time_period, init_time_frame)
      super(init_run_date, init_days_back_for_time_period, init_time_frame, *args, **rest)
    end

    def perform(perform_run_date = nil, perform_days_back_for_time_period = nil,
                perform_time_frame = nil)
      final_run_date = perform_run_date || @run_date
      final_days_back = perform_days_back_for_time_period || @days_back_for_time_period
      final_time_frame = perform_time_frame || @time_frame
      validate_parameters!(final_days_back, final_time_frame)
      assign_parameters(final_run_date, final_days_back, final_time_frame)

      issuer_configs = report_configs
      if issuer_configs.nil? || issuer_configs.empty?
        Rails.logger.error 'verification_funnel_s3_report_configs is empty or nil - no work to do'
        raise ArgumentError, 'No issuer configurations found in'\
                             ' verification_funnel_s3_report_configs'
      end

      Rails.logger.info "Starting #{report_type}-facing #{@time_frame} verification funnel "\
                        "report generation for #{issuer_configs.length} issuers "\
                        "#{report_time_range.begin.to_date} to #{report_time_range.end.to_date}"

      failed_issuers = []
      issuer_configs.each do |issuer_config|
        begin
          generate_and_upload_report_for_issuer(issuer_config)
        rescue StandardError => err
          issuer_string = issuer_config['issuer_string']
          Rails.logger.error "Failed to generate verification funnel report for issuer"\
                             " #{issuer_string}: #{err.message}"
          failed_issuers << issuer_string
        end
      end

      if failed_issuers.any?
        Rails.logger.warn "Verification funnel report generation completed with "\
                          "#{failed_issuers.length} failures: #{failed_issuers.join(', ')}"
      else
        Rails.logger.info 'Completed verification funnel report generation'\
                          ' for all issuers successfully'
      end
    end

    private

    def assign_parameters(run_date, days_back, time_frame)
      @run_date = run_date || Time.zone.now
      @days_back_for_time_period = days_back || DEFAULT_LOOK_BACK_DAYS
      @time_frame = time_frame || 'monthly'
    end

    def validate_parameters!(days_back = @days_back_for_time_period,
                             time_frame = @time_frame)
      unless VALID_TIME_FRAMES.include?(time_frame)
        raise ArgumentError, "#{time_frame} time frame not supported - must be one of "\
                             "#{VALID_TIME_FRAMES.join(', ')}"
      end
      unless days_back.between?(0, 90)
        raise ArgumentError, "days_back_for_time_period must be between 0 and 90, "\
                            "got #{days_back}. Adjust run_date for periods "\
                            "greater than 90 days."
      end
    end

    def generate_and_upload_report_for_issuer(issuer_config)
      issuer_string = issuer_config['issuer_string']

      Rails.logger.info "Generating verification funnel report for issuer: #{issuer_string}"

      # Numeric service provider id for the S3 path (from IssuerStringToSpIdHelper).
      sp_id = get_sp_id_for_issuer(issuer_string)
      unless sp_id
        raise StandardError, "No service provider ID found for issuer: #{issuer_string}"
      end

      reports = funnel_reports_for_issuer(issuer_string)

      reports.each do |report|
        upload_to_s3(report.fetch(:table), sp_id: sp_id, filename: report.fetch(:filename))
      end

      Rails.logger.info "Completed verification funnel report for issuer: #{issuer_string}"
    end

    def funnel_reports_for_issuer(issuer_string)
      Reporting::VerificationFunnelReport.new(
        issuer_string: issuer_string,
        time_range: report_time_range,
      ).as_reports
    end

    # Currently assuming week boundary using Ruby, which may not be intended. Verify
    def report_time_range
      anchor = @run_date.prev_day(@days_back_for_time_period)

      case @time_frame
      when 'daily'
        anchor.all_day
      when 'weekly'
        # all_week(:sunday) snaps to the Sunday..Saturday week containing anchor.
        anchor.all_week(:sunday)
      when 'monthly'
        anchor.all_month
      when 'quarterly'
        anchor.all_quarter
      else
        raise ArgumentError, "Unsupported time frame: #{@time_frame}"
      end
    end

    #   Time period labels:
    #   daily     -> Jan01
    #   weekly    -> 20260104_20260110
    #   monthly   -> Jan2026
    #   quarterly -> Q12026
    # ------------------------------------------------------------------------
    def report_time_range_label
      range = report_time_range
      end_of_range = range.end

      case @time_frame
      when 'daily'
        "#{end_of_range.strftime('%b')}#{end_of_range.strftime('%d')}"
      when 'weekly'
        "#{range.begin.strftime('%Y%m%d')}_#{end_of_range.strftime('%Y%m%d')}"
      when 'monthly'
        "#{end_of_range.strftime('%b')}#{end_of_range.strftime('%Y')}"
      when 'quarterly'
        q_int = ((end_of_range.month - 1) / 3) + 1
        "Q#{q_int}#{end_of_range.strftime('%Y')}"
      else
        raise ArgumentError, "Unsupported time frame: #{@time_frame}"
      end
    end

    # External report means the time period has ended (adjusting for data lag)
    # True (external) once the period has fully ended (+ lag). DATA_LAG_DAYS is
    # 0 here because funnel data comes from logs with minimal modeling delay.
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

      files_to_upload = [fname_specific, fname_latest_internal]
      files_to_upload << fname_latest_external if external_report?

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
      # Array of issuer configs, each with 'issuer_string' only
      # example format: [{'issuer_string' => 'https://gsa.gov'}, {'issuer_string' => 'https://gsa2.gov'}]
      IdentityConfig.store.verification_funnel_s3_report_configs
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
