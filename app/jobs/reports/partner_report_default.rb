# frozen_string_literal: true

require 'reporting/partner_report_default'
require 'reporting/partner_report_default_v2'

module Reports
  class PartnerReportDefault < BaseReport
    REPORT_CADENCE = 'monthly' # Eventually this will be a parameter, hardcoding monthly for now
    DEFAULT_VERSION = 'v2'
    VALID_VERSIONS = %w[v1 v2].freeze

    # A single job that wraps two versions of partner report (will eventually delete v1)
    # Runs v2 by default

    # Usage:
    #   # CRON JOB
    #   Reports::PartnerReportDefault.perform_now(Date.parse('2026-03-15'), 'v2')
    #
    #   # MANUAL RUN
    #   Reports::PartnerReportDefault.perform_now(
    #     Date.parse('2026-03-15'), 'v1',
    #     { included_issuers: ['issuer1', 'issuer2'] },
    #   )

    # Default report date is 4 days ago
    # This is because event data is ingested into the warehouse 1x/day and transforms
    # run 1x/day, there is an up to 2 day lag for marts data
    # We assume this job runs on the 3rd of the month for Monthly reports
    REPORT_DELAY_DAYS = 4

    attr_reader :report_date, :included_issuers, :excluded_issuers, :report_version

    def perform(report_date = nil, report_version = nil, options = {})
      options ||= {}

      @report_version = report_version || DEFAULT_VERSION
      unless VALID_VERSIONS.include?(@report_version)
        raise ArgumentError,
              "Invalid report_version: #{@report_version}. "\
              "Must be one of: #{VALID_VERSIONS.join(', ')}"
      end

      @included_issuers = normalize_issuer_list(options[:included_issuers])
      @excluded_issuers = normalize_issuer_list(options[:excluded_issuers])
      if @included_issuers&.any? && @excluded_issuers&.any?
        raise ArgumentError, 'Cannot specify both included_issuers and excluded_issuers'
      end

      unless IdentityConfig.store.redshift_sia_v3_enabled
        log_message(:warn, 'Redshift SIA V3 is disabled', false, log_context)
        return false
      end
      unless IdentityConfig.store.s3_reports_enabled
        log_message(:warn, 'S3 reports are disabled', false, log_context)
        return false
      end

      @report_date = report_date || REPORT_DELAY_DAYS.days.ago.end_of_day

      log_message(
        :info,
        'Generating partner default reports',
        true,
        log_context(period_date: period_date),
      )

      # Wait 10 seconds to allow user to cancel job if they realize they ran it with the wrong date
      # This hopefully prevents accidental population of reports manually for wrong dates
      log_message(
        :info,
        'Waiting before generating reports so manual runs can be canceled if needed',
        true,
        log_context(wait_seconds: 10),
      )
      sleep 10

      if @included_issuers&.any?
        log_message(
          :info,
          'Filtering partner default report to included issuers',
          true,
          log_context(included_issuers: @included_issuers),
        )
      elsif @excluded_issuers&.any?
        log_message(
          :info,
          'Filtering partner default report to exclude issuers',
          true,
          log_context(excluded_issuers: @excluded_issuers),
        )
      end

      generate_and_upload_reports(@report_date)
      log_message(:info, 'Completed partner default report', true, log_context)

      true
    rescue StandardError => e
      log_message(
        :error,
        'Failed to generate partner default reports',
        false,
        log_context(error: e.message, error_class: e.class.name),
      )
      raise
    end

    private

    def log_context(extra = {})
      {
        job_id: job_id,
        report_cadence: REPORT_CADENCE,
        report_version: @report_version,
        report_date: @report_date&.to_date&.to_s,
      }.merge(extra)
    end

    def generate_and_upload_reports(report_date)
      reporter_class = case @report_version
                       when 'v1'
                         Reporting::PartnerReportDefault
                       when 'v2'
                         Reporting::PartnerReportDefaultV2
                       else
                         raise ArgumentError, "Unsupported report_version: #{@report_version}"
                       end

      reporter = reporter_class.new(
        report_date: report_date,
        report_cadence: REPORT_CADENCE,
        included_issuers: @included_issuers,
        excluded_issuers: @excluded_issuers,
      )
      # Generate issuer mapping to link IDs and issuer strings
      issuer_mapping = reporter.generate_issuer_mapping
      issuer_reports = reporter.generate_reports

      # If included issuers specified, check they were in SQL results
      if @included_issuers&.any?
        missing = @included_issuers - issuer_reports.keys
        if missing.any?
          log_message(
            :warn,
            'Requested issuers not found in partner default report data',
            false,
            log_context(missing_issuers: missing),
          )
        end
      end

      # Check that all service_provider_ids exist in mapping (just in case, warn if any missing)
      validate_service_provider_ids(issuer_reports, issuer_mapping)

      upload_issuer_mapping_to_s3(issuer_mapping)

      # Upload individual reports
      uploaded_count = 0
      skipped_uploads = []

      issuer_reports.each do |issuer, json_data|
        if json_data.nil?
          skipped_uploads << "#{issuer}: report generation failed and returned nil"
          next
        end

        service_provider_id = json_data.dig(:provider_information, :service_provider_id)

        if service_provider_id.nil?
          skipped_uploads << "#{issuer}: missing service_provider_id"
          next
        end

        begin
          upload_to_s3(
            json_data,
            service_provider_id: service_provider_id,
            period_date: period_date,
          )
          uploaded_count += 1
        rescue StandardError => e
          skipped_uploads << "#{issuer} / #{service_provider_id}: upload failed - #{e.message}"
        end
      end

      log_message(
        :info,
        'Partner default report upload summary',
        true,
        log_context(uploaded_count: uploaded_count, skipped_upload_count: skipped_uploads.count),
      )

      if skipped_uploads.any?
        log_message(
          :warn,
          'Skipped partner default report uploads',
          false,
          log_context(skipped_uploads: skipped_uploads),
        )
      end

      if uploaded_count.zero?
        raise StandardError, 'Partner default report failed because no issuer reports were uploaded'
      end
    end

    # S3 path structure:
    # v1: env/v1/service_provider_id/REPORT_CADENCE/2025-01-01.json
    # v2: env/v2/service_provider_id/REPORT_CADENCE/2025-01-01.json
    def upload_to_s3(json_data, service_provider_id:, period_date:)
      base_path = generate_base_s3_path(directory: 'portal')

      path = case @report_version
             when 'v1', 'v2'
               "#{base_path}#{@report_version}/#{service_provider_id}"\
                 "/#{REPORT_CADENCE}/#{period_date}.json"
             else
               raise ArgumentError, "Unsupported report_version: #{@report_version}"
             end

      return unless bucket_name.present?

      upload_file_to_s3_bucket(
        path: path,
        body: json_file(json_data),
        content_type: 'application/json',
        bucket: bucket_name,
      )
    end

    def normalize_issuer_list(issuers)
      return nil if issuers.nil?
      issuers = [issuers] if issuers.is_a?(String)
      unless issuers.is_a?(Array)
        raise ArgumentError, "Issuers must be a string or array of strings, got #{issuers.class}"
      end
      # Validate all elements are strings and remove empty/nil values
      validated = issuers.compact.map(&:to_s).reject(&:empty?)
      validated.empty? ? nil : validated
    end

    def validate_service_provider_ids(issuer_reports, issuer_mapping)
      mapping_ids = issuer_mapping.values.map { |v| v[:id] }.to_set

      issuer_reports.each do |issuer, json_data|
        next if json_data.nil?

        service_provider_id = json_data.dig(:provider_information, :service_provider_id)
        next if service_provider_id.nil?

        unless mapping_ids.include?(Integer(service_provider_id, exception: false))
          log_message(
            :warn,
            'Service provider ID for issuer not found in issuer mapping',
            false,
            log_context(issuer: issuer, service_provider_id: service_provider_id),
          )
        end
      end
    end

    def upload_issuer_mapping_to_s3(mapping_data)
      base_path = generate_base_s3_path(directory: 'portal')
      path = "#{base_path}issuers_service_provider_id.json"

      if bucket_name.present?
        upload_file_to_s3_bucket(
          path: path,
          body: JSON.pretty_generate(mapping_data),
          content_type: 'application/json',
          bucket: bucket_name,
        )
        log_message(
          :info,
          'Uploaded issuer mapping to S3',
          true,
          log_context(s3_path: path),
        )
      end
    end

    def json_file(data)
      JSON.pretty_generate(data)
    end

    def period_date
      raise ArgumentError, 'report_date must be set before calling period_date' if @report_date.nil?
      reporter_class_for(@report_version).get_period_date_from_report_date(
        report_date: @report_date,
        cadence: REPORT_CADENCE,
      )
    end

    def reporter_class_for(version)
      case version
      when 'v1' then Reporting::PartnerReportDefault
      when 'v2' then Reporting::PartnerReportDefaultV2
      else raise ArgumentError, "Unsupported report_version: #{version}"
      end
    end
  end
end
