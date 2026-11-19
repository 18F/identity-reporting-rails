# frozen_string_literal: true

require 'reporting/partner_report_default'
require 'reporting/partner_report_default_v2'
require 'reporting/partner_report_period'

module Reports
  class PartnerReportDefault < BaseReport
    DEFAULT_CADENCE = 'monthly'
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

    attr_reader :report_date, :included_issuers, :excluded_issuers, :report_version,
                :report_cadence, :result

    # Used by the manual backfill preview. No AWS request is made here.
    def upload_target
      { environment: Identity::Hostdata.env,
        bucket: bucket_name,
        prefix: generate_base_s3_path(directory: 'portal') }
    end

    def perform(report_date = nil, report_version = nil, options = {})
      options ||= {}
      @result = nil
      @period_date = nil
      @report_cadence = options.fetch(:report_cadence, DEFAULT_CADENCE)
      unless Reporting::PartnerReportPeriod::CADENCES.include?(@report_cadence)
        raise ArgumentError, "Invalid report_cadence: #{@report_cadence}"
      end

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

      @report_date = report_date || REPORT_DELAY_DAYS.days.ago.end_of_day
      @report_date = Date.parse(@report_date) if @report_date.is_a?(String)

      log_message(
        :info,
        'Generating partner default reports',
        true,
        log_context(period_date: period_date),
      )

      # Wait 10 seconds to allow user to cancel job if they realize they ran it with the wrong date
      # This hopefully prevents accidental population of reports manually for wrong dates
      unless options[:skip_confirmation_delay] == true
        log_message(
          :info,
          'Waiting before generating reports so manual runs can be canceled if needed',
          true,
          log_context(wait_seconds: 10),
        )
        sleep 10
      end

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

      @result = generate_and_upload_reports(@report_date)
      log_message(:info, 'Completed partner default report', true, log_context)

      @result
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
        report_cadence: @report_cadence,
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
        report_cadence: @report_cadence,
        included_issuers: @included_issuers,
        excluded_issuers: @excluded_issuers,
      )
      # Generate issuer mapping to link IDs and issuer strings
      issuer_mapping = reporter.generate_issuer_mapping
      issuer_reports = reporter.generate_reports
      @result = log_context.merge(
        period_start_date: period_date,
        period_end_date: Reporting::PartnerReportPeriod.end_date(period_date, @report_cadence).to_s,
        uploaded_count: 0,
        failed_count: 0,
        skipped_count: 0,
        failures: [],
        skipped: [],
        missing_issuers: [],
        issuer_mapping_uploaded: false, # flips to true after successful upload
      )

      # If included issuers specified, check they were in SQL results
      if @included_issuers&.any?
        missing = @included_issuers - issuer_reports.keys
        @result[:missing_issuers] = missing
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
      @result[:issuer_mapping_uploaded] = true

      # Upload individual reports
      uploaded_count = 0
      skipped_uploads = []

      issuer_reports.each do |issuer, json_data|
        if json_data.nil?
          skipped_uploads << "#{issuer}: report generation failed and returned nil"
          @result[:skipped] << { issuer: issuer, reason: 'report generation returned nil' }
          @result[:skipped_count] += 1
          next
        end

        service_provider_id = json_data.dig(:provider_information, :service_provider_id)

        if service_provider_id.nil?
          skipped_uploads << "#{issuer}: missing service_provider_id"
          @result[:skipped] << { issuer: issuer, reason: 'missing service_provider_id' }
          @result[:skipped_count] += 1
          next
        end

        begin
          upload_to_s3(
            json_data,
            service_provider_id: service_provider_id,
            period_date: period_date,
          )
          uploaded_count += 1
          @result[:uploaded_count] += 1
        rescue StandardError => e
          skipped_uploads << record_upload_failure(issuer, service_provider_id, e)
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
      @result
    end

    def record_upload_failure(issuer, service_provider_id, error)
      @result[:failed_count] += 1
      @result[:failures] << {
        issuer: issuer,
        service_provider_id: service_provider_id,
        error: error.message,
        error_class: error.class.name,
      }
      "#{issuer} / #{service_provider_id}: upload failed - #{error.message}"
    end

    # S3 path structure:
    # Monthly/daily: env/portal/version/service_provider_id/cadence/2025-01-01.json
    # Weekly: env/portal/version/service_provider_id/weekly/2025-01-06_2025-01-12.json
    def upload_to_s3(json_data, service_provider_id:, period_date:)
      base_path = generate_base_s3_path(directory: 'portal')
      cadence = @report_cadence || DEFAULT_CADENCE
      filename = period_date.to_s
      if cadence == 'weekly'
        end_date = Reporting::PartnerReportPeriod.end_date(period_date, cadence).to_s
        filename = "#{period_date}_#{end_date}"
      end

      path = case @report_version
             when 'v1', 'v2'
               "#{base_path}#{@report_version}/#{service_provider_id}"\
                  "/#{cadence}/#{filename}.json"
             else
               raise ArgumentError, "Unsupported report_version: #{@report_version}"
             end

      raise ArgumentError, 'Partner report bucket must be configured' unless bucket_name.present?

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
      raise ArgumentError, 'Partner report bucket must be configured' unless bucket_name.present?
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
      @period_date ||= Reporting::PartnerReportPeriod.start_date(
        report_date: @report_date,
        cadence: @report_cadence || DEFAULT_CADENCE,
      )
    end
  end
end
