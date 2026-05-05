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

    def perform(date = nil)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end
      return unless IdentityConfig.store.s3_reports_enabled

      # Use provided date, or constructor date, or default
      @report_date = date || @report_date || REPORT_DELAY_DAYS.days.ago.end_of_day

      if period_date.nil?
        Rails.logger.error "Cannot generate reports: "\
        "failed to retrieve period_date in marts.calendar for report_date #{report_date}"
        return false
      end

      Rails.logger.info "Generating partner default #{REPORT_CADENCE} reports for report date: "\
                      "#{report_date} (#{REPORT_CADENCE} report period starting on #{period_date})"
      generate_and_upload_reports(report_date)
      Rails.logger.info "Completed partner default #{REPORT_CADENCE} report"
    end

    private

    def generate_and_upload_reports(report_date)
      reporter = Reporting::PartnerReportDefault.new(
        report_date: report_date,
        report_cadence: REPORT_CADENCE,
      )
      # Generate issuer mapping to link IDs and issuer strings
      issuer_mapping = reporter.generate_issuer_mapping
      issuer_reports = reporter.generate_reports

      # Check that all service_provider_ids exist in mapping (just in case, warn if any missing)
      validate_service_provider_ids(issuer_reports, issuer_mapping)

      upload_issuer_mapping_to_s3(issuer_mapping)

      # Upload individual reports
      uploaded_count = 0
      skipped_count = 0

      issuer_reports.each do |issuer, json_data|
        if json_data.nil?
          Rails.logger.warn "Skipping upload for #{issuer}: report generation failed and returned nil"
          skipped_count += 1
          next
        end

        begin
          service_provider_id = json_data[:provider_information][:service_provider_id]
          if service_provider_id.nil?
            Rails.logger.error "Missing service_provider_id for #{issuer}, skipping upload"
            skipped_count += 1
            next
          end

          upload_to_s3(
            json_data, service_provider_id: service_provider_id,
                       period_date: period_date
          )
          uploaded_count += 1
        rescue => e
          Rails.logger.error "Failed to upload report for #{issuer}: #{e.message}"
          skipped_count += 1
        end
      end

      Rails.logger.info "Upload summary: #{uploaded_count} successful, #{skipped_count} skipped"
    rescue StandardError => err
      Rails.logger.error "Failed to generate partner default "\
                         "#{REPORT_CADENCE} reports: #{err.message}"
      raise err
    end

    def upload_to_s3(json_data, service_provider_id:, period_date:)
      # S3 path structure: service_provider_id/REPORT_CADENCE/2025-01-01.json
      base_path = generate_base_s3_path(directory: 'portal')
      path = "#{base_path}#{service_provider_id}/#{REPORT_CADENCE}/#{period_date}.json"

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

    def validate_service_provider_ids(issuer_reports, issuer_mapping)
      mapping_ids = issuer_mapping.values.map { |v| v[:id] }.to_set

      issuer_reports.each do |issuer, json_data|
        next if json_data.nil?

        service_provider_id = json_data[:provider_information][:service_provider_id]
        next if service_provider_id.nil?

        unless mapping_ids.include?(service_provider_id)
          Rails.logger.warn "Service provider ID #{service_provider_id} for issuer "\
                            "'#{issuer}' not found in issuer mapping"
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
        Rails.logger.info "Uploaded issuer mapping to S3: #{path}"
      end
    end

    def json_file(data)
      JSON.pretty_generate(data)
    end

    def period_date
      return nil unless @report_date

      date_str = Reporting::PartnerReportDefault.get_period_date_from_report_date(
        report_date: @report_date,
        cadence: REPORT_CADENCE,
      )

      # Validate format if present
      if date_str && !date_str.match?(/\A\d{4}-\d{2}-\d{2}\z/)
        Rails.logger.error "Invalid period_date format received: '#{date_str}'."\
        " Expected YYYY-MM-DD"
        nil
      else
        date_str
      end
    end
  end
end
