# frozen_string_literal: true

require 'reporting/demographics_metrics_report'

module Reports
  class DemographicsMetricsReport < BaseReport
    REPORT_NAME = 'demographics-metrics-report'
    attr_accessor :report_date

    def perform(report_date)
      unless IdentityConfig.store.redshift_sia_v3_enabled
        Rails.logger.warn 'Redshift SIA V3 is disabled'
        return false
      end

      return unless IdentityConfig.store.s3_reports_enabled

      self.report_date = report_date

      report_configs.each do |report_config|
        generate_and_upload_report(report_config)
      end
    end

    private

    def generate_and_upload_report(report_config)
      issuers = report_config['issuers']
      agency_abbreviation = report_config['agency_abbreviation']

      Rails.logger.info "Generating demographics report for #{agency_abbreviation}"

      reports = demographics_reports(issuers, agency_abbreviation)

      reports.each do |report|
        table = report.fetch(:table)
        filename = report.fetch(:filename)
        upload_to_s3(table, report_name: filename, agency: agency_abbreviation)
      end

      Rails.logger.info "Completed demographics report for #{agency_abbreviation}"
    rescue StandardError => err
      Rails.logger.error "Failed to generate demographics report for #{agency_abbreviation}: #{err.message}"
      raise err
    end

    def demographics_reports(issuers, agency_abbreviation)
      Reporting::DemographicsMetricsReport.new(
        issuers: issuers,
        agency_abbreviation: agency_abbreviation,
        time_range: report_date.all_quarter,
      ).as_reports
    end

    def upload_to_s3(report_body, report_name: nil, agency: nil)
      # Create agency-specific path for better organization
      report_name_with_agency = agency ? "#{agency.downcase}_#{REPORT_NAME}" : REPORT_NAME
      _latest, path = generate_s3_paths(
        report_name_with_agency,
        'csv',
        directory: 'idp',
        subname: report_name,
        now: report_date,
      )

      if bucket_name.present?
        upload_file_to_s3_bucket(
          path: path,
          body: csv_file(report_body),
          content_type: 'text/csv',
          bucket: bucket_name,
        )
        Rails.logger.info "Uploaded #{report_name} to S3: #{path}"
      end
    end

    def report_configs
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
