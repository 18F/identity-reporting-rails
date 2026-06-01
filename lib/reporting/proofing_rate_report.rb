# frozen_string_literal: true

require 'csv'
require 'reporting/identity_verification_report'

module Reporting
  class ProofingRateReport
    DATE_INTERVALS = [30].freeze

    attr_reader :end_date

    def initialize(end_date:)
      @end_date = end_date.in_time_zone('UTC')
    end

    def as_reports
      [
        {
          title: 'Proofing Rate Metrics',
          subtitle: 'Detail',
          float_as_percent: true,
          precision: 2,
          table: as_csv,
          filename: 'proofing_rate_metrics',
        },
      ]
    end

    def as_csv
      csv = []

      csv << ['Metric', *DATE_INTERVALS.map { |days| "Trailing #{days}d" }]
      csv << ['Start Date', *reports.map(&:time_range).map(&:begin).map(&:to_date)]
      csv << ['End Date', *reports.map(&:time_range).map(&:end).map(&:to_date)]

      csv << ['IDV Started', *reports.map(&:idv_started)]
      csv << ['Welcome Submitted', *reports.map(&:idv_doc_auth_welcome_submitted)]
      csv << ['Image Submitted', *reports.map(&:idv_doc_auth_image_vendor_submitted)]
      csv << ['Socure', *reports.map(&:idv_doc_auth_socure_verification_data_requested)]
      csv << ['Successfully Verified', *reports.map(&:successfully_verified_users)]
      csv << ['IDV Rejected (Non-Fraud)', *reports.map(&:idv_doc_auth_rejected)]
      csv << ['IDV Rejected (Fraud)', *reports.map(&:idv_fraud_rejected)]

      csv << [
        'Blanket Proofing Rate (IDV Started to Successfully Verified)',
        *reports.map(&:blanket_proofing_rate),
      ]
      csv << [
        'Intent Proofing Rate (Welcome Submitted to Successfully Verified)',
        *reports.map(&:intent_proofing_rate),
      ]
      csv << [
        'Actual Proofing Rate (Image Submitted to Successfully Verified)',
        *reports.map(&:actual_proofing_rate),
      ]
      csv << [
        'Industry Proofing Rate (Verified minus IDV Rejected)',
        *reports.map(&:industry_proofing_rate),
      ]

      csv
    rescue StandardError => err
      [
        ['Error', 'Message'],
        [err.class.name, err.message],
      ]
    end

    def to_csv
      CSV.generate do |csv|
        as_csv.each { |row| csv << row }
      end
    end

    def reports
      @reports ||= begin
        trailing_days_subreports.reduce([]) do |acc, report|
          if acc.empty?
            acc << report
          else
            acc << report.merge(acc.last)
          end
        end
      end
    end

    def trailing_days_subreports
      [0, *DATE_INTERVALS].each_cons(2).map do |slice_end, slice_start|
        time_range = if slice_end.zero?
                       Range.new(
                         (end_date - slice_start.days).beginning_of_day,
                         (end_date - slice_end.days).end_of_day,
                       )
                     else
                       Range.new(
                         (end_date - slice_start.days).beginning_of_day,
                         (end_date - slice_end.days).end_of_day - 1.day,
                       )
                     end

        Reporting::IdentityVerificationReport.new(
          issuers: nil,
          time_range: time_range,
        )
      end
    end
  end
end
