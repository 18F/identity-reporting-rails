# frozen_string_literal: true

require 'csv'
require 'reporting/identity_verification_report'

module Reporting
  class MonthlyIdvReport
    attr_reader :end_date

    def initialize(end_date:)
      @end_date = end_date.in_time_zone('UTC')
    end

    def as_reports
      [
        {
          title: 'Proofing Rate Metrics',
          subtitle: 'Condensed (NEW)',
          float_as_percent: true,
          precision: 2,
          table: as_csv,
          filename: 'condensed_idv',
        },
      ]
    end

    def as_csv
      csv = []

      csv << ['Metric', *reports.map { |t| t.time_range.begin.strftime('%b %Y') }]
      csv << ['IDV started', *reports.map(&:idv_started)]

      csv << ['# of successfully verified users', *reports.map(&:successfully_verified_users)]
      csv << ['% IDV started to successfully verified', *reports.map(&:blanket_proofing_rate)]

      csv << ['# of workflow completed', *reports.map(&:idv_final_resolution)]
      csv << ['% rate of workflow completed', *reports.map(&:idv_final_resolution_rate)]

      csv << ['# of users verified (total)', *reports.map(&:verified_user_count)]
    rescue StandardError => err
      [
        ['Error', 'Message'],
        [err.class.name, err.message],
      ]
    end

    def reports
      @reports ||= monthly_subreports
    end

    def monthly_subreports
      [end_date.all_month].map do |range|
        Reporting::IdentityVerificationReport.new(
          issuers: nil,
          time_range: range,
        )
      end
    end
  end
end
