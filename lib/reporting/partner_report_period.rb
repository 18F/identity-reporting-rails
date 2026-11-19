# frozen_string_literal: true

require 'date'

module Reporting
  module PartnerReportPeriod
    CADENCES = %w[monthly weekly daily].freeze

    # Returns the warehouse-defined period start as a YYYY-MM-DD string.
    def self.start_date(report_date:, cadence: 'monthly')
      unless CADENCES.include?(cadence)
        raise ArgumentError, "Invalid cadence: #{cadence}"
      end

      begin
        Date.parse(report_date.to_s)
      rescue ArgumentError => e
        raise ArgumentError, "Invalid date format for report_date: #{report_date} - #{e.message}"
      end

      query = <<~SQL
        SELECT
          CASE
            WHEN $1 = 'monthly' THEN TO_CHAR(month_start_date_actual, 'YYYY-MM-DD')
            WHEN $1 = 'weekly' THEN TO_CHAR(week_start_date_actual, 'YYYY-MM-DD')
            WHEN $1 = 'daily' THEN TO_CHAR(cal.date_actual, 'YYYY-MM-DD')
          END AS period_date_actual
        FROM marts.calendar cal
        WHERE cal.calendar_id = TO_CHAR($2::date, 'YYYYMMDD')::int;
      SQL

      result = DataWarehouseApplicationRecord.connection.exec_query(
        query,
        'get_period_date',
        [cadence, report_date],
      ).first

      if result.nil?
        raise StandardError, "No calendar entry found for report_date: #{report_date}"
      end

      period_date = result['period_date_actual']
      if period_date.nil?
        raise StandardError, "No period_date_actual found for report_date: #{report_date}"
      end

      period_date
    end

    def self.end_date(start_date, cadence)
      date = Date.parse(start_date.to_s) # Make sure date is not a time
      case cadence
      when 'monthly' then Date.new(date.year, date.month, -1) # last day of the month
      when 'weekly' then date + 6 # Add 6 days to date object
      when 'daily' then date
      else raise ArgumentError, "Invalid report_cadence: #{cadence}"
      end
    end

    def self.next_start(start_date, cadence)
      end_date(start_date, cadence) + 1
    end
  end
end
